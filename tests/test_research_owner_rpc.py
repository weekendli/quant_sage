"""01-G process, Unix-socket owner and CLI acceptance tests."""

from __future__ import annotations

import json
import os
import signal
import socket
import stat
import struct
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

from tests.research_execution_fixtures import make_execution_environment

REPO_ROOT = Path(__file__).resolve().parents[1]
RPC_SCHEMA = "research_rpc_v1"
STUDY_ID = "planning_study"
SOURCE_REF = "planning_source@1"
FEATURE_SET_REF = "planning_features@1"


def _envelope(value: Any) -> dict[str, Any]:
    assert isinstance(value, dict)
    assert value.get("schema_version") == RPC_SCHEMA
    assert value.get("request_id") is None or isinstance(value.get("request_id"), str)
    assert type(value.get("ok")) is bool
    if value["ok"]:
        assert value.get("error") is None
        assert "data" in value
    else:
        error = value.get("error")
        assert isinstance(error, dict)
        assert isinstance(error.get("code"), str) and error["code"]
        assert isinstance(error.get("message"), str)
        assert isinstance(error.get("details"), dict)
        assert type(error.get("retryable")) is bool
    return value


def _call_rpc(
    socket_path: Path,
    database_path: Path,
    method: str,
    params: dict[str, Any],
    *,
    request_id: str | None = None,
    timeout: float = 4.0,
    max_response: int = 16 * 1024 * 1024,
) -> dict[str, Any]:
    request_id = request_id or uuid.uuid4().hex
    payload = {
        "schema_version": RPC_SCHEMA,
        "request_id": request_id,
        "database": str(database_path.resolve()),
        "method": method,
        "params": params,
    }
    encoded = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode() + b"\n"
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout)
        client.connect(str(socket_path))
        client.sendall(encoded)
        response = bytearray()
        while b"\n" not in response:
            block = client.recv(min(65536, max_response + 1 - len(response)))
            if not block:
                break
            response.extend(block)
            assert len(response) <= max_response, "owner exceeded the bounded response size"
        assert response.endswith(b"\n"), "owner closed without a complete JSON line"
    value = _envelope(json.loads(response.split(b"\n", 1)[0]))
    assert value["request_id"] == request_id
    return value


def _cli(
    database_path: Path,
    socket_path: Path,
    *command: str,
    input_value: dict[str, Any] | None = None,
    extra_args: tuple[str, ...] = (),
    timeout: float = 5.0,
) -> tuple[subprocess.CompletedProcess[str], dict[str, Any]]:
    command_line = [
        sys.executable,
        "-m",
        "quantsage_discovery.cli",
        "research",
        *command,
        "--database",
        str(database_path),
        "--socket",
        str(socket_path),
        *extra_args,
    ]
    stdin = None
    if input_value is not None:
        command_line.extend(("--input", "-"))
        stdin = json.dumps(input_value, separators=(",", ":"), allow_nan=False)
    completed = subprocess.run(
        command_line,
        cwd=REPO_ROOT,
        input=stdin,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    assert completed.stdout.strip(), (completed.returncode, completed.stderr)
    try:
        response = _envelope(json.loads(completed.stdout))
    except json.JSONDecodeError as exc:
        pytest.fail(f"CLI stdout was not one JSON envelope: {completed.stdout!r}; {exc}")
    return completed, response


def _owner_command(database_path: Path, socket_path: Path) -> list[str]:
    return [
        sys.executable,
        "-m",
        "quantsage_discovery.cli",
        "research",
        "owner",
        "serve",
        "--database",
        str(database_path),
        "--socket",
        str(socket_path),
    ]


def _wait_for_owner(process: subprocess.Popen[str], socket_path: Path,
                    database_path: Path, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            pytest.fail(f"owner exited early: {process.returncode}\n{stdout}\n{stderr}")
        if socket_path.exists():
            try:
                response = _call_rpc(
                    socket_path,
                    database_path,
                    "sources.get",
                    {"ref": SOURCE_REF},
                    timeout=0.25,
                )
            except (OSError, AssertionError, TimeoutError, json.JSONDecodeError):
                time.sleep(0.02)
                continue
            assert response["ok"] is True
            return
        time.sleep(0.02)
    pytest.fail("owner did not create a responsive Unix socket before timeout")


def _stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def _workspace(tmp_path: Path):
    workspace = tmp_path / "workspace"
    registry, service, _source, frozen, request = make_execution_environment(workspace)
    try:
        study = service.get_study(STUDY_ID)
        request["study_revision"] = study["revision"]
        source_document = service.get_source(SOURCE_REF)
    finally:
        # The socket owner must be the only process holding the active Registry.
        registry.close()
    database_path = workspace / "research.duckdb"
    socket_path = workspace / "owner" / "research.sock"
    socket_path.parent.mkdir(mode=0o700)
    return workspace, database_path, socket_path, source_document, frozen, request


def _start_owner(database_path: Path, socket_path: Path) -> subprocess.Popen[str]:
    process = subprocess.Popen(
        _owner_command(database_path, socket_path),
        cwd=REPO_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    _wait_for_owner(process, socket_path, database_path)
    return process


def _start_hook_owner(
    database_path: Path,
    socket_path: Path,
    marker_path: Path,
    release_path: Path,
    *,
    phase: str | None = None,
    delay_method: str | None = None,
) -> subprocess.Popen[str]:
    """Start a real owner with a trusted, test-only deterministic pause seam."""
    script = r'''import json, sys, time
from pathlib import Path
from quantsage_discovery.research.rpc import ResearchOwner

database, socket_path, marker, release, phase, delay_method = sys.argv[1:7]
marker_path, release_path = Path(marker), Path(release)

def pause(label, details):
    if marker_path.exists():
        return
    marker_path.write_text(json.dumps({"label": label, **details}), encoding="utf-8")
    while not release_path.exists():
        time.sleep(0.01)

def fault_hook(label, details):
    if label == phase:
        pause(label, details)

owner = ResearchOwner(database, socket_path, fault_hook=(
    fault_hook if phase else None
))
if delay_method:
    dispatch = owner._dispatch
    def delayed_dispatch(method, params):
        if method == delay_method:
            pause(method, {"method": method})
        return dispatch(method, params)
    owner._dispatch = delayed_dispatch
owner.serve_forever()
'''
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            script,
            str(database_path),
            str(socket_path),
            str(marker_path),
            str(release_path),
            phase or "",
            delay_method or "",
        ],
        cwd=REPO_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    _wait_for_owner(process, socket_path, database_path)
    return process


def _wait_for_file(path: Path, process: subprocess.Popen[str], timeout: float = 10.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            pytest.fail(f"owner exited before test marker: {process.returncode}\n{stdout}\n{stderr}")
        time.sleep(0.02)
    pytest.fail(f"owner did not reach its controlled test seam: {path}")


def _plan_and_submit(
    database_path: Path,
    socket_path: Path,
    request: dict[str, Any],
    request_key: str,
) -> dict[str, Any]:
    planned, plan_response = _cli(
        database_path, socket_path, "runs", "plan", input_value=request,
    )
    assert planned.returncode == 0, plan_response
    submitted, run_response = _cli(
        database_path,
        socket_path,
        "runs",
        "submit",
        input_value=request,
        extra_args=("--request-key", request_key,
                    "--expected-plan-hash", plan_response["data"]["plan_hash"]),
    )
    assert submitted.returncode == 0, run_response
    return run_response["data"]


def _execute_and_wait(
    database_path: Path,
    socket_path: Path,
    run_id: str,
    *,
    resume: bool = False,
    timeout: float = 12.0,
) -> dict[str, Any]:
    action = "resume" if resume else "execute"
    dispatched, dispatch_response = _cli(
        database_path, socket_path, "runs", action, run_id,
    )
    assert dispatched.returncode == 0, dispatch_response
    assert dispatch_response["data"]["run_id"] == run_id
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        inspected, response = _cli(database_path, socket_path, "runs", "get", run_id)
        assert inspected.returncode == 0, response
        run = response["data"]
        status = run.get("summary", {}).get("status", run.get("status"))
        if status == "COMPLETE":
            return run
        if status in {"FAILED", "PARTIAL", "BLOCKED", "SKIPPED"}:
            pytest.fail(f"Run {run_id} ended in {status}: {run}")
        time.sleep(0.05)
    pytest.fail(f"Run {run_id} did not complete within {timeout:g}s")


def test_private_same_uid_socket_unknown_methods_and_owner_lock(tmp_path):
    _, database_path, socket_path, source_document, _, _ = _workspace(tmp_path)
    owner = _start_owner(database_path, socket_path)
    second_owner = None
    try:
        socket_info = socket_path.stat()
        assert stat.S_IMODE(socket_info.st_mode) == 0o600
        assert stat.S_IMODE(socket_path.parent.stat().st_mode) == 0o700
        assert socket_info.st_uid == os.getuid()

        source_response = _call_rpc(
            socket_path,
            database_path,
            "sources.get",
            {"ref": SOURCE_REF},
        )
        assert source_response["ok"] is True
        source = source_response["data"]
        assert source["source_id"] == source_document["source_id"]
        assert source["availability_mode"] == "research_only"
        assert source.get("time_rules", {}).get("available_time") is None

        forbidden = _call_rpc(
            socket_path,
            database_path,
            "sql.query",
            {"sql": "DELETE FROM data_sources"},
        )
        assert forbidden["ok"] is False
        assert forbidden["error"]["code"] == "INVALID_REQUEST"
        assert forbidden["error"]["details"]["method"] == "sql.query"

        second_owner = subprocess.run(
            _owner_command(database_path, socket_path),
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            timeout=5,
            check=False,
        )
        assert second_owner.returncode == 4
        lock_response = _envelope(json.loads(second_owner.stdout))
        assert lock_response["ok"] is False
        assert lock_response["error"]["code"] == "WORKSPACE_BUSY"
    finally:
        _stop_process(owner)
        if second_owner is not None:
            assert second_owner.returncode == 4
    from quantsage_discovery.core.registry import Registry
    from quantsage_discovery.research.service import ResearchService

    registry = Registry(database_path)
    try:
        assert ResearchService(registry).get_source(SOURCE_REF)["source_id"] == \
            source_document["source_id"]
    finally:
        registry.close()


def test_peer_uid_gate_rejects_a_different_uid_without_changing_system_users():
    from quantsage_discovery.research import rpc

    same_uid = os.getuid()
    assert rpc._require_same_uid(same_uid, expected_uid=same_uid) is None
    with pytest.raises(rpc.RpcPeerRejectedError):
        rpc._require_same_uid(same_uid + 1, expected_uid=same_uid)

    class PeerCredentials:
        def getsockopt(self, level, option, size):
            assert level == socket.SOL_SOCKET
            assert option == socket.SO_PEERCRED
            assert size == struct.calcsize("3i")
            return struct.pack("3i", 12345, same_uid, os.getgid())

    assert rpc.peer_uid_from_socket(PeerCredentials()) == same_uid


def test_owner_rejects_requests_over_transport_byte_limit_with_error_envelope(tmp_path):
    from quantsage_discovery.research.rpc import MAX_REQUEST_BYTES

    _, database_path, socket_path, _, _, _ = _workspace(tmp_path)
    owner = _start_owner(database_path, socket_path)
    try:
        raw_request = b'{"padding":"' + b"x" * MAX_REQUEST_BYTES + b'"}\n'
        assert len(raw_request) > MAX_REQUEST_BYTES
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(5)
            client.connect(str(socket_path))
            client.sendall(raw_request)
            response = bytearray()
            while b"\n" not in response:
                block = client.recv(4096)
                if not block:
                    break
                response.extend(block)
        envelope = _envelope(json.loads(response.split(b"\n", 1)[0]))
        assert envelope["request_id"] is None
        assert envelope["ok"] is False
        assert envelope["error"]["code"] == "REQUEST_TOO_LARGE"
        assert "byte limit" in envelope["error"]["message"]
    finally:
        _stop_process(owner)


def test_research_cli_management_and_execution_closure(tmp_path):
    from tests.research_catalog_fixtures import (
        feature_set_document,
        provider_document,
        recipe_item,
    )
    from tests.research_execution_fixtures import make_numeric_source_fixture
    from tests.research_fixtures import make_source_fixture
    from tests.research_planning_fixtures import make_planning_study_document

    workspace, database_path, socket_path, _, _, base_request = _workspace(tmp_path)
    owner = _start_owner(database_path, socket_path)
    try:
        alternate = make_numeric_source_fixture(
            workspace / "alternate-source",
            [{"event_time": "2024-06-03T13:10:15+00:00", "value": 7.0}],
            source_id="rpc_cli_source",
        )
        registered_source, source_response = _cli(
            database_path,
            socket_path,
            "sources",
            "register",
            input_value=alternate.document,
        )
        assert registered_source.returncode == 0, source_response
        assert source_response["data"]["source_id"] == "rpc_cli_source"
        fetched_source, fetched_source_response = _cli(
            database_path, socket_path, "sources", "get", "rpc_cli_source@1",
        )
        assert fetched_source.returncode == 0, fetched_source_response
        assert fetched_source_response["data"]["availability_mode"] == "research_only"
        assert fetched_source_response["data"].get("time_rules", {}).get("available_time") is None

        metadata_source = make_source_fixture(
            workspace / "metadata-source",
            source_id="rpc_cli_metadata_source",
            include_metadata=True,
        )
        registered_metadata_source, metadata_register_response = _cli(
            database_path,
            socket_path,
            "sources",
            "register",
            input_value=metadata_source.document,
        )
        assert registered_metadata_source.returncode == 0, metadata_register_response
        fetched_metadata_source, metadata_get_response = _cli(
            database_path,
            socket_path,
            "sources",
            "get",
            "rpc_cli_metadata_source@1",
        )
        assert fetched_metadata_source.returncode == 0, metadata_get_response
        assert metadata_get_response["data"]["availability_mode"] == "research_only"
        assert metadata_get_response["data"]["market_metadata_ref"] == \
            metadata_source.document["market_metadata_ref"]
        assert "available_time" not in metadata_get_response["data"]["time_rules"]
        assert "available_time_field" not in metadata_get_response["data"]["time_rules"]

        study_document = make_planning_study_document(study_id="rpc_cli_study")
        created_study, create_response = _cli(
            database_path,
            socket_path,
            "studies",
            "create",
            input_value=study_document,
        )
        assert created_study.returncode == 0, create_response
        fetched_study, get_study_response = _cli(
            database_path, socket_path, "studies", "get", "rpc_cli_study",
        )
        assert fetched_study.returncode == 0, get_study_response
        assert get_study_response["data"]["revision"] == 1

        backlog = [{
            "feature_set_ref": FEATURE_SET_REF,
            "item_id": "quote",
            "priority": 1,
            "state": "active",
            "reason": "Finite owner RPC acceptance target.",
        }]
        revised_study, update_response = _cli(
            database_path,
            socket_path,
            "studies",
            "update",
            "planning_study",
            input_value={"backlog": backlog, "notes": "updated through local owner"},
            extra_args=("--expected-revision", "1", "--actor-kind", "agent",
                        "--actor-id", "rpc-acceptance", "--reason", "Add one finite target."),
        )
        assert revised_study.returncode == 0, update_response
        assert update_response["data"]["revision"] == 2

        provider = provider_document(provider_id="rpc_cli_provider")
        registered_provider, provider_response = _cli(
            database_path,
            socket_path,
            "catalog",
            "register",
            "provider",
            input_value=provider,
        )
        assert registered_provider.returncode == 0, provider_response
        fetched_provider, get_provider_response = _cli(
            database_path, socket_path, "catalog", "get", "provider", "rpc_cli_provider@1",
        )
        assert fetched_provider.returncode == 0, get_provider_response
        assert get_provider_response["data"]["provider_id"] == "rpc_cli_provider"

        feature_set = feature_set_document(
            feature_set_id="rpc_cli_features",
            items=[recipe_item("rpc_quote", parameters={"scale": 2})],
        )
        saved_set, save_response = _cli(
            database_path,
            socket_path,
            "feature-sets",
            "save",
            input_value=feature_set,
        )
        assert saved_set.returncode == 0, save_response
        draft_revision = save_response["data"]["draft_revision"]
        frozen_set, freeze_response = _cli(
            database_path,
            socket_path,
            "feature-sets",
            "freeze",
            "rpc_cli_features@1",
            extra_args=("--expected-revision", str(draft_revision)),
        )
        assert frozen_set.returncode == 0, freeze_response
        resolved = freeze_response["data"]["resolved_items"]
        feature_ref = resolved[0]["outputs"][0]["feature_ref"]
        read_set, set_response = _cli(
            database_path, socket_path, "feature-sets", "get", "rpc_cli_features@1",
        )
        assert read_set.returncode == 0, set_response
        listed_features, list_features_response = _cli(
            database_path,
            socket_path,
            "features",
            "list",
            input_value={"kind": "primitive"},
        )
        assert listed_features.returncode == 0, list_features_response
        feature_id = feature_ref.rsplit("@", 1)[0]
        assert any(item["feature_id"] == feature_id
                   for item in list_features_response["data"]["items"])
        read_feature, feature_response = _cli(
            database_path, socket_path, "features", "get", feature_ref,
        )
        assert read_feature.returncode == 0, feature_response
        assert feature_response["data"]["feature_id"] == feature_id

        request = dict(base_request)
        request["study_revision"] = 2
        request["scope"] = {**base_request["scope"], "end": "2024-06-03T13:10:30+00:00"}
        first_submission = _plan_and_submit(database_path, socket_path, request, "rpc-cli-first")
        first_run = _execute_and_wait(database_path, socket_path, first_submission["run_id"])
        assert first_run["result_refs"]

        resumed = _execute_and_wait(
            database_path,
            socket_path,
            first_submission["run_id"],
            resume=True,
        )
        assert resumed["status"] == "COMPLETE"

        second_request = dict(request)
        second_request["scope"] = {
            **request["scope"], "end": "2024-06-03T13:11:00+00:00",
        }
        second_submission = _plan_and_submit(
            database_path, socket_path, second_request, "rpc-cli-second",
        )
        second_run = _execute_and_wait(database_path, socket_path, second_submission["run_id"])
        assert second_run["result_refs"]

        listed_results, list_results_response = _cli(
            database_path, socket_path, "results", "list", input_value={},
        )
        assert listed_results.returncode == 0, list_results_response
        assert any(item["result_id"] in first_run["result_refs"]
                   for item in list_results_response["data"]["items"])
        read_results, read_results_response = _cli(
            database_path,
            socket_path,
            "results",
            "read",
            input_value={
                "result_refs": [first_run["result_refs"][0]],
                "columns": ["event_time", "available_time", "value"],
                "scope": request["scope"],
            },
            extra_args=("--limit", "10"),
        )
        assert read_results.returncode == 0, read_results_response
        assert read_results_response["data"]["rows"]
        assert all(row.get("available_time") is None
                   for row in read_results_response["data"]["rows"])

        review = {
            "schema_version": "review_v1",
            "review_id": "rpc_cli_review",
            "study_id": STUDY_ID,
            "actor": {"kind": "agent", "id": "rpc-acceptance"},
            "run_refs": [first_run["run_id"]],
            "result_refs": [first_run["result_refs"][0]],
            "scope": request["scope"],
            "observations": "Inspected serialized synthetic owner output.",
            "decision": "investigate",
            "rationale": "Exercise the append-only Review CLI method.",
            "limitations": "Synthetic Parquet fixture only.",
        }
        added_review, add_review_response = _cli(
            database_path, socket_path, "reviews", "add", input_value=review,
        )
        assert added_review.returncode == 0, add_review_response
        listed_reviews, list_reviews_response = _cli(
            database_path,
            socket_path,
            "reviews",
            "list",
            STUDY_ID,
            extra_args=("--run-id", first_run["run_id"]),
        )
        assert listed_reviews.returncode == 0, list_reviews_response
        assert any(item["review_id"] == "rpc_cli_review"
                   for item in list_reviews_response["data"]["items"])

        coverage, coverage_response = _cli(
            database_path,
            socket_path,
            "studies",
            "coverage",
            input_value={
                "study_id": STUDY_ID,
                "revision": 2,
                "source_refs": [SOURCE_REF],
                "scope": request["scope"],
                "feature_set_ref": FEATURE_SET_REF,
                "item_ids": ["quote"],
                "verify": True,
            },
        )
        assert coverage.returncode == 0, coverage_response
        assert coverage_response["data"]["targets"]

        comparison, compare_response = _cli(
            database_path,
            socket_path,
            "runs",
            "compare",
            first_run["run_id"],
            second_run["run_id"],
        )
        assert comparison.returncode == 0, compare_response
        assert compare_response["data"]["left_run_id"] == first_run["run_id"]
        assert compare_response["data"]["right_run_id"] == second_run["run_id"]

        rejected_limit, limit_response = _cli(
            database_path,
            socket_path,
            "results",
            "read",
            input_value={"result_refs": [first_run["result_refs"][0]]},
            extra_args=("--limit", "501"),
        )
        assert rejected_limit.returncode == 2
        assert limit_response["ok"] is False
        assert limit_response["error"]["code"] == "INVALID_CONFIG"
    finally:
        _stop_process(owner)


def test_cli_timeout_returns_error_envelope_and_exit_code_three(tmp_path):
    _, database_path, socket_path, _, _, _ = _workspace(tmp_path)
    marker_path = tmp_path / "timeout-entered.json"
    release_path = tmp_path / "timeout-release"
    owner = _start_hook_owner(
        database_path,
        socket_path,
        marker_path,
        release_path,
        delay_method="studies.coverage",
    )
    try:
        completed, response = _cli(
            database_path,
            socket_path,
            "studies",
            "coverage",
            input_value={"study_id": STUDY_ID},
            extra_args=("--timeout", "0.1"),
            timeout=3,
        )
        _wait_for_file(marker_path, owner)
        assert completed.returncode == 3
        assert response["ok"] is False
        assert response["error"]["code"] == "REQUEST_TIMEOUT"
        assert response["error"]["retryable"] is True
        release_path.touch()
        healthy, healthy_response = _cli(
            database_path, socket_path, "sources", "get", SOURCE_REF,
        )
        assert healthy.returncode == 0, healthy_response
    finally:
        release_path.touch(exist_ok=True)
        _stop_process(owner)


def test_query_completes_while_execution_worker_is_paused_before_commit(tmp_path):
    _, database_path, socket_path, _, _, request = _workspace(tmp_path)
    marker_path = tmp_path / "execution-paused.json"
    release_path = tmp_path / "execution-release"
    owner = _start_hook_owner(
        database_path,
        socket_path,
        marker_path,
        release_path,
        phase="after_publish_before_db_commit",
    )
    try:
        submission = _plan_and_submit(database_path, socket_path, request, "query-during-run")
        dispatched, dispatch_response = _cli(
            database_path, socket_path, "runs", "execute", submission["run_id"],
        )
        assert dispatched.returncode == 0, dispatch_response
        assert dispatch_response["data"]["dispatch_status"] in {"QUEUED", "RUNNING"}
        marker = _wait_for_file(marker_path, owner)
        assert marker["label"] == "after_publish_before_db_commit"

        start = time.monotonic()
        query, query_response = _cli(
            database_path,
            socket_path,
            "sources",
            "get",
            SOURCE_REF,
            timeout=2,
        )
        elapsed = time.monotonic() - start
        assert query.returncode == 0, query_response
        assert elapsed < 2

        release_path.touch()
        completed = _execute_and_wait(database_path, socket_path, submission["run_id"])
        assert completed["status"] == "COMPLETE"
    finally:
        release_path.touch(exist_ok=True)
        _stop_process(owner)


def test_sigkill_after_published_receipt_recovers_same_attempt_and_result(tmp_path):
    _, database_path, socket_path, _, _, request = _workspace(tmp_path)
    marker_path = tmp_path / "receipt-published.json"
    release_path = tmp_path / "receipt-release"
    owner = _start_hook_owner(
        database_path,
        socket_path,
        marker_path,
        release_path,
        phase="after_publish_before_db_commit",
    )
    restarted_owner = None
    try:
        submission = _plan_and_submit(database_path, socket_path, request, "receipt-crash")
        dispatched, dispatch_response = _cli(
            database_path, socket_path, "runs", "execute", submission["run_id"],
        )
        assert dispatched.returncode == 0, dispatch_response
        _wait_for_file(marker_path, owner)
        crash_context = json.loads(marker_path.read_text(encoding="utf-8"))
        receipt_path = Path(crash_context["receipt_path"])
        assert receipt_path.is_file()
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        assert receipt["attempt"] == 1
        assert Path(receipt["final_bundle_uri"].removeprefix("file://")).is_dir()
        result_id = receipt["result"]["result_id"]

        owner.send_signal(signal.SIGKILL)
        owner.wait(timeout=5)
        restarted_owner = _start_owner(database_path, socket_path)
        recovered = None
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            inspected, run_response = _cli(
                database_path, socket_path, "runs", "get", submission["run_id"],
            )
            assert inspected.returncode == 0, run_response
            recovered = run_response["data"]
            if recovered["status"] == "COMPLETE":
                break
            time.sleep(0.05)
        assert recovered is not None and recovered["status"] == "COMPLETE"
        work = next(
            item for item in recovered["item_executions"]
            if "quote" in item.get("item_ids", [])
        )
        assert work["result_refs"] == [result_id]
        assert [attempt["attempt"] for attempt in work["attempts"]] == [1]
        assert work["attempts"][0]["state"] == "SUCCEEDED"
        event_states = [event["state"] for event in work["attempts"][0]["events"]]
        assert "RUNNING" in event_states and "SUCCEEDED" in event_states
    finally:
        release_path.touch(exist_ok=True)
        if owner.poll() is None:
            owner.kill()
            owner.wait(timeout=5)
        if restarted_owner is not None:
            _stop_process(restarted_owner)


def test_submit_disconnect_retry_and_registered_not_dispatched_survive_restart(tmp_path):
    _, database_path, socket_path, _, _, request = _workspace(tmp_path)
    owner = _start_owner(database_path, socket_path)
    request_id = uuid.uuid4().hex
    request_key = "disconnect-idempotency"
    try:
        plan = _call_rpc(socket_path, database_path, "runs.plan", {"request": request})
        assert plan["ok"] is True
        params = {
            "request": request,
            "request_key": request_key,
            "expected_plan_hash": plan["data"]["plan_hash"],
        }
        payload = {
            "schema_version": RPC_SCHEMA,
            "request_id": request_id,
            "database": str(database_path.resolve()),
            "method": "runs.submit",
            "params": params,
        }
        # Simulate a client that loses the response after sending its idempotent
        # registration. The retry must return the committed Run identity.
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(2)
            client.connect(str(socket_path))
            client.sendall(json.dumps(payload, separators=(",", ":")).encode() + b"\n")
        time.sleep(0.05)
        retry = _call_rpc(
            socket_path,
            database_path,
            "runs.submit",
            params,
            request_id=uuid.uuid4().hex,
        )
        assert retry["ok"] is True
        run_id = retry["data"]["run_id"]
        assert retry["data"].get("idempotent_replay") in {None, True}
        assert retry["data"]["status"] == "NOT_STARTED"
    finally:
        _stop_process(owner)

    owner = _start_owner(database_path, socket_path)
    try:
        reopened = _call_rpc(socket_path, database_path, "runs.get", {"run_id": run_id})
        assert reopened["ok"] is True
        assert reopened["data"]["run_id"] == run_id
        assert reopened["data"]["status"] == "NOT_STARTED"
        assert reopened["data"].get("item_executions")
        assert all(item["state"] in {"PENDING", "BLOCKED"}
                   for item in reopened["data"]["item_executions"])
    finally:
        _stop_process(owner)
