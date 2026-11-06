"""Persistence, ownership, and schema invariants for the DuckDB Registry."""

from __future__ import annotations

import concurrent.futures
import gc
import subprocess
import sys
import threading
import weakref
from pathlib import Path

import duckdb
import pytest

from quantsage_discovery.core import Registry, StageStatus, WorkspaceBusyError
from quantsage_discovery.core.errors import RegistrySchemaError, WorkflowError
from quantsage_discovery.core.registry import (
    _SCHEMA_V1_CHECKSUM,
    _SCHEMA_V1_DDL,
)

ROOT = Path(__file__).resolve().parents[1]


def _create_run(registry: Registry, run_id: str) -> None:
    registry.create_run(
        run_id=run_id,
        config={"seed": 3},
        config_hash="sha256:config",
        data_snapshot_id=None,
        split_id=None,
        as_of_cutoff=None,
        code_revision="test-revision",
        graph={"order": ["example"]},
        execution={"partitions": []},
    )


def _create_stage(registry: Registry, run_id: str, stage_run_id: str) -> None:
    registry.ensure_stage_run(
        run_id=run_id,
        stage_name="example",
        stage_version="1",
        partition={},
        stage_run_id=stage_run_id,
        cache_key=None,
        config_hash=None,
        code_revision=None,
    )


def test_new_registry_reopens_and_same_process_handles_share_owner(tmp_path):
    path = tmp_path / "workspace.duckdb"
    with Registry(path) as first:
        _create_run(first, "run-a")
        with pytest.raises(WorkspaceBusyError):
            Registry(tmp_path / "different.duckdb")
        with Registry(tmp_path / "subdir" / ".." / "workspace.duckdb") as second:
            assert first._owner is second._owner
            assert second.get_run("run-a")["execution"] == {"partitions": []}
        assert first.get_run("run-a")["status"] == "RUNNING"

    with Registry(path) as reopened:
        assert reopened.get_run("run-a")["config"] == {"seed": 3}
        assert reopened.get_run("run-a")["graph"] == {"order": ["example"]}

    connection = duckdb.connect(str(path), read_only=True)
    try:
        migrations = connection.execute(
            "SELECT version, checksum, duckdb_version FROM schema_migrations ORDER BY version"
        ).fetchall()
    finally:
        connection.close()
    assert [version for version, _, _ in migrations] == list(range(1, Registry.schema_version + 1))
    assert all(version == duckdb.__version__ for _, _, version in migrations)


def test_supported_v1_duckdb_schema_upgrades_to_current_version(tmp_path):
    path = tmp_path / "v1.duckdb"
    connection = duckdb.connect(str(path))
    connection.execute(
        "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, checksum VARCHAR NOT NULL, "
        "applied_at VARCHAR NOT NULL, duckdb_version VARCHAR NOT NULL)"
    )
    for statement in _SCHEMA_V1_DDL.split(";"):
        if statement.strip():
            connection.execute(statement)
    connection.execute(
        "INSERT INTO schema_migrations VALUES (1, ?, '2026-01-01T00:00:00+00:00', ?)",
        [_SCHEMA_V1_CHECKSUM, duckdb.__version__],
    )
    connection.execute(
        "INSERT INTO runs VALUES ('historical', '{}', 'hash', NULL, NULL, NULL, "
        "'rev', '{}', 'SUCCEEDED', 'then', 'then')"
    )
    connection.close()

    with Registry(path) as upgraded:
        assert upgraded.get_run("historical")["execution"] == {}
        assert upgraded.get_run("historical")["status"] == "SUCCEEDED"
        _create_run(upgraded, "old-run")
        assert upgraded.get_run("old-run")["execution"] == {"partitions": []}

    connection = duckdb.connect(str(path), read_only=True)
    try:
        columns = {row[1] for row in connection.execute("PRAGMA table_info('runs')").fetchall()}
        versions = connection.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
    finally:
        connection.close()
    assert "execution_json" in columns
    assert versions == [(1,), (2,), (3,)]


def test_registry_rejects_future_schema_and_checksum_tampering(tmp_path):
    future_path = tmp_path / "future.duckdb"
    registry = Registry(future_path)
    registry.close()
    connection = duckdb.connect(str(future_path))
    connection.execute("INSERT INTO schema_migrations VALUES (?, 'future', 'now', ?)",
                       [Registry.schema_version + 1, duckdb.__version__])
    connection.close()
    with pytest.raises(RegistrySchemaError, match="unsupported.*version"):
        Registry(future_path)

    checksum_path = tmp_path / "tampered.duckdb"
    registry = Registry(checksum_path)
    registry.close()
    connection = duckdb.connect(str(checksum_path))
    connection.execute("UPDATE schema_migrations SET checksum='tampered' WHERE version=2")
    connection.close()
    with pytest.raises(RegistrySchemaError, match="checksum mismatch"):
        Registry(checksum_path)


def test_failed_owner_transaction_rolls_back_state_and_event_together(tmp_path):
    with Registry(tmp_path / "rollback.duckdb") as registry:
        _create_run(registry, "rollback-run")
        _create_stage(registry, "rollback-run", "stage-a")

        def fail_after_state_update(connection):
            connection.execute(
                "UPDATE stage_runs SET status='FAILED' WHERE stage_run_id='stage-a'"
            )
            connection.execute(
                "INSERT INTO stage_state_events(stage_run_id,state,attempt,fields_json,created_at) "
                "VALUES('stage-a','FAILED',0,'{}','now')"
            )
            raise RuntimeError("injected failure after state and event writes")

        with pytest.raises(RuntimeError, match="injected failure"):
            registry._call(fail_after_state_update)

        row = registry.get_stage_run_by_id("stage-a")
        assert row["status"] == StageStatus.PENDING.value
        assert registry.get_stage_events("stage-a") == []


def test_state_transitions_and_events_have_explicit_order(tmp_path):
    with Registry(tmp_path / "events.duckdb") as registry:
        _create_run(registry, "events-run")
        _create_stage(registry, "events-run", "stage-a")
        assert registry.set_stage_running(
            "stage-a", cache_key="key", config_hash="config", code_revision="rev", inputs={}
        ) == 1
        registry.set_stage_failed("stage-a", error={"code": "test"})
        events = registry.get_stage_events("stage-a")
        assert [event["state"] for event in events] == ["RUNNING", "FAILED"]
        assert [event["event_id"] for event in events] == sorted(
            event["event_id"] for event in events
        )


@pytest.mark.parametrize("other_filename", ["locked.duckdb", "different.duckdb"])
def test_second_process_writer_gets_workspace_busy(tmp_path, other_filename):
    path = tmp_path / "locked.duckdb"
    other_path = tmp_path / other_filename
    code = (
        "from quantsage_discovery.core import Registry, WorkspaceBusyError; "
        f"path={str(other_path)!r}\n"
        "try:\n"
        " Registry(path)\n"
        "except WorkspaceBusyError as exc:\n"
        " print(exc.code)\n"
        "else:\n"
        " raise SystemExit('second owner unexpectedly opened workspace')"
    )
    with Registry(path):
        result = subprocess.run([sys.executable, "-c", code], cwd=ROOT, text=True,
                                capture_output=True, check=False)
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "WORKSPACE_BUSY"

    reopened = subprocess.run(
        [sys.executable, "-c",
         ("from quantsage_discovery.core import Registry; "
          f"registry=Registry({str(path)!r}); print(registry.schema_version); registry.close()")],
        cwd=ROOT, text=True, capture_output=True, check=False,
    )
    assert reopened.returncode == 0, reopened.stderr
    assert reopened.stdout.strip() == str(Registry.schema_version)


def test_multithreaded_registry_calls_are_serialized_and_persisted(tmp_path):
    path = tmp_path / "threads.duckdb"
    with Registry(path) as registry:
        def write_one(index: int) -> str:
            run_id = f"run-{index}"
            stage_id = f"stage-{index}"
            _create_run(registry, run_id)
            _create_stage(registry, run_id, stage_id)
            registry.set_stage_running(
                stage_id, cache_key=f"key-{index}", config_hash="config",
                code_revision="rev", inputs={},
            )
            registry.set_stage_failed(stage_id, error={"index": index})
            return stage_id

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            stage_ids = list(pool.map(write_one, range(24)))

        assert len({registry.get_run(f"run-{index}")["run_id"] for index in range(24)}) == 24
        event_ids = [registry.get_stage_events(stage_id)[0]["event_id"] for stage_id in stage_ids]
        assert len(set(event_ids)) == 24
        assert all(registry.get_stage_run_by_id(stage_id)["status"] == "FAILED"
                   for stage_id in stage_ids)


def test_registry_close_is_idempotent_and_closed_handle_rejects_calls(tmp_path):
    registry = Registry(tmp_path / "close.duckdb")
    registry.close()
    registry.close()
    with pytest.raises(WorkflowError, match="closed"):
        registry.get_run("missing")


def test_unknown_parent_rejected_and_forced_attempt_order_survives_reopen(tmp_path):
    path = tmp_path / "parents.duckdb"
    with Registry(path) as registry:
        with pytest.raises(WorkflowError, match="unknown run_id"):
            _create_stage(registry, "missing", "orphan")
        assert registry.get_stage_run_by_id("orphan") is None
        _create_run(registry, "known")
        _create_stage(registry, "known", "first")
        registry.ensure_stage_run(
            run_id="known", stage_name="example", stage_version="1", partition={},
            stage_run_id="second", cache_key=None, config_hash=None, code_revision=None,
            force_new=True,
        )
        assert registry.get_stage_run("known", "example", {})["stage_run_id"] == "second"
    with Registry(path) as reopened:
        assert reopened.get_stage_run("known", "example", {})["stage_run_id"] == "second"


def test_close_drains_inflight_request_and_connection_is_owned_by_worker(tmp_path):
    path = tmp_path / "drain.duckdb"
    registry = Registry(path)
    entered = threading.Event()
    release = threading.Event()

    def operation(connection):
        entered.set()
        assert release.wait(timeout=5)
        connection.execute("SELECT 1")
        return threading.get_ident()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        request = pool.submit(registry._call, operation)
        assert entered.wait(timeout=5)
        closing = pool.submit(registry.close)
        assert not closing.done()
        release.set()
        assert request.result(timeout=5) != threading.get_ident()
        closing.result(timeout=5)
    with Registry(path) as reopened:
        assert reopened.get_run("missing") is None


def test_abandoned_handle_releases_last_owner(tmp_path):
    path = tmp_path / "finalize.duckdb"
    registry = Registry(path)
    _create_run(registry, "stored")
    owner = registry._owner
    reference = weakref.ref(registry)
    del registry
    gc.collect()
    assert reference() is None
    assert not owner._thread.is_alive()
    with Registry(path) as reopened:
        assert reopened.get_run("stored") is not None


def test_failed_schema_upgrade_rolls_back_ddl_and_version_record(tmp_path, monkeypatch):
    from quantsage_discovery.core.registry import _DatabaseOwner

    path = tmp_path / "upgrade-failure.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, checksum VARCHAR, "
            "applied_at VARCHAR, duckdb_version VARCHAR)"
        )
        connection.execute(_SCHEMA_V1_DDL)
        connection.execute("INSERT INTO schema_migrations VALUES (1, ?, 'then', ?)",
                           [_SCHEMA_V1_CHECKSUM, duckdb.__version__])
    original = _DatabaseOwner._apply_migration_v2

    def fail_after_migration(self, connection):
        original(self, connection)
        raise RuntimeError("interrupted upgrade")

    with monkeypatch.context() as patch:
        patch.setattr(_DatabaseOwner, "_apply_migration_v2", fail_after_migration)
        with pytest.raises(RuntimeError, match="interrupted upgrade"):
            Registry(path)
    with duckdb.connect(str(path), read_only=True) as connection:
        assert connection.execute("SELECT version FROM schema_migrations").fetchall() == [(1,)]
        columns = {row[1] for row in connection.execute("PRAGMA table_info('runs')").fetchall()}
        assert "execution_json" not in columns
    with Registry(path) as reopened:
        _create_run(reopened, "after-retry")
        assert reopened.get_run("after-retry") is not None
