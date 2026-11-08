"""Acceptance tests for serialized 01-E execution, publication and recovery."""

from __future__ import annotations

import copy
import json
from datetime import datetime

import pyarrow.parquet as pq
import pytest

from quantsage_discovery.core.artifacts import artifact_path
from quantsage_discovery.core.errors import WorkflowError
from quantsage_discovery.core.types import ArtifactRef
from quantsage_discovery.research.errors import (
    ResearchValidationError,
    SourceScopeMissingError,
    StudyNotActiveError,
)
from tests.research_catalog_fixtures import (
    feature_set_document,
    primitive_template_document,
    provider_document,
    recipe_item,
)
from tests.research_execution_fixtures import (
    INSTRUMENT,
    make_execution_environment,
)
from tests.research_planning_fixtures import (
    METRIC_MEAN,
    make_plan_request,
    make_planning_feature_set_document,
    make_planning_study_document,
    split_ref_from_fixture,
)


class InjectedCrash(BaseException):
    """Model process death so the executor cannot record a normal failure."""


def _submit(service, request, key: str) -> dict:
    plan = service.plan_run(request)
    return service.submit_run(
        request,
        request_key=key,
        expected_plan_hash=plan["plan_hash"],
    )


def _execution(run: dict, item_id: str) -> dict:
    matches = [work for work in run["item_executions"] if item_id in work.get("item_ids", [])]
    assert len(matches) == 1, (item_id, run["item_executions"])
    return matches[0]


def _executions(run: dict, item_id: str) -> list[dict]:
    return [work for work in run["item_executions"] if item_id in work.get("item_ids", [])]


def _inventory_item(run: dict, item_id: str) -> dict:
    matches = [item for item in run["inventory"] if item["item_id"] == item_id]
    assert len(matches) == 1, (item_id, run["inventory"])
    return matches[0]


def _inventory_selection(item: dict) -> str:
    return item.get("selection", item.get("status"))


def _artifact_for(run: dict, item_id: str) -> dict:
    artifacts = _execution(run, item_id).get("artifacts", [])
    assert artifacts, f"{item_id} has no published artifact"
    assert len(artifacts) == 1
    return artifacts[0]


def _rows_for(run: dict, item_id: str) -> list[dict]:
    return pq.read_table(artifact_path(_artifact_for(run, item_id)["uri"])).to_pylist()


def _all_indexed_results(registry, run_id: str) -> list[dict]:
    return registry.list_feature_results(run_id=run_id)


def _summary_status(run: dict) -> str:
    summary = run["summary"]
    return summary["status"] if isinstance(summary, dict) else summary


def _two_root_features():
    return feature_set_document(
        feature_set_id="planning_features",
        items=[
            recipe_item("quote", parameters={"scale": 2}),
            recipe_item("other", parameters={"scale": 3}),
        ],
    )


def _seed_window_cache(tmp_path):
    registry, service, _, _, request = make_execution_environment(tmp_path)
    request["selected_item_ids"] = ["window"]
    request["scope"]["start"] = "2024-06-03T13:09:00+00:00"
    request["allowed_steps"] = [3, 4]
    request["missing_dependencies"] = "compute_within_allowed_steps"
    submitted = _submit(service, request, "seed-window-cache")
    seeded = service.execute_run(submitted["run_id"])
    assert _summary_status(seeded) == "COMPLETE"
    assert _execution(seeded, "window")["state"] == "SUCCEEDED"
    return registry, service, request, seeded


def test_arithmetic_exec_reads_serialized_parquet_and_publishes_exact_scoped_values(tmp_path):
    rows = [
        {"event_time": "2024-06-03T13:10:10+00:00", "value": 2.0},
        {"event_time": "2024-06-03T13:10:20+00:00", "value": 3.0},
        # This value is in the authorized source but outside this Run's output scope.
        {"event_time": "2024-06-03T13:12:10+00:00", "value": 100.0},
    ]
    registry, service, source, _, request = make_execution_environment(
        tmp_path,
        rows=rows,
        feature_set_document=_two_root_features(),
    )
    try:
        source_manifest = json.loads(source.paths["manifest"].read_text(encoding="utf-8"))
        raw = pq.read_table(artifact_path(source_manifest["data_artifacts"][0]["uri"]))
        assert raw.column("value").to_pylist() == [2.0, 3.0, 100.0]

        submitted = _submit(service, request, "exact-parquet")
        run = service.execute_run(submitted["run_id"])

        assert _summary_status(run) == "COMPLETE"
        quote = _execution(run, "quote")
        assert quote["state"] == "SUCCEEDED"
        output = _rows_for(run, "quote")
        assert [row["value"] for row in output] == [4.0, 6.0]
        assert [row["event_time"].isoformat() for row in output] == [
            "2024-06-03T13:10:10+00:00",
            "2024-06-03T13:10:20+00:00",
        ]
        output_schema = pq.read_schema(artifact_path(_artifact_for(run, "quote")["uri"]))
        if "available_time" in output_schema.names:
            assert all(row["available_time"] is None for row in output)
        assert _inventory_selection(_inventory_item(run, "other")) == "NOT_SELECTED"
        result = _all_indexed_results(registry, run["run_id"])[0]
        assert result["producer_run_id"] == run["run_id"]
        assert result["producer_work_id"] == quote["work_key"]
        assert result["quality"]["n_total"] == 2
        assert result["quality"]["n_valid"] == 2
        assert result["time_metadata"]["availability_mode"] == "research_only"
    finally:
        registry.close()


def test_execution_follows_frozen_dependency_dag_and_scope_without_running_unselected_item(
    tmp_path,
):
    rows = [
        {"event_time": "2024-06-03T13:07:10+00:00", "value": 1.0},
        {"event_time": "2024-06-03T13:08:10+00:00", "value": 2.0},
        {"event_time": "2024-06-03T13:09:10+00:00", "value": 3.0},
        {"event_time": "2024-06-03T13:10:10+00:00", "value": 4.0},
        {"event_time": "2024-06-03T13:12:10+00:00", "value": 99.0},
    ]
    registry, service, _, _, request = make_execution_environment(tmp_path, rows=rows)
    try:
        request["selected_item_ids"] = ["window"]
        preview = service.plan_run(request)
        planned_items = {
            item_id
            for work in preview["work"]
            for item_id in work["item_ids"]
        }
        assert planned_items == {"quote", "window"}
        assert "summary" not in planned_items

        submitted = service.submit_run(
            request,
            request_key="frozen-dag-scope",
            expected_plan_hash=preview["plan_hash"],
        )
        run = service.execute_run(submitted["run_id"])

        actual_work = {work["work_key"] for work in run["item_executions"]
                       if work["state"] in {"SUCCEEDED", "CACHED"}}
        expected_work = {work["work_key"] for work in run["plan"]["work"]
                         if work["state"] == "PENDING"}
        assert actual_work == expected_work
        assert run["selected_item_ids"] == ["window"]
        assert datetime.fromisoformat(run["scope"]["start"]) == datetime.fromisoformat(
            request["scope"]["start"]
        )
        assert datetime.fromisoformat(run["scope"]["end"]) == datetime.fromisoformat(
            request["scope"]["end"]
        )
        assert not any(
            "summary" in work.get("item_ids", [])
            and work["state"] in {"SUCCEEDED", "CACHED"}
            for work in run["item_executions"]
        )
        assert _inventory_selection(_inventory_item(run, "summary")) == "NOT_SELECTED"
        for work in run["item_executions"]:
            if work["state"] not in {"SUCCEEDED", "CACHED"}:
                continue
            start = work["partition_scope"]["start"]
            end = work["partition_scope"]["end"]
            for row in _rows_for(run, work["item_ids"][0]):
                assert datetime.fromisoformat(start) <= row["event_time"]
                assert row["event_time"] < datetime.fromisoformat(end)
                assert row["value"] != 99.0
    finally:
        registry.close()


def test_trailing_statistic_at_t_does_not_include_later_values_inside_the_same_run_scope(tmp_path):
    feature_set = make_planning_feature_set_document()
    summary_item = next(item for item in feature_set["items"] if item["item_id"] == "summary")
    summary_item["recipe"]["metrics"] = [METRIC_MEAN]
    summary_item["recipe"]["sampling"] = {
        "axis": "event_time",
        "interval": "10s",
        "evaluation_stride": "10s",
    }
    rows = [
        {"event_time": "2024-06-03T13:09:30+00:00", "value": 2.0},
        {"event_time": "2024-06-03T13:10:10+00:00", "value": 4.0},
        # Same authorized scope, later than t=13:10:10; must not leak backward.
        {"event_time": "2024-06-03T13:10:20+00:00", "value": 100.0},
    ]
    registry, service, _, _, request = make_execution_environment(
        tmp_path,
        rows=rows,
        feature_set_document=feature_set,
    )
    try:
        request["selected_item_ids"] = ["summary"]
        submitted = _submit(service, request, "trailing-no-future")
        run = service.execute_run(submitted["run_id"])
        summary_work = _executions(run, "summary")
        assert len(summary_work) == 1
        output = _rows_for(run, "summary")
        at_t = [row for row in output
                if row["event_time"].isoformat() == "2024-06-03T13:10:10+00:00"]
        assert len(at_t) == 1
        assert at_t[0]["value"] == pytest.approx(3.0)
        assert at_t[0]["value"] != 52.0
    finally:
        registry.close()


def test_step_limited_run_reuses_verified_cache_for_unselected_dependencies(tmp_path):
    registry, service, seed_request, seeded = _seed_window_cache(tmp_path)
    try:
        request = copy.deepcopy(seed_request)
        request["selected_item_ids"] = ["summary"]
        request["scope"]["start"] = "2024-06-03T13:10:00+00:00"
        request["allowed_steps"] = [5]
        request["missing_dependencies"] = "block"

        preview = service.plan_run(request)
        window_work = [work for work in preview["work"] if "window" in work["item_ids"]]
        assert len(window_work) == 1
        assert window_work[0]["cache_only"] is True
        assert window_work[0]["cache_candidate"] is True
        assert window_work[0]["candidate_result_ref"] == _execution(
            seeded, "window"
        )["result_refs"][0]

        submitted = service.submit_run(
            request,
            request_key="step-five-with-cache",
            expected_plan_hash=preview["plan_hash"],
        )
        run = service.execute_run(submitted["run_id"])
        cached = _execution(run, "window")
        assert cached["state"] == "CACHED"
        assert cached["cache_source_run_id"] == seeded["run_id"]
        assert cached["result_refs"] == _execution(seeded, "window")["result_refs"]
        assert all(work["state"] == "SUCCEEDED" for work in _executions(run, "summary"))
        assert _inventory_selection(_inventory_item(run, "quote")) == "NOT_SELECTED"
        assert _inventory_selection(_inventory_item(run, "window")) == "DEPENDENCY_ONLY"
        assert _inventory_selection(_inventory_item(run, "summary")) == "REQUESTED"
    finally:
        registry.close()


def test_invalidated_cache_only_dependency_blocks_without_widening_allowed_steps(tmp_path):
    registry, service, seed_request, seeded = _seed_window_cache(tmp_path)
    try:
        request = copy.deepcopy(seed_request)
        request["selected_item_ids"] = ["summary"]
        request["scope"]["start"] = "2024-06-03T13:10:00+00:00"
        request["allowed_steps"] = [5]
        request["missing_dependencies"] = "block"
        preview = service.plan_run(request)
        dependency_plan = next(work for work in preview["work"]
                               if "window" in work["item_ids"])
        assert dependency_plan["cache_candidate"] is True
        submitted = service.submit_run(
            request,
            request_key="step-five-with-lost-cache",
            expected_plan_hash=preview["plan_hash"],
        )

        artifact_path(_artifact_for(seeded, "window")["uri"]).unlink()
        blocked = service.execute_run(submitted["run_id"])
        window_work = _execution(blocked, "window")
        assert window_work["state"] == "BLOCKED"
        assert window_work["attempts"] == []
        assert all(work["state"] == "BLOCKED" for work in _executions(blocked, "summary"))
        assert all(work["attempts"] == [] for work in _executions(blocked, "summary"))
        assert _executions(blocked, "quote") == []
        assert _all_indexed_results(registry, blocked["run_id"]) == []
        assert blocked["plan"]["allowed_steps"] == [5]
    finally:
        registry.close()


def test_empty_selection_is_no_work_but_zero_rows_and_all_na_are_published_results(tmp_path):
    registry, service, _, _, request = make_execution_environment(tmp_path / "empty")
    try:
        request["selected_item_ids"] = []
        submitted = _submit(service, request, "explicit-empty")
        run = service.execute_run(submitted["run_id"])
        assert _summary_status(run) == "NO_WORK"
        assert run["item_executions"] == []
        assert _all_indexed_results(registry, run["run_id"]) == []
    finally:
        registry.close()

    for name, rows, expected_rows in (
        ("all-na", [
            {"event_time": "2024-06-03T13:10:10+00:00", "value": None},
            {"event_time": "2024-06-03T13:10:20+00:00", "value": None},
        ], 2),
        ("zero-rows", [], 0),
    ):
        registry, service, _, _, request = make_execution_environment(
            tmp_path / name,
            rows=rows,
            feature_set_document=_two_root_features(),
        )
        try:
            submitted = _submit(service, request, name)
            run = service.execute_run(submitted["run_id"])
            assert _summary_status(run) == "COMPLETE"
            work = _execution(run, "quote")
            assert work["state"] == "SUCCEEDED"
            result = _all_indexed_results(registry, run["run_id"])[0]
            assert result["quality"]["n_total"] == expected_rows
            assert result["quality"]["n_valid"] == 0
            output = _rows_for(run, "quote")
            assert len(output) == expected_rows
            assert all(row["value"] is None for row in output)
        finally:
            registry.close()


def test_unenforceable_hard_resource_budget_is_rejected_before_run_registration(tmp_path):
    registry, service, _, _, request = make_execution_environment(tmp_path)
    try:
        request["budget"]["max_cpu_seconds"] = 1
        before = registry.list_research_runs()
        with pytest.raises(ResearchValidationError) as plan_error:
            service.plan_run(request)
        assert plan_error.value.code == "INVALID_CONFIG"
        with pytest.raises(ResearchValidationError) as submit_error:
            service.submit_run(request, request_key="unsupported-hard-budget")
        assert submit_error.value.code == "INVALID_CONFIG"
        assert registry.list_research_runs() == before
    finally:
        registry.close()


def test_prepublication_failure_retries_only_failed_work_and_preserves_attempt_history(tmp_path):
    registry, service, _, _, request = make_execution_environment(
        tmp_path,
        feature_set_document=_two_root_features(),
    )
    request["selected_item_ids"] = ["quote", "other"]
    try:
        submitted = _submit(service, request, "failure-then-resume")
        injected = {"used": False}

        def fail_before_first_publish(phase, context):
            if phase == "before_publish" and not injected["used"]:
                injected["used"] = True
                raise RuntimeError("synthetic prepublication failure")

        partial = service.execute_run(submitted["run_id"], fault_hook=fail_before_first_publish)
        assert _summary_status(partial) == "PARTIAL"
        states = [work["state"] for work in partial["item_executions"]]
        assert states.count("FAILED") == 1
        assert states.count("SUCCEEDED") == 1
        failed_key = next(work["work_key"] for work in partial["item_executions"]
                          if work["state"] == "FAILED")
        succeeded_key = next(work["work_key"] for work in partial["item_executions"]
                             if work["state"] == "SUCCEEDED")

        recovered = service.resume_run(submitted["run_id"])
        assert _summary_status(recovered) == "COMPLETE"
        by_key = {work["work_key"]: work for work in recovered["item_executions"]}
        assert len(by_key[failed_key]["attempts"]) == 2
        assert len(by_key[succeeded_key]["attempts"]) == 1
        assert [attempt["state"] for attempt in by_key[failed_key]["attempts"]] == [
            "FAILED", "SUCCEEDED",
        ]
        assert len(_all_indexed_results(registry, submitted["run_id"])) == 2
    finally:
        registry.close()


def test_failed_old_run_can_resume_from_later_run_cache_without_rewriting_failed_attempt(tmp_path):
    registry, service, _, _, request = make_execution_environment(
        tmp_path,
        feature_set_document=_two_root_features(),
    )
    try:
        old_submission = _submit(service, request, "old-failed-run")

        def fail_before_publish(phase, context):
            if phase == "before_publish":
                raise RuntimeError("fail first attempt before publication")

        failed = service.execute_run(old_submission["run_id"], fault_hook=fail_before_publish)
        old_work = _execution(failed, "quote")
        assert old_work["state"] == "FAILED"
        assert len(old_work["attempts"]) == 1
        assert old_work["attempts"][0]["state"] == "FAILED"
        assert old_work["result_refs"] == []

        producer_submission = _submit(service, request, "new-producer-run")
        produced = service.execute_run(producer_submission["run_id"])
        producer_work = _execution(produced, "quote")
        assert producer_work["state"] == "SUCCEEDED"
        result_id = producer_work["result_refs"][0]

        resumed = service.resume_run(old_submission["run_id"])
        resumed_work = _execution(resumed, "quote")
        assert resumed_work["state"] == "CACHED"
        assert resumed_work["cache_source_run_id"] == produced["run_id"]
        assert resumed_work["result_refs"] == [result_id]
        assert len(resumed_work["attempts"]) == 1
        assert resumed_work["attempts"][0]["attempt"] == 1
        assert resumed_work["attempts"][0]["state"] == "FAILED"
        cache_event = registry.get_stage_events(resumed_work["stage_run_id"])[-1]
        assert cache_event["state"] == "CACHED"
        assert cache_event["attempt"] == 0
        assert cache_event["fields"]["result_id"] == result_id
        cached_result = registry.get_feature_result(result_id)
        assert cached_result["producer_run_id"] == produced["run_id"]
    finally:
        registry.close()


def test_prepublication_crash_reconciles_running_attempt_and_resumes_only_unfinished_work(tmp_path):
    registry, service, _, _, request = make_execution_environment(
        tmp_path,
        feature_set_document=_two_root_features(),
    )
    request["selected_item_ids"] = ["quote", "other"]
    try:
        submitted = _submit(service, request, "crash-before-publish")

        def crash_before_publish(phase, context):
            if phase == "before_publish":
                raise InjectedCrash(phase)

        with pytest.raises(InjectedCrash):
            service.execute_run(submitted["run_id"], fault_hook=crash_before_publish)

        interrupted = service.get_run(submitted["run_id"])
        assert _all_indexed_results(registry, submitted["run_id"]) == []
        assert all(not work.get("artifacts") for work in interrupted["item_executions"])
        crashed = [work for work in interrupted["item_executions"]
                   if work["attempts"] and work["attempts"][-1]["state"] == "RUNNING"]
        assert len(crashed) == 1
        crashed_key = crashed[0]["work_key"]
        untouched_key = next(work["work_key"] for work in interrupted["item_executions"]
                             if work["work_key"] != crashed_key)

        recovered = service.resume_run(submitted["run_id"])
        assert _summary_status(recovered) == "COMPLETE"
        by_key = {work["work_key"]: work for work in recovered["item_executions"]}
        assert len(by_key[crashed_key]["attempts"]) == 2
        assert by_key[crashed_key]["attempts"][0]["state"] in {"FAILED", "INTERRUPTED"}
        assert by_key[crashed_key]["attempts"][1]["state"] == "SUCCEEDED"
        assert len(by_key[untouched_key]["attempts"]) == 1
        assert by_key[untouched_key]["attempts"][0]["state"] == "SUCCEEDED"
        assert len(_all_indexed_results(registry, submitted["run_id"])) == 2
    finally:
        registry.close()


@pytest.mark.parametrize("phase", ["after_publish_before_db_commit", "before_db_commit"])
def test_published_receipt_recovers_once_even_if_the_same_receipt_is_discovered_twice(
    tmp_path, phase,
):
    registry, service, _, _, request = make_execution_environment(
        tmp_path,
        feature_set_document=_two_root_features(),
    )
    try:
        submitted = _submit(service, request, f"receipt-{phase}")

        def crash_after_publication(current_phase, context):
            if current_phase == phase:
                raise InjectedCrash(phase)

        with pytest.raises(InjectedCrash):
            service.execute_run(submitted["run_id"], fault_hook=crash_after_publication)

        assert _all_indexed_results(registry, submitted["run_id"]) == []
        receipt_root = tmp_path / "artifacts" / ".receipts" / "research" / submitted["run_id"]
        receipts = list(receipt_root.glob("*.json"))
        assert len(receipts) == 1
        durable_receipt = json.loads(receipts[0].read_text(encoding="utf-8"))
        assert durable_receipt["schema_version"] == "publication_receipt_v1"

        restored = service.recover_runs(submitted["run_id"])
        assert _summary_status(restored) == "COMPLETE"
        results = _all_indexed_results(registry, submitted["run_id"])
        assert len(results) == 1
        assert len(_execution(restored, "quote")["attempts"]) == 1
        again = service.recover_runs(submitted["run_id"])
        before_again = _execution(restored, "quote")
        after_again = _execution(again, "quote")
        for field in ("state", "result_refs", "artifacts", "attempts", "producer_run_id"):
            assert after_again[field] == before_again[field]
        assert len(_all_indexed_results(registry, submitted["run_id"])) == 1
    finally:
        registry.close()


def test_database_commit_crash_is_idempotent_and_does_not_create_a_new_producer(tmp_path):
    registry, service, _, _, request = make_execution_environment(
        tmp_path,
        feature_set_document=_two_root_features(),
    )
    try:
        submitted = _submit(service, request, "after-db-commit")

        def crash_after_commit(phase, context):
            if phase == "after_db_commit":
                raise InjectedCrash(phase)

        with pytest.raises(InjectedCrash):
            service.execute_run(submitted["run_id"], fault_hook=crash_after_commit)
        first = _all_indexed_results(registry, submitted["run_id"])
        assert len(first) == 1
        assert first[0]["producer_run_id"] == submitted["run_id"]

        recovered = service.recover_runs(submitted["run_id"])
        assert _summary_status(recovered) == "COMPLETE"
        final = _all_indexed_results(registry, submitted["run_id"])
        assert len(final) == 1
        assert final[0]["result_id"] == first[0]["result_id"]
        assert final[0]["producer_run_id"] == submitted["run_id"]
        assert len(_execution(recovered, "quote")["attempts"]) == 1
    finally:
        registry.close()


def test_invalid_receipt_is_quarantined_and_never_adopted(tmp_path):
    registry, service, _, _, request = make_execution_environment(
        tmp_path,
        feature_set_document=_two_root_features(),
    )
    try:
        submitted = _submit(service, request, "invalid-receipt")
        receipt_dir = tmp_path / "artifacts" / ".receipts" / "research" / submitted["run_id"]
        receipt_dir.mkdir(parents=True)
        receipt_path = receipt_dir / "invalid-work.attempt-1.json"
        receipt_path.write_text('{"schema_version":"not_a_publication_receipt_v1"}\n',
                          encoding="utf-8")
        orphan_dir = tmp_path / "artifacts" / ".receipts" / "research" / "orphan-run"
        orphan_dir.mkdir(parents=True)
        orphan_path = orphan_dir / "orphan.attempt-1.json"
        orphan_payload = '{"schema_version":"not_a_publication_receipt_v1"}\n'
        orphan_path.write_text(orphan_payload, encoding="utf-8")

        service.recover_runs()

        quarantined = tmp_path / "artifacts" / ".receipts" / "quarantine" / "research" \
            / submitted["run_id"] / receipt_path.name
        assert quarantined.exists()
        assert quarantined.read_text(encoding="utf-8") == (
            '{"schema_version":"not_a_publication_receipt_v1"}\n'
        )
        assert list(quarantined.parent.glob("*.reason.json")) or list(
            quarantined.parent.glob("*reason*.json")
        )
        orphan_quarantined = tmp_path / "artifacts" / ".receipts" / "quarantine" / "research" \
            / "orphan-run" / orphan_path.name
        assert orphan_quarantined.exists()
        assert orphan_quarantined.read_text(encoding="utf-8") == orphan_payload
        assert list(orphan_quarantined.parent.glob("*.reason.json")) or list(
            orphan_quarantined.parent.glob("*reason*.json")
        )
        assert _all_indexed_results(registry, submitted["run_id"]) == []
        assert service.get_run(submitted["run_id"])["status"] == "NOT_STARTED"
    finally:
        registry.close()


def test_cache_reuses_actual_producer_and_missing_file_retries_with_new_attempt(tmp_path):
    registry, service, _, _, request = make_execution_environment(
        tmp_path,
        feature_set_document=_two_root_features(),
    )
    try:
        first_submitted = _submit(service, request, "producer-run")
        first = service.execute_run(first_submitted["run_id"])
        first_work = _execution(first, "quote")
        first_result = _all_indexed_results(registry, first["run_id"])[0]
        assert first_result["producer_run_id"] == first["run_id"]

        second_submitted = _submit(service, request, "cache-consumer-run")
        second = service.execute_run(second_submitted["run_id"])
        second_work = _execution(second, "quote")
        assert second_work["state"] == "CACHED"
        assert second_work["cache_source_run_id"] == first["run_id"]
        assert second_work["result_refs"] == first_work["result_refs"]
        assert _rows_for(second, "quote") == _rows_for(first, "quote")
        linked_results = _all_indexed_results(registry, second["run_id"])
        assert len(linked_results) == 1
        assert linked_results[0]["producer_run_id"] == first["run_id"]
        cached_result = registry.get_feature_result(second_work["result_refs"][0])
        assert cached_result["producer_run_id"] == first["run_id"]

        lost_path = artifact_path(_artifact_for(first, "quote")["uri"])
        lost_path.unlink()
        repaired = service.resume_run(first["run_id"])
        repaired_work = _execution(repaired, "quote")
        assert repaired_work["state"] == "SUCCEEDED"
        assert len(repaired_work["attempts"]) == 2
        result_refs = repaired_work["result_refs"]
        assert len(result_refs) == 2
        old_result = registry.get_feature_result(first_work["result_refs"][0])
        new_result = registry.get_feature_result(result_refs[-1])
        assert old_result["producer_run_id"] == first["run_id"]
        assert new_result["producer_run_id"] == first["run_id"]
        assert new_result["producer_work_id"] == repaired_work["work_key"]
        health_text = json.dumps(old_result.get("health_checks", [])).lower()
        assert "missing" in health_text or "invalid" in health_text
        assert artifact_path(repaired_work["artifacts"][0]["uri"]).exists()
    finally:
        registry.close()


def test_new_implementation_identity_does_not_reuse_old_cache_and_reuses_its_own(tmp_path):
    registry, service, source, _, request = make_execution_environment(tmp_path)
    try:
        old_submission = _submit(service, request, "old-implementation")
        old_run = service.execute_run(old_submission["run_id"])
        old_work = _execution(old_run, "quote")
        assert old_work["state"] == "SUCCEEDED"
        old_feature_ref = old_work["feature_ref"]
        old_result_id = old_work["result_refs"][0]

        provider = provider_document(provider_id="arith_v2", version="1")
        provider["implementation_version"] = "2.0"
        service.register_provider(provider)
        service.register_template(primitive_template_document(
            template_id="scaled_value_v2",
            version=1,
            formula_ref="synthetic.scale@2",
            provider_ref="arith_v2@1",
        ))
        new_document = feature_set_document(
            feature_set_id="remediated_v2",
            items=[recipe_item(
                "quote",
                template_ref="scaled_value_v2@1",
                parameters={"scale": 2},
            )],
        )
        draft = service.save_feature_set(new_document)
        frozen = service.freeze_feature_set(
            "remediated_v2@1", expected_revision=draft["draft_revision"],
        )
        new_feature_ref = next(
            output["feature_ref"]
            for item in frozen["resolved_items"]
            if item["item_id"] == "quote"
            for output in item["outputs"]
        )
        assert new_feature_ref != old_feature_ref

        study = make_planning_study_document(
            study_id="remediated_study_v2",
            feature_set_ref="remediated_v2@1",
            source_ref="planning_source@1",
        )
        service.create_study(study)
        new_request = make_plan_request(
            split_ref_from_fixture(source),
            study_id="remediated_study_v2",
            feature_set_ref="remediated_v2@1",
            selected_item_ids=["quote"],
            include_selection=True,
        )
        preview = service.plan_run(new_request)
        quote_plan = next(work for work in preview["work"] if "quote" in work["item_ids"])
        assert quote_plan["feature_ref"] == new_feature_ref
        assert quote_plan["cache_candidate"] is False

        new_submission = service.submit_run(
            new_request,
            request_key="new-implementation",
            expected_plan_hash=preview["plan_hash"],
        )
        new_run = service.execute_run(new_submission["run_id"])
        new_work = _execution(new_run, "quote")
        assert new_work["state"] == "SUCCEEDED"
        assert new_work["result_refs"][0] != old_result_id
        assert new_work["feature_ref"] == new_feature_ref

        reuse_preview = service.plan_run(new_request)
        reuse_quote = next(
            work for work in reuse_preview["work"] if "quote" in work["item_ids"]
        )
        assert reuse_quote["cache_candidate"] is True
        assert reuse_quote["candidate_result_ref"] == new_work["result_refs"][0]
        reused_submission = service.submit_run(
            new_request,
            request_key="new-implementation-reuse",
            expected_plan_hash=reuse_preview["plan_hash"],
        )
        reused_run = service.execute_run(reused_submission["run_id"])
        reused_work = _execution(reused_run, "quote")
        assert reused_work["state"] == "CACHED"
        assert reused_work["cache_source_run_id"] == new_run["run_id"]
        assert reused_work["result_refs"] == new_work["result_refs"]
    finally:
        registry.close()


def test_restored_earlier_cache_becomes_active_without_erasing_result_history(tmp_path):
    registry, service, _, _, request = make_execution_environment(
        tmp_path,
        feature_set_document=_two_root_features(),
    )
    try:
        submitted = _submit(service, request, "restore-earlier-cache")
        run_a = service.execute_run(submitted["run_id"])
        work_a = _execution(run_a, "quote")
        result_a_id = work_a["result_refs"][0]
        result_a = registry.get_feature_result(result_a_id)
        artifact_a = result_a["artifacts"][0]["artifact_ref"]
        path_a = artifact_path(artifact_a["uri"])
        payload_a = path_a.read_bytes()

        path_a.unlink()
        run_b = service.resume_run(run_a["run_id"])
        work_b = _execution(run_b, "quote")
        assert work_b["state"] == "SUCCEEDED"
        assert len(work_b["attempts"]) == 2
        results_after_b = _all_indexed_results(registry, run_a["run_id"])
        by_attempt = {result["producer_attempt"]: result for result in results_after_b}
        assert set(by_attempt) == {1, 2}
        result_b_id = by_attempt[2]["result_id"]
        artifact_b = by_attempt[2]["artifacts"][0]["artifact_ref"]
        path_b = artifact_path(artifact_b["uri"])
        assert path_b != path_a
        assert path_b.exists()

        # Restoring the earlier exact artifact makes A healthy again while B is unavailable.
        path_a.write_bytes(payload_a)
        path_b.unlink()
        restored = service.resume_run(run_a["run_id"])
        work_restored = _execution(restored, "quote")

        assert work_restored["state"] == "CACHED"
        assert work_restored["result_refs"] == [result_a_id, result_b_id]
        assert work_restored["artifacts"][0]["uri"] == artifact_a["uri"]
        assert work_restored["producer_run_id"] == run_a["run_id"]
        assert work_restored["needs_repair"] is False
        assert len(work_restored["attempts"]) == 2
        assert [attempt["state"] for attempt in work_restored["attempts"]] == [
            "SUCCEEDED", "SUCCEEDED",
        ]
        assert artifact_path(work_restored["artifacts"][0]["uri"]).read_bytes() == payload_a
        assert len(_all_indexed_results(registry, run_a["run_id"])) == 2

        result_b = registry.get_feature_result(result_b_id)
        stage = registry.get_stage_run_by_id(work_restored["stage_run_id"])
        events_before = registry.get_stage_events(work_restored["stage_run_id"])
        links_before = registry.list_run_result_links(run_a["run_id"])
        result_b_ref = ArtifactRef.from_dict(result_b["artifacts"][0]["artifact_ref"])
        with pytest.raises(WorkflowError, match="different cached result requires explicit work requeue"):
            registry.link_cached_feature_result(
                run_id=run_a["run_id"],
                work_key=work_restored["work_key"],
                stage_run_id=stage["stage_run_id"],
                result=result_b,
                artifacts={"result": result_b_ref},
                metrics={
                    "n_total": result_b["quality"]["n_total"],
                    "n_valid": result_b["quality"]["n_valid"],
                    "processed_scope": "complete",
                },
                cache_source_run_id=result_b["producer_run_id"],
            )
        assert registry.get_stage_run_by_id(stage["stage_run_id"])["status"] == "CACHED"
        assert registry.get_stage_events(stage["stage_run_id"]) == events_before
        assert registry.list_run_result_links(run_a["run_id"]) == links_before
        still_active = service.get_run(run_a["run_id"])
        active_work = _execution(still_active, "quote")
        assert active_work["artifacts"][0]["uri"] == artifact_a["uri"]
        assert active_work["result_refs"] == [result_a_id, result_b_id]
    finally:
        registry.close()


def test_owner_rejects_feature_result_scope_or_feature_mismatch_without_any_commit(tmp_path):
    registry, service, _, frozen, request = make_execution_environment(
        tmp_path,
        feature_set_document=_two_root_features(),
    )
    try:
        submitted = _submit(service, request, "registry-boundary-check")
        run = service.execute_run(submitted["run_id"])
        quote_work = _execution(run, "quote")
        quote_result = registry.get_feature_result(quote_work["result_refs"][0])
        stage = next(row for row in registry.get_run_stages(run["run_id"])
                     if row["stage_run_id"] == quote_work["stage_run_id"])
        artifact_refs = {
            alias: ArtifactRef.from_dict(reference)
            for alias, reference in stage["artifacts"].items()
        }
        expected_result_ids = [row["result_id"] for row in registry.list_feature_results()]
        expected_links = registry.list_run_result_links(run["run_id"])
        expected_events = registry.get_stage_events(quote_work["stage_run_id"])

        other_output = next(
            output for item in frozen["resolved_items"] if item["item_id"] == "other"
            for output in item["outputs"]
        )
        for suffix, mutation in (
            (
                "scope",
                lambda result: result["scope"].update(
                    end="2024-06-03T14:00:00+00:00",
                ),
            ),
            (
                "feature",
                lambda result: result.update(
                    feature_ref=other_output["feature_ref"],
                    definition_hash=other_output["definition_hash"],
                ),
            ),
        ):
            tampered = copy.deepcopy(quote_result)
            tampered.pop("health_checks", None)
            tampered["result_id"] = f"{quote_result['result_id']}-wrong-{suffix}"
            mutation(tampered)
            with pytest.raises(WorkflowError):
                registry.commit_research_result(
                    run_id=run["run_id"],
                    work_key=quote_work["work_key"],
                    stage_run_id=quote_work["stage_run_id"],
                    result=tampered,
                    artifacts=artifact_refs,
                    metrics=stage["metrics"],
                    receipt_id=f"bad-result-{suffix}",
                    attempt=stage["attempt"],
                )
            assert [row["result_id"] for row in registry.list_feature_results()] == \
                expected_result_ids
            assert registry.list_run_result_links(run["run_id"]) == expected_links
            assert registry.get_stage_events(quote_work["stage_run_id"]) == expected_events
            assert registry.get_stage_run_by_id(quote_work["stage_run_id"])["status"] == \
                "SUCCEEDED"
    finally:
        registry.close()


def test_resume_rechecks_current_scope_and_paused_study_access(tmp_path):
    for name, error in (("scope-shrink", SourceScopeMissingError),
                        ("paused", StudyNotActiveError)):
        registry, service, _, _, request = make_execution_environment(
            tmp_path / name,
            feature_set_document=_two_root_features(),
        )
        try:
            submitted = _submit(service, request, name)
            if name == "scope-shrink":
                study = service.get_study("planning_study")
                limits = copy.deepcopy(study["limits"])
                limits["allowed_scope"] = [{
                    "instruments": [INSTRUMENT],
                    "start": "2024-06-03T21:00:00+08:00",
                    "end": "2024-06-03T21:10:30+08:00",
                    "session_filter": ["night"],
                }]
                changes = {"limits": limits}
            else:
                changes = {"status": "paused"}
            service.update_study(
                "planning_study",
                changes,
                expected_revision=1,
                actor={"kind": "human", "id": "fixture-reviewer"},
                reason=f"Exercise resume boundary: {name}.",
            )
            with pytest.raises(error):
                service.resume_run(submitted["run_id"])
            assert service.get_run(submitted["run_id"])["status"] == "NOT_STARTED"
        finally:
            registry.close()
