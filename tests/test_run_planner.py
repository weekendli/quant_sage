"""Persisted Run planning, authorization, history and idempotency acceptance."""

from __future__ import annotations

import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pytest

from quantsage_discovery.core import Registry
from quantsage_discovery.research.errors import (
    BudgetExceededError,
    IdempotencyConflictError,
    PlanChangedError,
    SourceScopeMissingError,
    StudyNotActiveError,
    UnknownReferenceError,
)
from quantsage_discovery.research.service import ResearchService
from tests.research_planning_fixtures import (
    EXCHANGE,
    FEATURE_SET_REF,
    INSTRUMENT,
    make_plan_request,
    make_planning_feature_set_document,
    make_planning_source_fixture,
    make_planning_study_document,
    register_planning_catalog,
    split_ref_from_fixture,
    unlisted_split_ref,
)


def _environment(
    tmp_path,
    *,
    include_pending: bool = False,
    missing_dates: tuple[str, ...] = (),
    missing_exchange: bool = False,
    study_id: str = "planning_study",
    feature_set_ref: str = FEATURE_SET_REF,
    split_start: str | None = None,
    split_end: str | None = None,
    split_role: str | None = None,
):
    registry = Registry(tmp_path / "research.duckdb")
    service = ResearchService(registry)
    source_fixture = make_planning_source_fixture(
        tmp_path / "source",
        missing_dates=missing_dates,
        missing_exchange=missing_exchange,
        split_start=split_start,
        split_end=split_end,
        split_role=split_role,
    )
    service.register_source(source_fixture.document)
    register_planning_catalog(service, include_pending=include_pending)
    set_document = make_planning_feature_set_document(
        feature_set_id=feature_set_ref.split("@", maxsplit=1)[0],
        include_pending=include_pending,
    )
    draft = service.save_feature_set(set_document)
    frozen = service.freeze_feature_set(
        feature_set_ref, expected_revision=draft["draft_revision"],
    )
    study = make_planning_study_document(
        study_id=study_id,
        feature_set_ref=feature_set_ref,
    )
    service.create_study(study)
    return registry, service, source_fixture, frozen, split_ref_from_fixture(source_fixture)


def _planned_feature_refs(plan: dict) -> set[str]:
    return {work["feature_ref"] for work in plan["work"]}


def _feature_refs_for_item(feature_set: dict, item_id: str) -> set[str]:
    item = next(item for item in feature_set["resolved_items"]
                if item["item_id"] == item_id)
    return {output["feature_ref"] for output in item["outputs"]}


def _work_for_features(plan: dict, feature_refs: set[str]) -> list[dict]:
    return [work for work in plan["work"] if work["feature_ref"] in feature_refs]


def _run_count(registry: Registry) -> int:
    return registry._call(lambda connection: connection.execute(
        "SELECT COUNT(*) FROM runs"
    ).fetchone()[0])


def test_study_scope_may_start_at_explicit_night_session_before_first_trade_date(tmp_path):
    registry, service, _, _, _ = _environment(tmp_path)
    try:
        stored = service.get_study("planning_study", revision=1)
        assert stored["limits"]["allowed_scope"][0]["start"] == (
            "2024-06-02T21:00:00+08:00"
        )
    finally:
        registry.close()


def test_selected_item_plan_closes_dependencies_and_uses_exchange_trade_date_and_session_history(
    tmp_path,
):
    registry, service, _, frozen, split_ref = _environment(tmp_path)
    try:
        request = make_plan_request(
            split_ref, selected_item_ids=["summary"], include_selection=True,
        )
        plan = service.plan_run(request)

        assert plan["schema_version"] == "run_plan_v1"
        assert plan["status"] == "READY"
        assert plan["work"]
        assert {work["role"] for work in plan["work"]} == {"requested", "dependency_only"}
        assert {work["step"] for work in plan["work"]}.issubset({3, 4, 5})
        assert all("validation" not in item_id.lower()
                   for work in plan["work"] for item_id in work["item_ids"])
        quote_refs = _feature_refs_for_item(frozen, "quote")
        window_refs = _feature_refs_for_item(frozen, "window")
        summary_refs = _feature_refs_for_item(frozen, "summary")
        assert _planned_feature_refs(plan) == quote_refs | window_refs | summary_refs

        requested_summary = _work_for_features(plan, summary_refs)
        assert requested_summary
        session_open = datetime.fromisoformat("2024-06-03T21:00:00+08:00")
        for work in plan["work"]:
            partition = work["partition_scope"]
            assert partition["exchange"] == EXCHANGE
            assert partition["instrument_id"] == INSTRUMENT
            assert partition["trade_date"] == "2024-06-04"
            assert partition["session_id"] == "night"
            # This night session belongs to June 4 although its requested UTC times are June 3.
            assert datetime.fromisoformat(partition["start"]).date().isoformat() \
                == "2024-06-03"
            history = work["required_history"]
            assert history["session_id"] == "night"
            history_start = datetime.fromisoformat(history["start"])
            output_start = datetime.fromisoformat(partition["start"])
            assert session_open <= history_start <= output_start

        summary_history = requested_summary[0]["required_history"]
        assert datetime.fromisoformat(summary_history["start"]) < datetime.fromisoformat(
            request["scope"]["start"]
        )
        assert plan["source_gaps"] == []
    finally:
        registry.close()


def test_statistic_input_window_and_transitive_history_are_not_double_expanded(tmp_path):
    registry, service, _, frozen, split_ref = _environment(tmp_path)
    try:
        plan = service.plan_run(make_plan_request(
            split_ref,
            selected_item_ids=["summary"],
            include_selection=True,
        ))
        statistic = _work_for_features(plan, _feature_refs_for_item(frozen, "summary"))[0]
        business_window = _work_for_features(
            plan, _feature_refs_for_item(frozen, "window"),
        )[0]
        quote = _work_for_features(plan, _feature_refs_for_item(frozen, "quote"))[0]

        # The statistic reads a one-minute window from its direct input. Its
        # transitive history also includes the business window's three-minute L.
        assert datetime.fromisoformat(statistic["partition_scope"]["start"]) == (
            datetime.fromisoformat("2024-06-03T21:10:00+08:00")
        )
        assert datetime.fromisoformat(statistic["read_scope"]["start"]) == (
            datetime.fromisoformat("2024-06-03T21:09:00+08:00")
        )
        assert datetime.fromisoformat(statistic["required_history"]["start"]) == (
            datetime.fromisoformat("2024-06-03T21:06:00+08:00")
        )
        assert datetime.fromisoformat(business_window["partition_scope"]["start"]) == (
            datetime.fromisoformat("2024-06-03T21:09:00+08:00")
        )
        assert datetime.fromisoformat(quote["partition_scope"]["start"]) == (
            datetime.fromisoformat("2024-06-03T21:06:00+08:00")
        )
    finally:
        registry.close()


def test_omitted_selection_means_all_but_empty_selection_is_no_work(tmp_path):
    registry, service, _, frozen, split_ref = _environment(tmp_path, include_pending=True)
    try:
        omitted = service.plan_run(make_plan_request(split_ref))
        assert omitted["summary"]["work_units"] > 0
        all_refs = {output["feature_ref"] for item in frozen["resolved_items"]
                    for output in item["outputs"]}
        assert _planned_feature_refs(omitted) == all_refs

        empty = service.plan_run(make_plan_request(
            split_ref, selected_item_ids=[], include_selection=True,
        ))
        assert empty["status"] == "NO_WORK"
        assert empty["summary"]["work_units"] == 0
        assert empty["work"] == []

        selected_ready = service.plan_run(make_plan_request(
            split_ref, selected_item_ids=["quote"], include_selection=True,
        ))
        assert _planned_feature_refs(selected_ready) == _feature_refs_for_item(frozen, "quote")

        unknown = make_plan_request(
            split_ref, selected_item_ids=["not_registered"], include_selection=True,
        )
        with pytest.raises(UnknownReferenceError):
            service.plan_run(unknown)
    finally:
        registry.close()


def test_pending_selection_blocks_its_downstream_but_not_an_unrelated_ready_item(tmp_path):
    registry, service, _, frozen, split_ref = _environment(tmp_path, include_pending=True)
    try:
        plan = service.plan_run(make_plan_request(
            split_ref, selected_item_ids=["quote", "pending_summary"],
            include_selection=True,
        ))
        assert plan["status"] == "READY"
        assert _planned_feature_refs(plan) == (
            _feature_refs_for_item(frozen, "quote")
            | _feature_refs_for_item(frozen, "pending_base")
            | _feature_refs_for_item(frozen, "pending_summary")
        )
        quote = _work_for_features(plan, _feature_refs_for_item(frozen, "quote"))
        pending_base = _work_for_features(plan, _feature_refs_for_item(frozen, "pending_base"))
        pending_summary = _work_for_features(
            plan, _feature_refs_for_item(frozen, "pending_summary"),
        )
        assert quote and all(work["state"] == "PENDING" for work in quote)
        assert pending_base and all(work["state"] == "BLOCKED" for work in pending_base)
        assert pending_summary and all(work["state"] == "BLOCKED" for work in pending_summary)
        assert all(work["blocked_reason"] == "PENDING_SEMANTICS"
                   for work in pending_base)
        assert all(work.get("blocked_reason") for work in pending_summary)
    finally:
        registry.close()


def test_forbidden_feature_step_is_blocked_without_adding_validation_work(tmp_path):
    registry, service, _, frozen, split_ref = _environment(tmp_path)
    try:
        plan = service.plan_run(make_plan_request(
            split_ref,
            selected_item_ids=["summary"],
            include_selection=True,
            allowed_steps=(3, 4),
            missing_dependencies="block",
        ))
        summary_work = _work_for_features(plan, _feature_refs_for_item(frozen, "summary"))
        assert summary_work
        assert all(work["state"] == "BLOCKED" for work in summary_work)
        assert all(work["blocked_reason"] == "STEP_NOT_ALLOWED" for work in summary_work)
        assert all(work["step"] in {3, 4, 5} for work in plan["work"])
        assert all("validation" not in item_id.lower()
                   for work in plan["work"] for item_id in work["item_ids"])
    finally:
        registry.close()


def test_source_gap_and_missing_exchange_metadata_have_distinct_plan_diagnostics(tmp_path):
    gap_registry, gap_service, _, _, gap_split = _environment(
        tmp_path / "gap", missing_dates=("2024-06-04",),
    )
    try:
        gap_plan = gap_service.plan_run(make_plan_request(
            gap_split, selected_item_ids=["summary"], include_selection=True,
        ))
        assert any(
            gap["trade_date"] == "2024-06-04"
            and gap["reason"] == "SOURCE_PARTITION_MISSING"
            for gap in gap_plan["source_gaps"]
        )
        gap_work = [work for work in gap_plan["work"]
                    if work.get("source_gap_reason") == "SOURCE_PARTITION_MISSING"]
        assert gap_work
        assert all(work["state"] == "PENDING" for work in gap_work)
        assert all(work.get("blocked_reason") != "MISSING_ARTIFACT" for work in gap_plan["work"])
    finally:
        gap_registry.close()

    metadata_registry, metadata_service, _, _, metadata_split = _environment(
        tmp_path / "missing_exchange", missing_exchange=True,
    )
    try:
        metadata_plan = metadata_service.plan_run(make_plan_request(
            metadata_split, selected_item_ids=["summary"], include_selection=True,
        ))
        blocked = [work for work in metadata_plan["work"] if work["state"] == "BLOCKED"]
        assert blocked
        assert all(work["blocked_reason"] == "EXCHANGE_METADATA_MISSING" for work in blocked)
    finally:
        metadata_registry.close()


def test_scope_and_split_authorization_cannot_be_expanded_by_a_run_request(tmp_path):
    registry, service, _, _, split_ref = _environment(tmp_path)
    try:
        outside_study = make_plan_request(split_ref)
        outside_study["scope"]["start"] = "2024-06-05T16:00:00+00:00"
        outside_study["scope"]["end"] = "2024-06-05T16:01:00+00:00"
        with pytest.raises(SourceScopeMissingError):
            service.plan_run(outside_study)

        unlisted = make_plan_request(split_ref)
        unlisted["split_ref"] = unlisted_split_ref(tmp_path, instrument_id=INSTRUMENT)
        with pytest.raises(SourceScopeMissingError):
            service.plan_run(unlisted)
    finally:
        registry.close()


def test_request_cannot_expand_an_authorized_split_read_range_inside_study_scope(tmp_path):
    registry, service, _, _, split_ref = _environment(
        tmp_path,
        split_start="2024-06-03T21:00:00+08:00",
        split_end="2024-06-03T21:20:00+08:00",
    )
    try:
        # This is still inside the Study's June 3 night-session scope, but the
        # exact source-listed split authorizes output only through 21:20 local.
        outside_split = make_plan_request(
            split_ref,
            selected_item_ids=["quote"],
            include_selection=True,
            start="2024-06-03T13:30:00+00:00",
            end="2024-06-03T13:31:00+00:00",
        )
        with pytest.raises(SourceScopeMissingError):
            service.plan_run(outside_split)
    finally:
        registry.close()


def test_recursive_history_read_cannot_expand_split_boundary(tmp_path):
    registry, service, _, frozen, split_ref = _environment(
        tmp_path,
        split_start="2024-06-03T21:08:00+08:00",
    )
    try:
        plan = service.plan_run(make_plan_request(
            split_ref,
            selected_item_ids=["summary"],
            include_selection=True,
        ))
        window_work = _work_for_features(plan, _feature_refs_for_item(frozen, "window"))
        assert window_work
        assert any(
            work["state"] == "BLOCKED"
            and work["blocked_reason"] == "HISTORY_OUTSIDE_SPLIT_SCOPE"
            for work in window_work
        )
        split_start = datetime.fromisoformat("2024-06-03T21:08:00+08:00")
        assert all(datetime.fromisoformat(work["read_scope"]["start"]) >= split_start
                   for work in plan["work"] if work["state"] == "PENDING")
    finally:
        registry.close()


def test_final_holdout_split_is_not_eligible_for_exploration(tmp_path):
    registry, service, _, _, split_ref = _environment(
        tmp_path, split_role="final_holdout",
    )
    try:
        with pytest.raises(SourceScopeMissingError):
            service.plan_run(make_plan_request(split_ref))
    finally:
        registry.close()


def test_short_same_session_history_is_clipped_and_recorded_without_crossing_session(tmp_path):
    registry, service, _, frozen, split_ref = _environment(tmp_path)
    try:
        plan = service.plan_run(make_plan_request(
            split_ref,
            selected_item_ids=["summary"],
            include_selection=True,
            start="2024-06-03T13:01:00+00:00",
            end="2024-06-03T13:02:00+00:00",
        ))
        summary_refs = _feature_refs_for_item(frozen, "summary")
        history_work = _work_for_features(plan, summary_refs)[0]
        assert history_work["partition_scope"]["trade_date"] == "2024-06-04"
        assert history_work["partition_scope"]["session_id"] == "night"
        shortfall = history_work["history_shortfall"]
        assert shortfall["reason"] == "SESSION_HISTORY_INSUFFICIENT"
        available = datetime.fromisoformat(shortfall["available_start"])
        assert available == datetime.fromisoformat("2024-06-03T21:00:00+08:00")
        assert history_work["read_scope"]["start"] == shortfall["available_start"]
    finally:
        registry.close()


def test_budget_failure_leaves_no_run_or_dispatch_record(tmp_path):
    registry, service, _, _, split_ref = _environment(tmp_path)
    try:
        request = make_plan_request(
            split_ref,
            selected_item_ids=["summary"],
            include_selection=True,
            budget={"max_work_units": 1, "max_partitions": 1, "max_concurrent_jobs": 1},
        )
        assert _run_count(registry) == 0
        with pytest.raises(BudgetExceededError):
            service.submit_run(request, request_key="too-small-budget")
        assert _run_count(registry) == 0
    finally:
        registry.close()


def test_plan_hash_change_rejects_submit_without_persisting(tmp_path):
    registry, service, _, _, split_ref = _environment(tmp_path)
    try:
        request = make_plan_request(split_ref, selected_item_ids=["summary"],
                                   include_selection=True)
        preview = service.plan_run(request)
        changed = copy.deepcopy(request)
        changed["scope"]["end"] = "2024-06-03T13:12:00+00:00"
        with pytest.raises(PlanChangedError):
            service.submit_run(
                changed, request_key="stale-preview", expected_plan_hash=preview["plan_hash"],
            )
        assert _run_count(registry) == 0
    finally:
        registry.close()


def test_equivalent_timezone_spellings_share_plan_identity_and_idempotent_run(tmp_path):
    registry, service, _, _, split_ref = _environment(tmp_path)
    try:
        utc_request = make_plan_request(split_ref, selected_item_ids=["quote"],
                                        include_selection=True)
        local_request = make_plan_request(
            split_ref,
            selected_item_ids=["quote"],
            include_selection=True,
            start="2024-06-03T21:10:00+08:00",
            end="2024-06-03T21:11:00+08:00",
        )
        utc_plan = service.plan_run(utc_request)
        local_plan = service.plan_run(local_request)
        assert utc_plan["request_hash"] == local_plan["request_hash"]
        assert utc_plan["plan_hash"] == local_plan["plan_hash"]

        utc_run = service.submit_run(
            utc_request,
            request_key="equivalent-times",
            expected_plan_hash=utc_plan["plan_hash"],
        )
        local_run = service.submit_run(
            local_request,
            request_key="equivalent-times",
            expected_plan_hash=local_plan["plan_hash"],
        )
        assert utc_run["run_id"] == local_run["run_id"]
        assert _run_count(registry) == 1
    finally:
        registry.close()


def test_idempotent_retry_survives_study_pause_but_new_request_is_rejected(tmp_path):
    registry, service, _, _, split_ref = _environment(tmp_path)
    request = make_plan_request(split_ref, selected_item_ids=["quote"],
                                include_selection=True)
    try:
        preview = service.plan_run(request)
        original = service.submit_run(
            request,
            request_key="created-before-pause",
            expected_plan_hash=preview["plan_hash"],
        )
        service.update_study(
            "planning_study",
            {"status": "paused"},
            expected_revision=1,
            actor={"kind": "human", "id": "fixture-reviewer"},
            reason="Exercise paused Study submission guard.",
        )

        duplicate = service.submit_run(
            request,
            request_key="created-before-pause",
            expected_plan_hash=preview["plan_hash"],
        )
        assert duplicate["run_id"] == original["run_id"]

        new_request = copy.deepcopy(request)
        new_request["study_revision"] = 2
        with pytest.raises(StudyNotActiveError):
            service.submit_run(new_request, request_key="new-after-pause")
        assert _run_count(registry) == 1
    finally:
        registry.close()


def test_concurrent_same_key_is_idempotent_and_conflicting_content_is_rejected_after_reopen(
    tmp_path,
):
    registry, service, _, _, split_ref = _environment(tmp_path, include_pending=True)
    request = make_plan_request(
        split_ref,
        selected_item_ids=["quote", "pending_summary"],
        include_selection=True,
    )
    preview = service.plan_run(request)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(
                service.submit_run,
                request,
                request_key="same-request",
                expected_plan_hash=preview["plan_hash"],
            ) for _ in range(2)]
            runs = [future.result() for future in futures]
        assert runs[0]["run_id"] == runs[1]["run_id"]
        run_id = runs[0]["run_id"]
        assert _run_count(registry) == 1
        original_states = {item["state"] for item in runs[0]["item_executions"]}
        assert {"PENDING", "BLOCKED"}.issubset(original_states)
    finally:
        registry.close()

    reopened = Registry(tmp_path / "research.duckdb")
    reopened_service = ResearchService(reopened)
    try:
        stored = reopened_service.get_run(run_id)
        assert stored["run_id"] == run_id
        states = {item["state"] for item in stored["item_executions"]}
        assert {"PENDING", "BLOCKED"}.issubset(states)

        changed = copy.deepcopy(request)
        changed["selected_item_ids"] = ["quote"]
        changed_preview = reopened_service.plan_run(changed)
        with pytest.raises(IdempotencyConflictError):
            reopened_service.submit_run(
                changed,
                request_key="same-request",
                expected_plan_hash=changed_preview["plan_hash"],
            )
        assert _run_count(reopened) == 1
    finally:
        reopened.close()


def test_dependency_expansion_budget_stops_before_over_limit_node_and_registers_nothing(
    tmp_path, monkeypatch,
):
    from quantsage_discovery.research import planner as planner_module

    registry, service, _, _, split_ref = _environment(tmp_path)
    request = make_plan_request(
        split_ref,
        selected_item_ids=["summary"],
        include_selection=True,
        budget={"max_work_units": 20, "max_partitions": 20,
                "max_concurrent_jobs": 2},
    )
    monkeypatch.setattr(planner_module, "MAX_PLAN_WORK_UNITS", 3)
    feature_lookups = []
    original_get_feature = service._catalog.get_feature

    def counted_get_feature(ref):
        feature_lookups.append(ref)
        return original_get_feature(ref)

    monkeypatch.setattr(service._catalog, "get_feature", counted_get_feature)
    runs_before = _run_count(registry)
    dispatches_before = registry._call(
        lambda connection: connection.execute(
            "SELECT COUNT(*) FROM research_dispatch_queue"
        ).fetchone()[0]
    )
    try:
        with pytest.raises(BudgetExceededError) as error:
            service.plan_run(request)
        details = error.value.details
        assert details["effective_budget"]["max_work_units"] == 3
        assert details["exceeded"]["max_work_units"]["limit"] == 3
        assert details["counts"]["max_work_units"] == 4
        # The fourth node is rejected before catalog lookup or dependency traversal.
        assert len(feature_lookups) == 3
        assert _run_count(registry) == runs_before == 0
        dispatches_after = registry._call(
            lambda connection: connection.execute(
                "SELECT COUNT(*) FROM research_dispatch_queue"
            ).fetchone()[0]
        )
        assert dispatches_after == dispatches_before == 0
    finally:
        registry.close()


def test_plan_accepts_small_scope_with_only_partition_and_concurrency_budget(tmp_path):
    from quantsage_discovery.research.planner import MAX_PLAN_WORK_UNITS

    registry, service, _, _, split_ref = _environment(tmp_path)
    try:
        study = service.get_study("planning_study")
        limits = copy.deepcopy(study["limits"])
        limits["budget"] = {"max_partitions": 20, "max_concurrent_jobs": 4}
        updated = service.update_study(
            "planning_study",
            {"limits": limits},
            expected_revision=study["revision"],
            actor={"kind": "agent", "id": "planner-budget-acceptance"},
            reason="Exercise bounded planning with partition and concurrency budgets.",
        )
        request = make_plan_request(
            split_ref,
            study_revision=updated["revision"],
            selected_item_ids=["quote"],
            include_selection=True,
            budget={"max_partitions": 20, "max_concurrent_jobs": 2},
        )
        plan = service.plan_run(request)
        assert "max_work_units" not in request["budget"]
        assert plan["budget"]["max_work_units"] == MAX_PLAN_WORK_UNITS
        assert plan["budget"]["max_partitions"] == 20
        assert plan["summary"]["work_units"] == 1
        assert plan["status"] == "READY"
        assert _run_count(registry) == 0
    finally:
        registry.close()
