"""01-F acceptance tests for result queries, coverage, reviews and comparison."""

from __future__ import annotations

import copy
import json
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from quantsage_discovery.core.artifacts import artifact_path
from quantsage_discovery.core.registry import Registry
from quantsage_discovery.research.errors import (
    ArtifactInvalidError,
    ResearchValidationError,
    SourceScopeMissingError,
    UnknownReferenceError,
)
from quantsage_discovery.research.service import ResearchService
from tests.research_catalog_fixtures import feature_set_document, recipe_item
from tests.research_execution_fixtures import (
    make_execution_environment,
    make_numeric_source_fixture,
)
from tests.research_fixtures import refresh_source_fixture_refs
from tests.research_planning_fixtures import (
    FEATURE_SET_REF,
    INSTRUMENT,
    METRIC_MAX,
    METRIC_MEAN,
    make_plan_request,
    make_planning_feature_set_document,
    make_planning_study_document,
    register_planning_catalog,
    split_ref_from_fixture,
)

STUDY_ID = "planning_study"
SOURCE_REF = "planning_source@1"
ALT_SOURCE_REF = "alternate_source@1"
RESEARCH_ACTOR = {"kind": "agent", "id": "queries-acceptance"}
PROVENANCE_FIELDS = {
    "result_id",
    "feature_ref",
    "definition_hash",
    "source_ref",
    "producer_run_id",
}


def _backlog(feature_set_ref: str, item_ids: tuple[str, ...]) -> list[dict[str, Any]]:
    return [
        {
            "feature_set_ref": feature_set_ref,
            "item_id": item_id,
            "priority": index,
            "state": "active",
            "reason": "Finite synthetic acceptance candidate.",
        }
        for index, item_id in enumerate(item_ids, start=1)
    ]


@contextmanager
def _case(
    tmp_path: Path,
    *,
    rows: list[dict[str, Any]] | None = None,
    feature_set_document: dict[str, Any] | None = None,
    item_ids: tuple[str, ...] = ("quote", "window", "summary"),
):
    registry, service, source, frozen, request = make_execution_environment(
        tmp_path,
        rows=rows,
        feature_set_document=feature_set_document,
    )
    try:
        study = service.get_study(STUDY_ID)
        study = service.update_study(
            STUDY_ID,
            {"backlog": _backlog(FEATURE_SET_REF, item_ids)},
            expected_revision=study["revision"],
            actor=RESEARCH_ACTOR,
            reason="Declare the finite 01-F test backlog.",
        )
        request["study_revision"] = study["revision"]
        yield registry, service, source, frozen, request
    finally:
        registry.close()


def _submit_execute(
    service,
    request: dict[str, Any],
    request_key: str,
    *,
    fault_hook=None,
) -> dict[str, Any]:
    plan = service.plan_run(request)
    submitted = service.submit_run(
        request,
        request_key=request_key,
        expected_plan_hash=plan["plan_hash"],
    )
    return service.execute_run(submitted["run_id"], fault_hook=fault_hook)


def _works(run: dict[str, Any], item_id: str) -> list[dict[str, Any]]:
    return [work for work in run["item_executions"] if item_id in work.get("item_ids", [])]


def _one_work(run: dict[str, Any], item_id: str) -> dict[str, Any]:
    matches = _works(run, item_id)
    assert len(matches) == 1, (item_id, run["item_executions"])
    return matches[0]


def _coverage(service, request: dict[str, Any], *, scope=None, verify: bool = True):
    return service.study_coverage(
        request["study_id"],
        revision=request["study_revision"],
        source_refs=request["source_refs"],
        scope=copy.deepcopy(scope if scope is not None else request["scope"]),
        feature_set_ref=request["feature_set_ref"],
        verify=verify,
    )


def _targets(coverage: dict[str, Any], item_id: str) -> list[dict[str, Any]]:
    return [target for target in coverage["targets"] if item_id in target.get("item_ids", [])]


def _single_target(coverage: dict[str, Any], item_id: str) -> dict[str, Any]:
    matches = _targets(coverage, item_id)
    assert len(matches) == 1, (item_id, coverage["targets"])
    return matches[0]


def _utc_pair(interval: dict[str, Any]) -> tuple[datetime, datetime]:
    start = datetime.fromisoformat(interval["start"])
    end = datetime.fromisoformat(interval["end"])
    assert start.tzinfo is not None and end.tzinfo is not None
    assert start.utcoffset() == end.utcoffset() == UTC.utcoffset(start)
    return start, end


def _result_ref(work: dict[str, Any]) -> str:
    assert len(work["result_refs"]) == 1
    return work["result_refs"][0]


def _review_document(
    *,
    review_id: str,
    study_id: str,
    run_id: str,
    result_refs: list[str],
    scope: dict[str, Any],
    decision: str,
    actor: dict[str, str] | None = None,
    rationale: str | None = None,
    supersedes_review_id: str | None = None,
) -> dict[str, Any]:
    review = {
        "schema_version": "review_v1",
        "review_id": review_id,
        "study_id": study_id,
        "actor": copy.deepcopy(actor or RESEARCH_ACTOR),
        "run_refs": [run_id],
        "result_refs": list(result_refs),
        "scope": copy.deepcopy(scope),
        "observations": "Inspected serialized values and their coverage.",
        "decision": decision,
        "rationale": rationale or f"Synthetic judgment {review_id}.",
        "limitations": "No real market data or scientific claim is represented.",
    }
    if supersedes_review_id is not None:
        review["supersedes_review_id"] = supersedes_review_id
    return review


def _register_other_study(service, source_ref: str, split_ref: dict[str, str]):
    study_id = "other_study"
    study = make_planning_study_document(
        study_id=study_id,
        source_ref=source_ref,
        feature_set_ref=FEATURE_SET_REF,
    )
    study["backlog"] = _backlog(FEATURE_SET_REF, ("quote", "window", "summary"))
    service.create_study(study)
    request = make_plan_request(
        split_ref,
        study_id=study_id,
        source_refs=(source_ref,),
        feature_set_ref=FEATURE_SET_REF,
        selected_item_ids=["quote"],
        include_selection=True,
    )
    return request


def test_coverage_uses_interval_union_actual_rows_and_exact_trade_session_time(tmp_path):
    rows = [
        {"event_time": "2024-06-03T13:10:10+00:00", "trade_date": "2024-06-04",
         "session_id": "night", "value": 2.0},
        {"event_time": "2024-06-03T13:10:50+00:00", "trade_date": "2024-06-04",
         "session_id": "night", "value": None},
    ]
    independent_candidates = feature_set_document(
        feature_set_id="planning_features",
        items=[
            recipe_item("quote", parameters={"scale": 1}),
            recipe_item("other", parameters={"scale": 2}),
            recipe_item("third", parameters={"scale": 3}),
        ],
    )
    with _case(
        tmp_path,
        rows=rows,
        feature_set_document=independent_candidates,
        item_ids=("quote", "other", "third"),
    ) as (_, service, _, _, base_request):
        selected = copy.deepcopy(base_request)
        selected["selected_item_ids"] = ["quote", "other"]
        left_request = copy.deepcopy(selected)
        left_request["scope"].update({
            "start": "2024-06-03T13:10:00+00:00",
            "end": "2024-06-03T13:10:20+00:00",
        })
        right_request = copy.deepcopy(selected)
        right_request["scope"].update({
            "start": "2024-06-03T13:10:40+00:00",
            "end": "2024-06-03T13:11:00+00:00",
        })
        first = _submit_execute(service, left_request, "coverage-left")
        second = _submit_execute(service, right_request, "coverage-right")
        assert first["status"] == second["status"] == "COMPLETE"

        query_scope = copy.deepcopy(selected["scope"])
        query_scope.update({
            "start": "2024-06-03T13:10:00+00:00",
            "end": "2024-06-03T13:11:00+00:00",
        })
        coverage = _coverage(service, selected, scope=query_scope)
        quote = _single_target(coverage, "quote")
        assert quote["status"] == "PARTIAL"
        assert quote["observed_points"] == 2
        assert quote["valid_points"] == 1

        processed = {
            (_utc_pair(interval), interval["trade_date"], interval["session_id"])
            for interval in quote["processed_intervals"]
        }
        assert processed == {
            ((datetime.fromisoformat("2024-06-03T13:10:00+00:00"),
              datetime.fromisoformat("2024-06-03T13:10:20+00:00")), "2024-06-04", "night"),
            ((datetime.fromisoformat("2024-06-03T13:10:40+00:00"),
              datetime.fromisoformat("2024-06-03T13:11:00+00:00")), "2024-06-04", "night"),
        }
        assert len(quote["unprocessed_intervals"]) == 1
        gap = quote["unprocessed_intervals"][0]
        assert _utc_pair(gap) == (
            datetime.fromisoformat("2024-06-03T13:10:20+00:00"),
            datetime.fromisoformat("2024-06-03T13:10:40+00:00"),
        )
        assert gap["trade_date"] == "2024-06-04"
        assert gap["session_id"] == "night"

        third_targets = _targets(coverage, "third")
        assert third_targets
        assert all(target["status"] == "NOT_SELECTED" for target in third_targets)
        assert all(
            interval["trade_date"] == "2024-06-04"
            for interval in quote["requested_intervals"]
        )
        # The quote session starts on the UTC calendar day before its exchange trade date.
        assert all(_utc_pair(interval)[0].date().isoformat() == "2024-06-03"
                   for interval in quote["processed_intervals"])
        outside_time = copy.deepcopy(query_scope)
        outside_time["start"] = "2024-06-01T13:00:00+00:00"
        with pytest.raises(SourceScopeMissingError):
            _coverage(service, selected, scope=outside_time)
        outside_session = copy.deepcopy(query_scope)
        outside_session["session_filter"] = ["day"]
        with pytest.raises(SourceScopeMissingError):
            _coverage(service, selected, scope=outside_session)


def test_later_success_covers_early_failure_and_cache_links_do_not_double_count(tmp_path):
    with _case(tmp_path) as (registry, service, _, _, request):
        request["selected_item_ids"] = ["quote"]
        first_plan = service.plan_run(request)
        first_submit = service.submit_run(
            request, request_key="fail-first", expected_plan_hash=first_plan["plan_hash"]
        )

        def fail_before_publish(phase, _context):
            if phase == "before_publish":
                raise RuntimeError("intentional first-attempt failure")

        failed = service.execute_run(first_submit["run_id"], fault_hook=fail_before_publish)
        assert failed["status"] == "FAILED"
        assert _one_work(failed, "quote")["attempts"][0]["state"] == "FAILED"

        succeeded = _submit_execute(service, request, "later-success")
        assert succeeded["status"] == "COMPLETE"
        produced_result = _result_ref(_one_work(succeeded, "quote"))
        cached = _submit_execute(service, request, "later-cache")
        cached_work = _one_work(cached, "quote")
        assert cached_work["state"] == "CACHED"
        assert cached_work["result_refs"] == [produced_result]

        coverage = _coverage(service, request)
        quote = _single_target(coverage, "quote")
        assert quote["status"] == "COMPLETE"
        assert quote["available"] is True
        assert quote["result_refs"] == [produced_result]
        assert quote["observed_points"] == 2
        assert quote["valid_points"] == 2
        attempt_states = [attempt["state"] for attempt in quote["attempts"]]
        assert "FAILED" in attempt_states
        assert "SUCCEEDED" in attempt_states
        # The cached Run links the immutable result but adds neither a second result
        # identity nor another copy of its observations.
        result_page = service.list_results({"study_id": STUDY_ID}, verify=True)
        assert [item["result_id"] for item in result_page["items"]].count(produced_result) == 1
        assert registry.get_feature_result(produced_result)["producer_run_id"] == succeeded["run_id"]


def test_all_na_is_processed_and_cached_without_recompute_while_zero_rows_stay_distinct(tmp_path):
    with _case(
        tmp_path / "all-na",
        rows=[
            {"event_time": "2024-06-03T13:10:10+00:00", "value": None},
            {"event_time": "2024-06-03T13:10:20+00:00", "value": None},
        ],
    ) as (_, service, _, _, request):
        request["selected_item_ids"] = ["quote"]
        first = _submit_execute(service, request, "all-na-first")
        result_ref = _result_ref(_one_work(first, "quote"))
        assert first["status"] == "COMPLETE"
        second = _submit_execute(service, request, "all-na-cache")
        assert _one_work(second, "quote")["state"] == "CACHED"
        assert _one_work(second, "quote")["result_refs"] == [result_ref]
        result = service.list_results({"result_ids": [result_ref]})["items"][0]
        assert result["quality"]["n_total"] == 2
        assert result["quality"]["n_valid"] == 0
        quote = _single_target(_coverage(service, request), "quote")
        assert quote["status"] == "COMPLETE"
        assert quote["available"] is True
        assert quote["observed_points"] == 2
        assert quote["valid_points"] == 0

    with _case(tmp_path / "zero-rows", rows=[]) as (_, service, _, _, request):
        request["selected_item_ids"] = ["quote"]
        run = _submit_execute(service, request, "zero-rows")
        result_ref = _result_ref(_one_work(run, "quote"))
        result = service.list_results({"result_ids": [result_ref]})["items"][0]
        assert result["quality"]["n_total"] == 0
        assert result["quality"]["n_valid"] == 0
        quote = _single_target(_coverage(service, request), "quote")
        assert quote["status"] == "COMPLETE"
        assert quote["available"] is True
        assert quote["observed_points"] == 0
        assert quote["valid_points"] == 0
        page = service.read_results([result_ref])
        assert page["rows"] == []
        assert page["result_refs"] == [result_ref]


def test_fast_health_is_unverified_and_missing_result_is_repair_not_unexecuted(tmp_path):
    with _case(tmp_path) as (registry, service, _, _, request):
        request["selected_item_ids"] = ["quote"]
        produced = _submit_execute(service, request, "health-source")
        quote_work = _one_work(produced, "quote")
        result_ref = _result_ref(quote_work)

        pending_request = copy.deepcopy(request)
        pending_request["selected_item_ids"] = ["window"]
        pending_plan = service.plan_run(pending_request)
        service.submit_run(
            pending_request,
            request_key="pending-window",
            expected_plan_hash=pending_plan["plan_hash"],
        )

        verified = service.list_results({"result_ids": [result_ref]}, verify=True)["items"][0]
        assert verified["health"]["status"] == "available"
        last_checked_at = verified["health"]["checked_at"]
        assert last_checked_at
        artifact_path(quote_work["artifacts"][0]["uri"]).unlink()
        quick = service.list_results({"result_ids": [result_ref]}, verify=False)["items"][0]
        assert quick["health"]["status"] == "unverified"
        assert quick["health"]["health_unverified_since_check"] is True
        assert quick["health"]["checked_at"] == last_checked_at

        fast_coverage = _coverage(service, request, verify=False)
        fast_quote = _single_target(fast_coverage, "quote")
        assert fast_quote["status"] == "UNVERIFIED"
        assert fast_quote["execution_status"] == "COMPLETE"
        assert fast_quote["processed_intervals"]
        assert fast_quote["available"] is None
        assert fast_quote["needs_repair"] is False
        assert fast_quote["health_verification"] == {
            "status": "unverified",
            "checked_at": last_checked_at,
            "last_status": "available",
            "health_unverified_since_check": True,
        }

        coverage = _coverage(service, request, verify=True)
        quote = _single_target(coverage, "quote")
        window = _single_target(coverage, "window")
        summary_targets = _targets(coverage, "summary")
        assert quote["status"] == "NEEDS_REPAIR"
        assert quote["available"] is False
        assert quote["processed_intervals"]
        assert not quote["unprocessed_intervals"]
        assert window["status"] == "NOT_STARTED"
        assert all(target["status"] == "NOT_SELECTED" for target in summary_targets)

        fresh = service.list_results({"result_ids": [result_ref]}, verify=True)["items"][0]
        assert fresh["health"]["status"] == "missing"
        assert fresh["health"]["checked_at"] >= last_checked_at
        with pytest.raises(ArtifactInvalidError) as missing_payload:
            service.read_results([result_ref], columns=["event_time", "value"])
        assert missing_payload.value.code == "ARTIFACT_INVALID"

        repaired = service.resume_run(produced["run_id"])
        repaired_work = _one_work(repaired, "quote")
        assert repaired_work["state"] == "SUCCEEDED"
        assert len(repaired_work["result_refs"]) == 2
        replacement_ref = repaired_work["result_refs"][-1]
        assert replacement_ref != result_ref
        replacement = service.list_results(
            {"result_ids": [replacement_ref]}, verify=True,
        )["items"][0]
        assert replacement["health"]["status"] == "available"
        historical = registry.get_feature_result(result_ref)
        assert historical["health_checks"][-1]["status"] == "missing"
        repaired_coverage = _coverage(service, request, verify=True)
        repaired_quote = _single_target(repaired_coverage, "quote")
        assert repaired_quote["status"] == "COMPLETE"
        assert repaired_quote["needs_repair"] is False
        assert repaired_quote["available"] is True
        assert repaired_quote["observed_points"] == 2
        assert repaired_quote["valid_points"] == 2
        assert set(repaired_quote["result_refs"]) == {result_ref, replacement_ref}
        assert repaired_quote["active_result_refs"] == [replacement_ref]
        assert [entry["status"] for entry in repaired_quote["health_history"]] == [
            "missing", "available",
        ]


def test_results_read_exact_refs_safe_columns_pagination_and_multi_source_lineage(tmp_path):
    rows = [
        {"event_time": "2024-06-03T13:10:10+00:00", "value": 2.0},
        {"event_time": "2024-06-03T13:10:20+00:00", "value": 3.0},
        # In the source manifest but beyond the Run scope.
        {"event_time": "2024-06-03T13:12:10+00:00", "value": 100.0},
    ]
    with _case(tmp_path, rows=rows) as (_, service, _, _, request):
        request["selected_item_ids"] = ["quote"]
        first_run = _submit_execute(service, request, "read-first-source")
        first_ref = _result_ref(_one_work(first_run, "quote"))
        first_result = service.list_results({"result_ids": [first_ref]})["items"][0]
        assert first_result["source_refs"] == [SOURCE_REF]

        alternate = make_numeric_source_fixture(
            tmp_path / "alternate-source",
            rows,
            source_id="alternate_source",
        )
        service.register_source(alternate.document)
        alternate_request = _register_other_study(
            service, ALT_SOURCE_REF, split_ref_from_fixture(alternate)
        )
        alternate_request["scope"] = copy.deepcopy(request["scope"])
        alternate_request["selected_item_ids"] = ["quote"]
        second_run = _submit_execute(service, alternate_request, "read-second-source")
        second_ref = _result_ref(_one_work(second_run, "quote"))

        first_page = service.list_results({"source_ref": SOURCE_REF}, verify=True)
        second_page = service.list_results({"source_ref": ALT_SOURCE_REF}, verify=True)
        assert [item["result_id"] for item in first_page["items"]] == [first_ref]
        assert [item["result_id"] for item in second_page["items"]] == [second_ref]
        assert service.list_results({"result_ids": []})["items"] == []
        with pytest.raises(ResearchValidationError) as too_many_ids:
            service.list_results({"result_ids": [f"unknown-{index}" for index in range(501)]})
        assert too_many_ids.value.code == "INVALID_CONFIG"

        page = service.read_results(
            [first_ref, second_ref],
            columns=["event_time", "value"],
            scope={"start": request["scope"]["start"], "end": request["scope"]["end"]},
            limit=1,
        )
        assert page["truncated"] is True
        assert page["next_cursor"]
        rows_read = list(page["rows"])
        cursor = page["next_cursor"]
        while cursor is not None:
            page = service.read_results(
                [first_ref, second_ref],
                columns=["event_time", "value"],
                scope={"start": request["scope"]["start"], "end": request["scope"]["end"]},
                limit=1,
                cursor=cursor,
            )
            rows_read.extend(page["rows"])
            cursor = page["next_cursor"]
        assert len(rows_read) == 4
        assert {row["result_id"] for row in rows_read} == {first_ref, second_ref}
        assert {row["source_ref"] for row in rows_read} == {
            SOURCE_REF, ALT_SOURCE_REF,
        }
        assert all(set(row) == PROVENANCE_FIELDS | {"event_time", "value"} for row in rows_read)
        assert all(row["value"] in {2.0, 3.0} for row in rows_read)

        time_only = service.read_results([first_ref], columns=["event_time"])
        assert time_only["rows"]
        assert all(set(row) == PROVENANCE_FIELDS | {"event_time"} for row in time_only["rows"])
        assert all("value" not in row for row in time_only["rows"])

        unrestricted = service.read_results([first_ref])
        assert all(row["value"] != 100.0 for row in unrestricted["rows"])
        if unrestricted["rows"] and "available_time" in unrestricted["rows"][0]:
            assert all(row["available_time"] is None for row in unrestricted["rows"])

        assert service.read_results([])["rows"] == []
        with pytest.raises(ResearchValidationError) as bad_column:
            service.read_results([first_ref], columns=["value", "not_in_artifact"])
        assert bad_column.value.code == "INVALID_CONFIG"
        with pytest.raises(UnknownReferenceError) as missing_ref:
            service.read_results(["result_does_not_exist"])
        assert missing_ref.value.code == "UNKNOWN_REF"
        with pytest.raises(ResearchValidationError) as bad_filter:
            service.list_results({"unexpected_filter": "value"})
        assert bad_filter.value.code == "INVALID_CONFIG"


def test_reviews_append_conflicting_judgments_corrections_and_bound_adoption(tmp_path):
    with _case(tmp_path) as (registry, service, source, _, request):
        request["selected_item_ids"] = ["quote"]
        run = _submit_execute(service, request, "review-evidence")
        result_ref = _result_ref(_one_work(run, "quote"))
        keep_document = _review_document(
            review_id="review_keep",
            study_id=STUDY_ID,
            run_id=run["run_id"],
            result_refs=[result_ref],
            scope=request["scope"],
            decision="keep",
            actor={"kind": "human", "id": "reviewer-a"},
        )
        keep_document["scores"] = [{
            "metric_ref": METRIC_MEAN,
            "metric_version": "pycatch22-0.4.5",
            "value": 0.75,
            "scope": copy.deepcopy(request["scope"]),
            "evidence_refs": [result_ref],
        }]
        keep = service.add_review(keep_document)
        park = service.add_review(_review_document(
            review_id="review_park",
            study_id=STUDY_ID,
            run_id=run["run_id"],
            result_refs=[result_ref],
            scope=request["scope"],
            decision="park",
            actor={"kind": "agent", "id": "reviewer-b"},
        ))
        correction = service.add_review(_review_document(
            review_id="review_keep_correction",
            study_id=STUDY_ID,
            run_id=run["run_id"],
            result_refs=[result_ref],
            scope=request["scope"],
            decision="investigate",
            rationale="The original judgment needs a narrower interpretation.",
            supersedes_review_id="review_keep",
        ))
        assert keep["actor"] == {"kind": "human", "id": "reviewer-a"}
        assert keep["result_refs"] == [result_ref]
        score = keep["scores"][0]
        assert score["metric_ref"] == METRIC_MEAN
        assert score["metric_version"] == "pycatch22-0.4.5"
        assert datetime.fromisoformat(score["scope"]["start"]).tzinfo == UTC
        assert park["decision"] == "park"
        assert correction["supersedes_review_id"] == "review_keep"
        review_page = service.list_reviews(STUDY_ID, run_id=run["run_id"])
        by_id = {review["review_id"]: review for review in review_page["items"]}
        assert set(by_id) >= {"review_keep", "review_park", "review_keep_correction"}
        assert by_id["review_keep"]["decision"] == "keep"
        assert by_id["review_keep"]["rationale"] == keep["rationale"]

        # A second Study can reuse this exact result through its own Run link.
        other_request = _register_other_study(
            service, SOURCE_REF, split_ref_from_fixture(source)
        )
        other_run = _submit_execute(service, other_request, "cross-study-cache")
        other_work = _one_work(other_run, "quote")
        assert other_work["state"] == "CACHED"
        assert other_work["result_refs"] == [result_ref]
        assert registry.get_feature_result(result_ref)["producer_run_id"] == run["run_id"]
        cross_study_review = service.add_review(_review_document(
            review_id="other_study_cached_result_review",
            study_id="other_study",
            run_id=other_run["run_id"],
            result_refs=[result_ref],
            scope=other_request["scope"],
            decision="investigate",
        ))
        assert cross_study_review["result_refs"] == [result_ref]

        # An existing result from the same producer Study is still invalid evidence
        # for S2 unless an S2 Run links that exact result.
        unlinked_request = copy.deepcopy(request)
        unlinked_request["selected_item_ids"] = ["window"]
        unlinked_run = _submit_execute(service, unlinked_request, "unlinked-study-result")
        unlinked_result = _result_ref(_one_work(unlinked_run, "window"))
        unlinked_review = _review_document(
            review_id="other_study_unlinked_result_review",
            study_id="other_study",
            run_id=other_run["run_id"],
            result_refs=[unlinked_result],
            scope=other_request["scope"],
            decision="investigate",
        )
        with pytest.raises(ResearchValidationError) as unlinked_error:
            service.add_review(unlinked_review)
        assert unlinked_error.value.code == "INVALID_CONFIG"

        adopted_request = copy.deepcopy(request)
        adopted_request["parent_run_id"] = run["run_id"]
        adopted_request["adopted_review_refs"] = ["review_keep"]
        adopted_plan = service.plan_run(adopted_request)
        adopted = service.submit_run(
            adopted_request,
            request_key="adopt-valid-review",
            expected_plan_hash=adopted_plan["plan_hash"],
        )
        assert adopted["adopted_review_refs"] == ["review_keep"]
        missing_adoption = copy.deepcopy(request)
        missing_adoption["adopted_review_refs"] = ["review_does_not_exist"]
        with pytest.raises(UnknownReferenceError):
            service.plan_run(missing_adoption)

        bad_score_scope = _review_document(
            review_id="review_bad_score_scope",
            study_id=STUDY_ID,
            run_id=run["run_id"],
            result_refs=[result_ref],
            scope=request["scope"],
            decision="investigate",
        )
        bad_score_scope["scores"] = [{
            "metric_ref": METRIC_MEAN,
            "metric_version": "pycatch22-0.4.5",
            "value": 0.75,
            "scope": {
                **copy.deepcopy(request["scope"]),
                "start": "2024-06-03T13:09:00+00:00",
            },
            "evidence_refs": [result_ref],
        }]
        with pytest.raises((ResearchValidationError, SourceScopeMissingError)):
            service.add_review(bad_score_scope)
        fake_score_evidence = copy.deepcopy(keep_document)
        fake_score_evidence["review_id"] = "review_fake_score_evidence"
        fake_score_evidence["scores"][0]["evidence_refs"] = ["missing_result"]
        with pytest.raises(UnknownReferenceError):
            service.add_review(fake_score_evidence)
        wrong_metric_version = copy.deepcopy(keep_document)
        wrong_metric_version["review_id"] = "review_wrong_metric_version"
        wrong_metric_version["scores"][0]["metric_version"] = "pycatch22-0.4.6"
        with pytest.raises(ResearchValidationError):
            service.add_review(wrong_metric_version)

        invalid_evidence = _review_document(
            review_id="review_bad_evidence",
            study_id=STUDY_ID,
            run_id=run["run_id"],
            result_refs=["missing_result"],
            scope=request["scope"],
            decision="investigate",
        )
        with pytest.raises(UnknownReferenceError):
            service.add_review(invalid_evidence)

        out_of_scope = copy.deepcopy(adopted_request)
        out_of_scope["scope"]["start"] = "2024-06-01T13:00:00+00:00"
        with pytest.raises(SourceScopeMissingError):
            service.plan_run(out_of_scope)
        over_budget = copy.deepcopy(adopted_request)
        over_budget["budget"]["max_partitions"] += 1
        bounded_plan = service.plan_run(over_budget)
        study_budget = service.get_study(STUDY_ID)["limits"]["budget"]
        assert bounded_plan["budget"]["max_partitions"] == study_budget["max_partitions"]
        assert bounded_plan["requested_budget"]["max_partitions"] > \
            bounded_plan["budget"]["max_partitions"]

        # A review and parent Run from another Study cannot be adopted here.
        cross_study = copy.deepcopy(request)
        cross_study["parent_run_id"] = other_run["run_id"]
        cross_study["adopted_review_refs"] = [cross_study_review["review_id"]]
        with pytest.raises(ResearchValidationError) as cross_study_error:
            service.plan_run(cross_study)
        assert cross_study_error.value.code == "INVALID_CONFIG"


def test_review_scope_requires_union_of_cited_results_but_allows_run_level_failure_review(
    tmp_path,
):
    rows = [
        {"event_time": "2024-06-02T13:10:10+00:00", "trade_date": "2024-06-03",
         "session_id": "night", "value": 2.0},
        {"event_time": "2024-06-03T13:10:10+00:00", "trade_date": "2024-06-04",
         "session_id": "night", "value": 4.0},
        {"event_time": "2024-06-03T13:10:50+00:00", "trade_date": "2024-06-04",
         "session_id": "night", "value": 6.0},
    ]
    with _case(tmp_path, rows=rows) as (_, service, _, _, base_request):
        base_request["selected_item_ids"] = ["quote"]
        broad_request = copy.deepcopy(base_request)
        broad_request["scope"].update({
            "start": "2024-06-02T13:00:00+00:00",
            "end": "2024-06-03T18:30:00+00:00",
        })
        broad_run = _submit_execute(service, broad_request, "review-broad-run")
        broad_work = _works(broad_run, "quote")
        broad_result_refs = [
            result_ref for work in broad_work for result_ref in work["result_refs"]
        ]
        assert len(broad_result_refs) == 2
        single_day_review = _review_document(
            review_id="review_broad_scope_one_day_evidence",
            study_id=STUDY_ID,
            run_id=broad_run["run_id"],
            result_refs=[broad_result_refs[0]],
            scope=broad_request["scope"],
            decision="investigate",
        )
        with pytest.raises(SourceScopeMissingError):
            service.add_review(single_day_review)

        full_evidence_review = _review_document(
            review_id="review_broad_scope_union_evidence",
            study_id=STUDY_ID,
            run_id=broad_run["run_id"],
            result_refs=broad_result_refs,
            scope=broad_request["scope"],
            decision="keep",
        )
        accepted = service.add_review(full_evidence_review)
        assert set(accepted["result_refs"]) == set(broad_result_refs)

        left_request = copy.deepcopy(base_request)
        left_request["scope"].update({
            "start": "2024-06-03T13:10:00+00:00",
            "end": "2024-06-03T13:10:20+00:00",
        })
        right_request = copy.deepcopy(base_request)
        right_request["scope"].update({
            "start": "2024-06-03T13:10:20+00:00",
            "end": "2024-06-03T13:11:00+00:00",
        })
        left_run = _submit_execute(service, left_request, "review-union-left")
        right_run = _submit_execute(service, right_request, "review-union-right")
        left_ref = _result_ref(_one_work(left_run, "quote"))
        right_ref = _result_ref(_one_work(right_run, "quote"))
        union_scope = copy.deepcopy(left_request["scope"])
        union_scope["end"] = right_request["scope"]["end"]
        union_review = _review_document(
            review_id="review_adjacent_result_union",
            study_id=STUDY_ID,
            run_id=left_run["run_id"],
            result_refs=[left_ref, right_ref],
            scope=union_scope,
            decision="investigate",
        )
        union_review["run_refs"] = [left_run["run_id"], right_run["run_id"]]
        accepted_union = service.add_review(union_review)
        assert set(accepted_union["result_refs"]) == {left_ref, right_ref}

        failed_request = copy.deepcopy(base_request)
        failed_request["scope"].update({
            "start": "2024-06-04T13:00:00+00:00",
            "end": "2024-06-04T13:10:00+00:00",
        })

        def fail_before_publish(phase, _context):
            if phase == "before_publish":
                raise RuntimeError("synthetic failure for run-level review")

        failed_run = _submit_execute(
            service, failed_request, "review-run-level-failure", fault_hook=fail_before_publish,
        )
        assert failed_run["status"] == "FAILED"
        run_level_review = _review_document(
            review_id="review_failed_run_without_result",
            study_id=STUDY_ID,
            run_id=failed_run["run_id"],
            result_refs=[],
            scope=failed_request["scope"],
            decision="investigate",
            rationale="The failed attempt and its diagnostic were reviewed.",
        )
        accepted_failure_review = service.add_review(run_level_review)
        assert accepted_failure_review["result_refs"] == []
        assert service.get_run(failed_run["run_id"])["status"] == "FAILED"


def test_compare_intersects_universal_and_night_sessions_without_day_quality_leak(tmp_path):
    rows = [
        {"event_time": "2024-06-03T13:10:10+00:00", "trade_date": "2024-06-04",
         "session_id": "night", "value": 2.0},
        {"event_time": "2024-06-03T13:10:20+00:00", "trade_date": "2024-06-04",
         "session_id": "night", "value": 4.0},
        {"event_time": "2024-06-04T01:10:10+00:00", "trade_date": "2024-06-04",
         "session_id": "day", "value": 10.0},
    ]
    registry = Registry(tmp_path / "research.duckdb")
    service = ResearchService(registry)
    source = make_numeric_source_fixture(tmp_path / "source", rows)
    session_policy = json.loads(source.paths["session_policy"].read_text(encoding="utf-8"))
    session_policy["sessions"] = ["day", "night"]
    session_policy["intervals"].append({
        "exchange": "SYNTH",
        "instrument_id": INSTRUMENT,
        "trade_date": "2024-06-04",
        "session_id": "day",
        "start": "2024-06-04T01:00:00+00:00",
        "end": "2024-06-04T07:00:00+00:00",
    })
    source.paths["session_policy"].write_text(
        json.dumps(session_policy, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    partition = next(
        part for part in source.document["coverage"]["partitions"]
        if part["trade_date"] == "2024-06-04"
    )
    partition["session_ids"] = ["night", "day"]
    split = json.loads(source.paths["split"].read_text(encoding="utf-8"))
    split["read_ranges"][0].pop("session_filter", None)
    source.paths["split"].write_text(
        json.dumps(split, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    refresh_source_fixture_refs(source, refresh_split=True)

    try:
        service.register_source(source.document)
        register_planning_catalog(service)
        feature_set = make_planning_feature_set_document(items=[recipe_item("quote")])
        draft = service.save_feature_set(feature_set)
        service.freeze_feature_set(
            FEATURE_SET_REF,
            expected_revision=draft["draft_revision"],
        )
        study = make_planning_study_document()
        study["study_id"] = "dual_session_study"
        study["limits"]["allowed_scope"][0].pop("session_filter", None)
        study["backlog"] = _backlog(FEATURE_SET_REF, ("quote",))
        service.create_study(study)

        request = make_plan_request(
            split_ref_from_fixture(source),
            study_id="dual_session_study",
            source_refs=(SOURCE_REF,),
            feature_set_ref=FEATURE_SET_REF,
            start="2024-06-03T13:00:00+00:00",
            end="2024-06-04T07:00:00+00:00",
            selected_item_ids=["quote"],
            include_selection=True,
        )
        universal_request = copy.deepcopy(request)
        universal_request["scope"].pop("session_filter")
        universal_run = _submit_execute(service, universal_request, "universal-sessions")
        night_request = copy.deepcopy(request)
        night_request["scope"]["session_filter"] = ["night"]
        night_run = _submit_execute(service, night_request, "night-session-only")

        comparison = service.compare_runs(universal_run["run_id"], night_run["run_id"])
        common_scope = comparison["scope"]["common"]
        assert common_scope["session_filter"] == ["night"]
        assert {item["session_id"] for item in comparison["common_dates"]} == {"night"}
        assert {item["session_id"] for item in comparison["added_dates"]["left"]} == {"day"}
        assert comparison["added_dates"]["right"] == []
        assert comparison["quality_differences"]
        assert {item["date"]["session_id"] for item in comparison["quality_differences"]} == {
            "night",
        }
    finally:
        registry.close()


def test_abcs_compare_reports_date_scope_definition_and_quality_deltas(tmp_path):
    rows = [
        {"event_time": "2024-06-02T13:10:10+00:00", "trade_date": "2024-06-03",
         "session_id": "night", "value": 2.0},
        {"event_time": "2024-06-02T13:10:20+00:00", "trade_date": "2024-06-03",
         "session_id": "night", "value": 4.0},
        {"event_time": "2024-06-03T13:10:10+00:00", "trade_date": "2024-06-04",
         "session_id": "night", "value": 6.0},
        {"event_time": "2024-06-03T13:10:20+00:00", "trade_date": "2024-06-04",
         "session_id": "night", "value": 8.0},
        {"event_time": "2024-06-04T13:10:10+00:00", "trade_date": "2024-06-05",
         "session_id": "night", "value": 10.0},
        {"event_time": "2024-06-04T13:10:20+00:00", "trade_date": "2024-06-05",
         "session_id": "night", "value": 12.0},
    ]
    feature_set_v1 = make_planning_feature_set_document()
    summary_v1 = next(item for item in feature_set_v1["items"] if item["item_id"] == "summary")
    summary_v1["recipe"]["metrics"] = [METRIC_MEAN]
    with _case(
        tmp_path,
        rows=rows,
        feature_set_document=feature_set_v1,
    ) as (registry, service, _, frozen_v1, base_request):
        first_request = copy.deepcopy(base_request)
        first_request["scope"].update({
            "start": "2024-06-02T13:00:00+00:00",
            "end": "2024-06-03T18:30:00+00:00",
        })
        first_request["selected_item_ids"] = ["summary"]
        run_a = _submit_execute(service, first_request, "abc-a")

        expanded_request = copy.deepcopy(first_request)
        expanded_request["scope"]["end"] = "2024-06-04T18:30:00+00:00"
        expanded_request["parent_run_id"] = run_a["run_id"]
        run_b = _submit_execute(service, expanded_request, "abc-b")
        assert run_a["status"] == run_b["status"] == "COMPLETE"
        first_summary_refs = {
            result_ref for work in _works(run_a, "summary") for result_ref in work["result_refs"]
        }
        expanded_summary_refs = {
            result_ref for work in _works(run_b, "summary") for result_ref in work["result_refs"]
        }
        assert len(first_summary_refs) == 2
        assert first_summary_refs <= expanded_summary_refs
        assert all(
            work["state"] == "CACHED"
            for work in _works(run_b, "summary")
            if first_summary_refs & set(work["result_refs"])
        )
        # A cached summary retains its producer and the exact dependency result lineage.
        for summary_ref in first_summary_refs:
            summary_result = registry.get_feature_result(summary_ref)
            assert summary_result["producer_run_id"] == run_a["run_id"]
            assert summary_result["input_result_refs"]
            for window_ref in summary_result["input_result_refs"]:
                window_result = registry.get_feature_result(window_ref)
                assert window_result["producer_run_id"] == run_a["run_id"]
                assert window_result["input_result_refs"]
                for quote_ref in window_result["input_result_refs"]:
                    assert registry.get_feature_result(quote_ref)["producer_run_id"] == run_a["run_id"]

        date_comparison = service.compare_runs(run_a["run_id"], run_b["run_id"])
        assert date_comparison["scope"]["common"]
        assert date_comparison["common_dates"]
        assert {entry["trade_date"] for entry in date_comparison["common_dates"]} == {
            "2024-06-03", "2024-06-04",
        }
        assert date_comparison["added_dates"]["left"] == []
        assert {entry["trade_date"] for entry in date_comparison["added_dates"]["right"]} == {
            "2024-06-05",
        }
        assert "quality_differences" in date_comparison
        assert {entry["date"]["trade_date"]
                for entry in date_comparison["quality_differences"]} == {
                    "2024-06-03", "2024-06-04",
                }
        common_scope = date_comparison["scope"]["common"]
        assert common_scope["session_filter"] == ["night"]
        assert datetime.fromisoformat(common_scope["start"]).tzinfo == UTC
        assert datetime.fromisoformat(common_scope["end"]).tzinfo == UTC

        feature_set_v2 = make_planning_feature_set_document()
        summary_v2 = next(item for item in feature_set_v2["items"] if item["item_id"] == "summary")
        summary_v2["recipe"]["metrics"] = [METRIC_MEAN, METRIC_MAX]
        feature_set_v2.update({
            "version": 2,
            "parent_ref": FEATURE_SET_REF,
            "change_reason": "Add one explicitly selected synthetic metric.",
        })
        draft = service.save_feature_set(feature_set_v2)
        frozen_v2 = service.freeze_feature_set(
            "planning_features@2", expected_revision=draft["draft_revision"]
        )
        feature_refs_v1 = {
            output["feature_ref"]
            for item in frozen_v1["resolved_items"]
            for output in item["outputs"]
        }
        feature_refs_v2 = {
            output["feature_ref"]
            for item in frozen_v2["resolved_items"]
            for output in item["outputs"]
        }
        assert len(feature_refs_v2 - feature_refs_v1) == 1
        added_metric_ref = next(iter(feature_refs_v2 - feature_refs_v1))

        study = service.get_study(STUDY_ID)
        study = service.update_study(
            STUDY_ID,
            {
                "feature_set_refs": [FEATURE_SET_REF, "planning_features@2"],
                "backlog": _backlog("planning_features@2", ("quote", "window", "summary")),
            },
            expected_revision=study["revision"],
            actor=RESEARCH_ACTOR,
            reason="Adopt an immutable FeatureSet version with one additional metric.",
        )
        run_c_request = copy.deepcopy(expanded_request)
        run_c_request.update({
            "study_revision": study["revision"],
            "feature_set_ref": "planning_features@2",
            "parent_run_id": run_b["run_id"],
        })
        run_c = _submit_execute(service, run_c_request, "abc-c")
        assert run_c["status"] == "COMPLETE"

        feature_comparison = service.compare_runs(run_b["run_id"], run_c["run_id"])
        assert feature_comparison["scope"]["common"]
        assert {entry["trade_date"] for entry in feature_comparison["common_dates"]} == {
            "2024-06-03", "2024-06-04", "2024-06-05",
        }
        assert added_metric_ref in str(feature_comparison["added_definitions"]["right"])
        assert "quality_differences" in feature_comparison
