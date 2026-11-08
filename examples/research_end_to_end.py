"""Execute real Step 02-05 A/B/C and bounded blocked/no-work acceptance cases."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from examples.l2_primitives_workflow import (
    _call,
    _install_catalog,
    _optional_get,
    _wait_for_run,
)
from examples.statistical_backends_workflow import _install as _install_statistical_catalog
from quantsage_discovery.research.business_windows import (
    CATALOG_VERSION as BUSINESS_VERSION,
)
from quantsage_discovery.research.business_windows import (
    business_template_documents,
)
from quantsage_discovery.research.business_windows import (
    provider_document as business_provider_document,
)
from quantsage_discovery.research.l2_primitives import (
    CATALOG_VERSION as PRIMITIVE_VERSION,
)
from quantsage_discovery.research.rpc import RpcError
from quantsage_discovery.research.statistical_windows import (
    CATALOG_VERSION as STATISTICAL_VERSION,
)

FEATURE_SET_ID = "real_abc_candidates"
FEATURE_SET_V1 = f"{FEATURE_SET_ID}@1"
FEATURE_SET_V2 = f"{FEATURE_SET_ID}@2"
STUDY_ID = "real_abc_study"
MORNING = "CN_STOCK:2026-01-05:continuous_morning"
AFTERNOON = "CN_STOCK:2026-01-05:continuous_afternoon"
MORNING_SCOPE = {
    "instruments": ["T2603"],
    "start": "2026-01-05T01:30:00+00:00",
    "end": "2026-01-05T03:30:00+00:00",
    "session_filter": [MORNING],
}
FULL_SCOPE = {
    **MORNING_SCOPE,
    "end": "2026-01-05T07:15:00+00:00",
    "session_filter": [MORNING, AFTERNOON],
}
SELECTED_A = [
    "obi_30s",
    "wobi_1m",
    "obi_mean_1m",
    "wobi_catch22_f1ecac_5m",
]
SELECTED_C = [*SELECTED_A, "wobi_spectral_entropy_5m"]


def _primitive(template: str, parameters: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "template_ref": f"{template}@{PRIMITIVE_VERSION}",
        "step": 3,
        "parameters": parameters or {},
    }


def _business(
    template: str,
    *,
    lookback: str,
    input_id: str,
    parameters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "template_ref": f"{template}@{BUSINESS_VERSION}",
        "step": 4,
        "parameters": parameters or {},
        "lookback": lookback,
        "input": {"item_id": input_id},
    }


def _statistic(
    metric: str,
    *,
    input_id: str,
    profile_window: str,
    stride: str,
    min_samples: int,
) -> dict[str, Any]:
    return {
        "template_ref": f"statistical_trailing@{STATISTICAL_VERSION}",
        "step": 5,
        "parameters": {},
        "input": {"item_id": input_id},
        "profile_window": profile_window,
        "sampling": {
            "axis": "clock_time",
            "interval": "1s",
            "evaluation_stride": stride,
            "max_feature_age": "2s",
        },
        "validity": {"min_samples": min_samples},
        "metrics": [f"{metric}@{STATISTICAL_VERSION}"],
    }


def _base_items() -> list[dict[str, Any]]:
    return [
        {
            "item_id": "obi_primitive",
            "alias": "obi_primitive",
            "recipe": _primitive("l2_obi_snapshot", {"space": "d0"}),
        },
        {
            "item_id": "wobi_primitive",
            "alias": "wobi_primitive",
            "recipe": _primitive("l2_wobi_snapshot", {"space": "d0"}),
        },
        {
            "item_id": "coverage_primitive",
            "alias": "coverage_primitive",
            "recipe": _primitive("l2_min_coverage_snapshot", {"space": "d0", "size": "q25"}),
        },
        {
            "item_id": "fill_distance_primitive",
            "alias": "fill_distance_primitive",
            "recipe": _primitive(
                "l2_fill_dist_asym_snapshot", {"size": "q25"},
            ),
        },
        {
            "item_id": "obi_30s",
            "alias": "obi_30s",
            "recipe": _business(
                "business_obi", lookback="30s", input_id="obi_primitive",
                parameters={"space": "d0"},
            ),
        },
        {
            "item_id": "wobi_1m",
            "alias": "wobi_1m",
            "recipe": _business(
                "business_wobi", lookback="1m", input_id="wobi_primitive",
                parameters={"space": "d0"},
            ),
        },
        {
            "item_id": "coverage_5m",
            "alias": "coverage_5m",
            "recipe": _business(
                "business_min_coverage", lookback="5m", input_id="coverage_primitive",
                parameters={"space": "d0", "size": "q25"},
            ),
        },
        {
            "item_id": "fill_dist_asym_1m",
            "alias": "fill_dist_asym_1m",
            "recipe": _business(
                "business_fill_dist_asym", lookback="1m",
                input_id="fill_distance_primitive",
                parameters={"size": "q25"},
            ),
        },
        {
            "item_id": "obi_mean_1m",
            "alias": "obi_mean_1m",
            "recipe": _statistic(
                "stat.mean", input_id="obi_30s",
                profile_window="1m", stride="5s", min_samples=1,
            ),
        },
        {
            "item_id": "coverage_mean_1m",
            "alias": "coverage_mean_1m",
            "recipe": _statistic(
                "stat.mean", input_id="coverage_5m",
                profile_window="1m", stride="5s", min_samples=1,
            ),
        },
        {
            "item_id": "wobi_catch22_f1ecac_5m",
            "alias": "wobi_catch22_f1ecac_5m",
            "recipe": _statistic(
                "c22.CO_f1ecac", input_id="wobi_1m",
                profile_window="5m", stride="60s", min_samples=256,
            ),
        },
    ]


def _feature_set_document(version: int) -> dict[str, Any]:
    items = _base_items()
    if version == 2:
        items.append({
            "item_id": "wobi_spectral_entropy_5m",
            "alias": "wobi_spectral_entropy_5m",
            "recipe": _statistic(
                "tsf.spectral_entropy", input_id="wobi_1m",
                profile_window="5m", stride="60s", min_samples=256,
            ),
        })
    document = {
        "schema_version": "feature_set_v1",
        "feature_set_id": FEATURE_SET_ID,
        "version": version,
        "name": "Real E2E A/B/C candidate set",
        "description": "Three business candidates and selected descriptive/shape metrics.",
        "items": items,
        "created_by": {"kind": "agent", "id": "step13-e2e"},
        "change_reason": (
            "Freeze the E2E candidate set."
            if version == 1 else "Add one fixed-R tsfeatures metric to the same scope."
        ),
    }
    if version == 2:
        document["parent_ref"] = FEATURE_SET_V1
    return document


def _backlog(version: int) -> list[dict[str, Any]]:
    ref = FEATURE_SET_V1 if version == 1 else FEATURE_SET_V2
    item_ids = [
        "obi_30s",
        "wobi_1m",
        "coverage_5m",
        "fill_dist_asym_1m",
        "obi_mean_1m",
        "wobi_catch22_f1ecac_5m",
    ]
    if version == 2:
        item_ids.append("wobi_spectral_entropy_5m")
    return [
        {
            "feature_set_ref": ref,
            "item_id": item_id,
            "priority": index,
            "state": "active",
            "reason": "Finite E2E research candidate.",
        }
        for index, item_id in enumerate(item_ids, start=1)
    ]


def _study_document(source_ref: str) -> dict[str, Any]:
    return {
        "schema_version": "study_v1",
        "study_id": STUDY_ID,
        "name": "Real Step 02-05 A/B/C acceptance",
        "objective": "Validate selected ready features, reviews, expansion and reuse.",
        "status": "active",
        "source_refs": [source_ref],
        "feature_set_refs": [FEATURE_SET_V1],
        "default_feature_set_ref": FEATURE_SET_V1,
        "default_scope": FULL_SCOPE,
        "backlog": _backlog(1),
        "limits": {
            "allowed_steps": [3, 4, 5],
            "allowed_scope": [FULL_SCOPE],
            "budget": {
                "max_work_units": 100,
                "max_partitions": 20,
                "max_concurrent_jobs": 2,
            },
        },
        "created_by": {"kind": "agent", "id": "step13-e2e"},
        "change_reason": "Create the bounded real E2E Study.",
    }


def _request(
    *,
    study: dict[str, Any],
    feature_set_ref: str,
    scope: dict[str, Any],
    selected: list[str],
    source_ref: str,
    split_ref: dict[str, Any],
    allowed_steps: list[int] | None = None,
    parent_run_id: str | None = None,
    review_refs: list[str] | None = None,
) -> dict[str, Any]:
    request = {
        "schema_version": "run_request_v1",
        "study_id": STUDY_ID,
        "study_revision": study["revision"],
        "feature_set_ref": feature_set_ref,
        "source_refs": [source_ref],
        "split_ref": split_ref,
        "scope": scope,
        "allowed_steps": allowed_steps or [3, 4, 5],
        "missing_dependencies": "compute_within_allowed_steps",
        "reuse_policy": "verified_only",
        "selected_item_ids": selected,
        "budget": {
            "max_work_units": 100,
            "max_partitions": 20,
            "max_concurrent_jobs": 2,
        },
        "created_by": {"kind": "agent", "id": "step13-e2e"},
    }
    if parent_run_id is not None:
        request["parent_run_id"] = parent_run_id
    if review_refs:
        request["adopted_review_refs"] = review_refs
    return request


def _execute(
    database: Path,
    socket: Path,
    request: dict[str, Any],
    request_key: str,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    plan = _call(database, socket, "runs.plan", {"request": request})
    submitted = _call(database, socket, "runs.submit", {
        "request": request,
        "request_key": request_key,
        "expected_plan_hash": plan["plan_hash"],
    })
    _call(database, socket, "runs.execute", {"run_id": submitted["run_id"]})
    final = _wait_for_run(database, socket, submitted["run_id"])
    results = _call(database, socket, "results.list", {
        "filters": {"run_id": submitted["run_id"]},
        "limit": 100,
        "verify": True,
    })["items"]
    return plan, final, results


def _item_results(run: dict[str, Any], item_id: str) -> list[str]:
    for execution in run.get("item_executions", []):
        if item_id in execution.get("item_ids", []):
            return list(execution.get("result_refs", []))
    return []


def _requested_result_refs(run: dict[str, Any]) -> set[str]:
    return {
        ref
        for execution in run.get("item_executions", [])
        if execution.get("role") == "requested"
        for ref in execution.get("result_refs", [])
    }


def _ensure_catalog(database: Path, socket: Path) -> None:
    _install_catalog(database, socket)
    provider = business_provider_document()
    try:
        _call(database, socket, "catalog.providers.get", {
            "ref": f"{provider['provider_id']}@{provider['version']}",
        })
    except RpcError:
        _call(database, socket, "catalog.providers.register", {"document": provider})
        for template in business_template_documents():
            _call(database, socket, "catalog.templates.register", {"document": template})
    _install_statistical_catalog(database, socket)


def _ensure_feature_set(database: Path, socket: Path, version: int) -> dict[str, Any]:
    ref = f"{FEATURE_SET_ID}@{version}"
    feature_set = _optional_get(database, socket, "feature_sets.get", ref)
    if feature_set is None:
        feature_set = _call(database, socket, "feature_sets.save", {
            "document": _feature_set_document(version),
        })
    elif feature_set.get("status") == "draft":
        feature_set = _call(database, socket, "feature_sets.save", {
            "document": _feature_set_document(version),
            "expected_revision": feature_set["draft_revision"],
        })
    if feature_set.get("status") != "frozen":
        feature_set = _call(database, socket, "feature_sets.freeze", {
            "ref": ref,
            "expected_revision": feature_set["draft_revision"],
        })
    return feature_set


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--socket", required=True, type=Path)
    parser.add_argument("--source-ref", default="cffex_treasury_l2_full79@2")
    parser.add_argument("--run-prefix", default="e2e-real-20260105")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    db, sock = args.database.resolve(), args.socket.resolve()

    source = _call(db, sock, "sources.get", {"ref": args.source_ref})
    policy_path = Path(source["access_policy_ref"]["uri"].removeprefix("file://"))
    split_ref = json.loads(policy_path.read_text(encoding="utf-8"))["split_refs"][0]
    _ensure_catalog(db, sock)
    frozen_v1 = _ensure_feature_set(db, sock, 1)
    frozen_v2 = _ensure_feature_set(db, sock, 2)

    try:
        study = _call(db, sock, "studies.get", {"study_id": STUDY_ID})
    except RpcError as exc:
        if "unknown" not in str(exc).lower():
            raise
        study = _call(db, sock, "studies.create", {
            "document": _study_document(args.source_ref),
        })

    request_a = _request(
        study=study,
        feature_set_ref=FEATURE_SET_V1,
        scope=MORNING_SCOPE,
        selected=SELECTED_A,
        source_ref=args.source_ref,
        split_ref=split_ref,
    )
    plan_a, run_a, results_a = _execute(db, sock, request_a, f"{args.run_prefix}-a")
    _require(run_a["status"] == "COMPLETE", f"Run A is {run_a['status']}")

    coverage = _call(db, sock, "studies.coverage", {
        "study_id": STUDY_ID,
        "revision": 1,
        "source_refs": [args.source_ref],
        "scope": MORNING_SCOPE,
        "feature_set_ref": FEATURE_SET_V1,
        "item_ids": [
            "obi_30s", "wobi_1m", "coverage_5m",
            "fill_dist_asym_1m", "obi_mean_1m", "wobi_catch22_f1ecac_5m",
        ],
        "verify": True,
    })
    _require(
        any(
            target.get("execution_status") == "NOT_SELECTED"
            for target in coverage["targets"]
        ),
        "A coverage did not expose unselected candidates",
    )

    evidence_refs = [
        *_item_results(run_a, "obi_30s")[:1],
        *_item_results(run_a, "wobi_catch22_f1ecac_5m")[:1],
    ]
    review_id = f"{args.run_prefix}-review-a"
    review_document = {
        "schema_version": "review_v1",
        "review_id": review_id,
        "study_id": STUDY_ID,
        "actor": {"kind": "agent", "id": "step13-e2e"},
        "run_refs": [run_a["run_id"]],
        "result_refs": evidence_refs,
        "target_item_refs": [f"{FEATURE_SET_V1}/obi_30s", f"{FEATURE_SET_V1}/wobi_1m"],
        "scope": MORNING_SCOPE,
        "observations": (
            "Verified serialized ready business and statistical results, retained NA reasons "
            "and exposed unselected candidates in coverage."
        ),
        "decision": "investigate",
        "rationale": "Proceed to the afternoon session without changing the frozen set.",
        "limitations": "Research-only availability; tick metadata is absent; no causal claim.",
        "next_action": {"operation": "extend_scope", "session": AFTERNOON},
    }
    existing_reviews = _call(db, sock, "reviews.list", {
        "study_id": STUDY_ID,
        "run_id": run_a["run_id"],
        "limit": 20,
    })["items"]
    review = next(
        (item for item in existing_reviews
         if item["review_id"] == review_id),
        None,
    )
    if review is None:
        review = _call(db, sock, "reviews.add", {"document": review_document})

    request_b = _request(
        study=study,
        feature_set_ref=FEATURE_SET_V1,
        scope=FULL_SCOPE,
        selected=SELECTED_A,
        source_ref=args.source_ref,
        split_ref=split_ref,
        parent_run_id=run_a["run_id"],
        review_refs=[review["review_id"]],
    )
    plan_b, run_b, results_b = _execute(db, sock, request_b, f"{args.run_prefix}-b")
    _require(run_b["status"] == "COMPLETE", f"Run B is {run_b['status']}")
    _require(
        _requested_result_refs(run_a).issubset(_requested_result_refs(run_b)),
        "Expanded Run B did not reuse all exact A results",
    )

    study = _call(db, sock, "studies.update", {
        "study_id": STUDY_ID,
        "changes": {
            "feature_set_refs": [FEATURE_SET_V1, FEATURE_SET_V2],
            "backlog": _backlog(2),
        },
        "expected_revision": study["revision"],
        "actor": {"kind": "agent", "id": "step13-e2e"},
        "reason": "Add the frozen tsfeatures metric while retaining the parent set.",
    })
    request_c = _request(
        study=study,
        feature_set_ref=FEATURE_SET_V2,
        scope=FULL_SCOPE,
        selected=SELECTED_C,
        source_ref=args.source_ref,
        split_ref=split_ref,
        parent_run_id=run_b["run_id"],
        review_refs=[review["review_id"]],
    )
    plan_c, run_c, results_c = _execute(db, sock, request_c, f"{args.run_prefix}-c")
    _require(run_c["status"] == "COMPLETE", f"Run C is {run_c['status']}")
    _require(
        _requested_result_refs(run_b).issubset(_requested_result_refs(run_c)),
        "Run C did not reuse all unchanged B results",
    )

    refs_v1 = {
        output["feature_ref"]
        for item in frozen_v1["resolved_items"]
        for output in item["outputs"]
    }
    refs_v2 = {
        output["feature_ref"]
        for item in frozen_v2["resolved_items"]
        for output in item["outputs"]
    }
    added_features = refs_v2 - refs_v1
    _require(len(added_features) == 1, "C did not add exactly one Feature definition")
    new_feature = next(iter(added_features))
    c_feature_refs = {item["feature_ref"] for item in results_c}
    b_feature_refs = {item["feature_ref"] for item in results_b}
    _require(
        c_feature_refs - b_feature_refs == {new_feature},
        "Run C executed definitions beyond the one added Feature",
    )

    comparison_bc = _call(db, sock, "runs.compare", {
        "left_run_id": run_b["run_id"],
        "right_run_id": run_c["run_id"],
        "verify": True,
    })
    _require(
        new_feature in json.dumps(comparison_bc["added_definitions"]["right"]),
        "B/C comparison did not report the added definition",
    )

    empty_plan = _call(db, sock, "runs.plan", {"request": {
        **_request(
            study=study,
            feature_set_ref=FEATURE_SET_V1,
            scope=MORNING_SCOPE,
            selected=[],
            source_ref=args.source_ref,
            split_ref=split_ref,
        ),
        "selected_item_ids": [],
    }})
    _require(empty_plan["status"] == "NO_WORK", "Explicit empty selection was not NO_WORK")

    pending_request = _request(
        study=study,
        feature_set_ref=FEATURE_SET_V1,
        scope=MORNING_SCOPE,
        selected=["fill_dist_asym_1m"],
        source_ref=args.source_ref,
        split_ref=split_ref,
        allowed_steps=[3, 4],
    )
    _pending_plan, pending_run, pending_results = _execute(
        db, sock, pending_request, f"{args.run_prefix}-pending",
    )
    pending_execution = next(
        item for item in pending_run["item_executions"]
        if "fill_dist_asym_1m" in item.get("item_ids", [])
    )
    _require(
        pending_execution["state"] in {"BLOCKED", "FAILED"}
        and pending_execution.get("blocked_reason") == "PENDING_SEMANTICS",
        "Selected F07 did not report PENDING_SEMANTICS",
    )

    reviews = _call(db, sock, "reviews.list", {
        "study_id": STUDY_ID,
        "run_id": run_a["run_id"],
        "limit": 20,
    })["items"]
    _require(
        any(item["review_id"] == review["review_id"] for item in reviews),
        "A Review was not queryable",
    )

    report = {
        "schema_version": "real_e2e_abc_report_v1",
        "source_ref": args.source_ref,
        "feature_sets": {
            "v1": FEATURE_SET_V1,
            "v2": FEATURE_SET_V2,
            "added_feature_ref": new_feature,
        },
        "study": {"study_id": STUDY_ID, "revision_after_c": study["revision"]},
        "runs": {
            "A": {
                "run_id": run_a["run_id"],
                "status": run_a["status"],
                "plan_summary": plan_a["summary"],
                "result_refs": run_a["result_refs"],
            },
            "B": {
                "run_id": run_b["run_id"],
                "status": run_b["status"],
                "plan_summary": plan_b["summary"],
                "state_counts": run_b["summary"]["state_counts"],
                "result_refs": run_b["result_refs"],
            },
            "C": {
                "run_id": run_c["run_id"],
                "status": run_c["status"],
                "plan_summary": plan_c["summary"],
                "state_counts": run_c["summary"]["state_counts"],
                "result_refs": run_c["result_refs"],
            },
            "pending": {
                "run_id": pending_run["run_id"],
                "status": pending_run["status"],
                "blocked_reason": pending_execution.get("blocked_reason"),
                "result_refs": pending_run["result_refs"],
            },
        },
        "review": review,
        "coverage_summary": coverage["summary"],
        "comparison_bc": comparison_bc,
        "empty_plan": {"status": empty_plan["status"], "work_units": empty_plan["summary"]["work_units"]},
        "result_counts": {
            "A": len(results_a),
            "B": len(results_b),
            "C": len(results_c),
            "pending": len(pending_results),
        },
    }
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


if __name__ == "__main__":
    main()
