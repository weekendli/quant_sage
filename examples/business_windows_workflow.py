"""Run selected Step 04-A ready business windows through the local owner."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from examples.l2_primitives_workflow import (
    _call,
    _install_catalog,
    _optional_get,
    _wait_for_run,
)
from quantsage_discovery.research.business_windows import (
    CATALOG_VERSION,
    business_template_documents,
    provider_document,
)
from quantsage_discovery.research.l2_primitives import CATALOG_VERSION as PRIMITIVE_VERSION
from quantsage_discovery.research.rpc import RpcError


def _recipe(template: str, parameters: dict, *, lookback: str | None = None,
            input_id: str | None = None, inputs: dict | None = None,
            max_age: str | None = None) -> dict:
    value = {
        "template_ref": f"{template}@{PRIMITIVE_VERSION if template.startswith('l2_') else CATALOG_VERSION}",
        "step": 3 if template.startswith("l2_") else 4,
        "parameters": parameters,
    }
    if lookback is not None:
        value["lookback"] = lookback
    if input_id is not None:
        value["input"] = {"item_id": input_id}
    if inputs is not None:
        value["inputs"] = {role: {"item_id": item} for role, item in inputs.items()}
    if max_age is not None:
        value["max_endpoint_age"] = max_age
    return value


def _document() -> dict:
    items = [
        {"item_id": "obi_primitive", "alias": "obi_primitive",
         "recipe": _recipe("l2_obi_snapshot", {"space": "d0"})},
        {"item_id": "wobi_primitive", "alias": "wobi_primitive",
         "recipe": _recipe("l2_wobi_snapshot", {"space": "d0"})},
        {"item_id": "coverage_primitive", "alias": "coverage_primitive",
         "recipe": _recipe("l2_min_coverage_snapshot", {"space": "d0", "size": "q25"})},
        {"item_id": "capacity_primitive", "alias": "capacity_primitive",
         "recipe": _recipe("l2_capacity_imb_snapshot", {"space": "d0", "size": "q50"})},
        {"item_id": "buy", "alias": "buy", "recipe": _recipe("l2_active_buy_cumulative", {})},
        {"item_id": "sell", "alias": "sell", "recipe": _recipe("l2_active_sell_cumulative", {})},
        {"item_id": "buy_reset", "alias": "buy_reset",
         "recipe": _recipe("l2_buy_cumulative_reset", {})},
        {"item_id": "sell_reset", "alias": "sell_reset",
         "recipe": _recipe("l2_sell_cumulative_reset", {})},
        {"item_id": "volume", "alias": "volume",
         "recipe": _recipe("l2_trade_volume_interval", {})},
        {"item_id": "depth", "alias": "depth", "recipe": _recipe("l2_depth_ref_5", {})},
        {"item_id": "obi_30s", "alias": "obi_30s", "recipe": _recipe(
            "business_obi", {"space": "d0"}, lookback="30s", input_id="obi_primitive",
        )},
        {"item_id": "wobi_1m", "alias": "wobi_1m", "recipe": _recipe(
            "business_wobi", {"space": "d0"}, lookback="1m", input_id="wobi_primitive",
        )},
        {"item_id": "coverage_5m", "alias": "coverage_5m", "recipe": _recipe(
            "business_min_coverage", {"space": "d0", "size": "q25"},
            lookback="5m", input_id="coverage_primitive",
        )},
        {"item_id": "capacity_10m", "alias": "capacity_10m", "recipe": _recipe(
            "business_capacity_imb", {"space": "d0", "size": "q50"},
            lookback="10m", input_id="capacity_primitive",
        )},
        {"item_id": "active_flow_30s", "alias": "active_flow_30s", "recipe": _recipe(
            "business_active_flow_imb", {}, lookback="30s", max_age="5s",
            inputs={"buy": "buy", "sell": "sell",
                    "buy_reset": "buy_reset", "sell_reset": "sell_reset"},
        )},
        {"item_id": "volume_depth_30s", "alias": "volume_depth_30s", "recipe": _recipe(
            "business_volume_to_depth", {}, lookback="30s",
            inputs={"volume": "volume", "depth": "depth"},
        )},
    ]
    return {
        "schema_version": "feature_set_v1",
        "feature_set_id": "real_business_windows",
        "version": 1,
        "name": "Step 04-A ready business subset",
        "description": "Selected ready business windows without pending semantics.",
        "items": items,
        "created_by": {"kind": "agent", "id": "step04a-example"},
        "change_reason": "Freeze selected ready window definitions.",
    }


def _ensure_catalog(database: Path, socket: Path) -> None:
    try:
        _call(database, socket, "catalog.providers.get", {"ref": f"quantsage_business_windows@{CATALOG_VERSION}"})
    except RpcError:
        _call(
            database, socket, "catalog.providers.register",
            {"document": provider_document()},
        )
        for document in business_template_documents():
            _call(database, socket, "catalog.templates.register", {"document": document})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--socket", required=True, type=Path)
    parser.add_argument("--source-ref", default="cffex_treasury_l2_full79@2")
    parser.add_argument("--instrument", default="T2603")
    parser.add_argument("--start", default="2026-01-05T01:29:00+00:00")
    parser.add_argument("--end", default="2026-01-05T07:15:00+00:00")
    parser.add_argument("--sessions", default=(
        "CN_STOCK:2026-01-05:opening_auction_matching,"
        "CN_STOCK:2026-01-05:continuous_morning,"
        "CN_STOCK:2026-01-05:continuous_afternoon"
    ))
    parser.add_argument("--run-key", default="step04a-real-t2603-20260105")
    args = parser.parse_args()
    db, sock = args.database.resolve(), args.socket.resolve()
    sessions = [value for value in args.sessions.split(",") if value]
    source = _call(db, sock, "sources.get", {"ref": args.source_ref})
    policy_path = Path(source["access_policy_ref"]["uri"].removeprefix("file://"))
    split_ref = json.loads(policy_path.read_text(encoding="utf-8"))["split_refs"][0]
    _install_catalog(db, sock)
    _ensure_catalog(db, sock)
    existing = _optional_get(db, sock, "feature_sets.get", "real_business_windows@1")
    if existing is None:
        existing = _call(db, sock, "feature_sets.save", {"document": _document()})
    if existing.get("status") != "frozen":
        existing = _call(db, sock, "feature_sets.freeze", {
            "ref": "real_business_windows@1",
            "expected_revision": existing["draft_revision"],
        })
    study_doc = {
        "schema_version": "study_v1",
        "study_id": "real_business_window_study",
        "name": "Real Step 04-A business windows",
        "objective": "Compute selected ready business windows.",
        "status": "active",
        "source_refs": [args.source_ref],
        "feature_set_refs": ["real_business_windows@1"],
        "backlog": [],
        "limits": {
            "allowed_steps": [3, 4],
            "allowed_scope": [{
                "instruments": [args.instrument], "start": args.start,
                "end": args.end, "session_filter": sessions,
            }],
            "budget": {"max_work_units": 200, "max_partitions": 100,
                       "max_concurrent_jobs": 2},
        },
        "created_by": {"kind": "agent", "id": "step04a-example"},
        "change_reason": "Create bounded ready business-window scope.",
    }
    try:
        study = _call(db, sock, "studies.get", {"study_id": study_doc["study_id"]})
    except RpcError as exc:
        if "unknown" not in str(exc).lower():
            raise
        study = _call(db, sock, "studies.create", {"document": study_doc})
    request = {
        "schema_version": "run_request_v1",
        "study_id": study["study_id"],
        "study_revision": study["revision"],
        "feature_set_ref": "real_business_windows@1",
        "source_refs": [args.source_ref],
        "split_ref": split_ref,
        "scope": {"instruments": [args.instrument], "start": args.start,
                  "end": args.end, "session_filter": sessions},
        "allowed_steps": [3, 4],
        "missing_dependencies": "compute_within_allowed_steps",
        "reuse_policy": "verified_only",
        "selected_item_ids": [
            "obi_30s", "wobi_1m", "coverage_5m", "capacity_10m",
            "active_flow_30s", "volume_depth_30s",
        ],
        "budget": {"max_work_units": 200, "max_partitions": 100,
                   "max_concurrent_jobs": 2},
        "created_by": {"kind": "agent", "id": "step04a-example"},
    }
    plan = _call(db, sock, "runs.plan", {"request": request})
    run = _call(db, sock, "runs.submit", {
        "request": request, "request_key": args.run_key,
        "expected_plan_hash": plan["plan_hash"],
    })
    _call(db, sock, "runs.execute", {"run_id": run["run_id"]})
    final = _wait_for_run(db, sock, run["run_id"])
    results = _call(db, sock, "results.list", {
        "filters": {"run_id": run["run_id"]}, "limit": 100, "verify": True,
    })
    print(json.dumps({
        "run_id": run["run_id"], "status": final["status"],
        "plan_summary": plan["summary"], "result_count": len(results["items"]),
        "selected_business_items": request["selected_item_ids"],
        "pending_not_selected": ["fill_dist_asym", "walk_cost_asym"],
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
