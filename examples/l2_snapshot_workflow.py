"""Run selected tick-independent Step 03-B snapshot descriptions and terminal inputs."""

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
from quantsage_discovery.research.l2_primitives import CATALOG_VERSION
from quantsage_discovery.research.rpc import RpcError

SELECTED_RECIPES = {
    "mid": ("l2_mid", {}),
    "obi_d0": ("l2_obi_snapshot", {"space": "d0"}),
    "wobi_d0": ("l2_wobi_snapshot", {"space": "d0"}),
    "min_coverage_d0_q25": (
        "l2_min_coverage_snapshot", {"space": "d0", "size": "q25"},
    ),
    "min_coverage_d0_q50": (
        "l2_min_coverage_snapshot", {"space": "d0", "size": "q50"},
    ),
    "min_coverage_d0_q100": (
        "l2_min_coverage_snapshot", {"space": "d0", "size": "q100"},
    ),
    "capacity_d0_q25": (
        "l2_capacity_imb_snapshot", {"space": "d0", "size": "q25"},
    ),
    "capacity_d0_q50": (
        "l2_capacity_imb_snapshot", {"space": "d0", "size": "q50"},
    ),
    "capacity_d0_q100": (
        "l2_capacity_imb_snapshot", {"space": "d0", "size": "q100"},
    ),
    "depth_ref_5": ("l2_depth_ref_5", {}),
    "target_size_q25": ("l2_target_size_q25", {}),
    "target_size_q50": ("l2_target_size_q50", {}),
    "target_size_q100": ("l2_target_size_q100", {}),
    "active_buy_cumulative": ("l2_active_buy_cumulative", {}),
    "active_sell_cumulative": ("l2_active_sell_cumulative", {}),
    "buy_cumulative_reset": ("l2_buy_cumulative_reset", {}),
    "sell_cumulative_reset": ("l2_sell_cumulative_reset", {}),
    "trade_volume_interval": ("l2_trade_volume_interval", {}),
}


def _document(feature_set_id: str) -> dict:
    return {
        "schema_version": "feature_set_v1",
        "feature_set_id": feature_set_id,
        "version": 1,
        "name": "Step 03-B selected snapshot descriptions",
        "description": "Tick-independent d0 descriptions and Step 04 terminal inputs.",
        "items": [{
            "item_id": item_id,
            "alias": item_id,
            "recipe": {
                "template_ref": f"{template_id}@{CATALOG_VERSION}",
                "step": 3,
                "parameters": parameters,
            },
        } for item_id, (template_id, parameters) in SELECTED_RECIPES.items()],
        "created_by": {"kind": "agent", "id": "step03b-example"},
        "change_reason": "Freeze selected snapshot descriptions without horizon expansion.",
    }


def _ensure_feature_set(database: Path, socket: Path, feature_set_id: str) -> dict:
    ref = f"{feature_set_id}@1"
    result = _optional_get(database, socket, "feature_sets.get", ref)
    if result is None:
        result = _call(
            database, socket, "feature_sets.save", {"document": _document(feature_set_id)},
        )
    if result.get("status") != "frozen":
        result = _call(
            database, socket, "feature_sets.freeze",
            {"ref": ref, "expected_revision": result["draft_revision"]},
        )
    return result


def _study(
    study_id: str,
    source_ref: str,
    feature_set_ref: str,
    instrument: str,
    start: str,
    end: str,
    sessions: list[str],
) -> dict:
    return {
        "schema_version": "study_v1",
        "study_id": study_id,
        "name": "Real Step 03-B snapshot scope",
        "objective": "Compute selected snapshot descriptions and terminal inputs.",
        "status": "active",
        "source_refs": [source_ref],
        "feature_set_refs": [feature_set_ref],
        "backlog": [],
        "limits": {
            "allowed_steps": [3],
            "allowed_scope": [{
                "instruments": [instrument],
                "start": start,
                "end": end,
                "session_filter": sessions,
            }],
            "budget": {
                "max_work_units": 100,
                "max_partitions": 100,
                "max_concurrent_jobs": 2,
            },
        },
        "created_by": {"kind": "agent", "id": "step03b-example"},
        "change_reason": "Create a bounded descriptive snapshot scope.",
    }


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
    parser.add_argument("--study-id", default="real_l2_snapshot_study")
    parser.add_argument("--feature-set-id", default="real_l2_snapshot_primitives")
    parser.add_argument("--run-key", default="step03b-real-t2603-20260105")
    args = parser.parse_args()
    database, socket = args.database.resolve(), args.socket.resolve()
    sessions = [value for value in args.sessions.split(",") if value]
    source = _call(database, socket, "sources.get", {"ref": args.source_ref})
    policy_path = Path(source["access_policy_ref"]["uri"].removeprefix("file://"))
    split_ref = json.loads(policy_path.read_text(encoding="utf-8"))["split_refs"][0]
    _install_catalog(database, socket)
    feature_set = _ensure_feature_set(database, socket, args.feature_set_id)
    feature_set_ref = f"{args.feature_set_id}@1"
    try:
        study = _call(database, socket, "studies.get", {"study_id": args.study_id})
    except RpcError as exc:
        if "unknown" not in str(exc).lower():
            raise
        study = _call(database, socket, "studies.create", {"document": _study(
            args.study_id, args.source_ref, feature_set_ref, args.instrument,
            args.start, args.end, sessions,
        )})
    request = {
        "schema_version": "run_request_v1",
        "study_id": study["study_id"],
        "study_revision": study["revision"],
        "feature_set_ref": feature_set_ref,
        "source_refs": [args.source_ref],
        "split_ref": split_ref,
        "scope": {
            "instruments": [args.instrument],
            "start": args.start,
            "end": args.end,
            "session_filter": sessions,
        },
        "allowed_steps": [3],
        "missing_dependencies": "block",
        "reuse_policy": "verified_only",
        "selected_item_ids": list(SELECTED_RECIPES),
        "budget": {
            "max_work_units": 100,
            "max_partitions": 100,
            "max_concurrent_jobs": 2,
        },
        "created_by": {"kind": "agent", "id": "step03b-example"},
    }
    plan = _call(database, socket, "runs.plan", {"request": request})
    run = _call(database, socket, "runs.submit", {
        "request": request,
        "request_key": args.run_key,
        "expected_plan_hash": plan["plan_hash"],
    })
    _call(database, socket, "runs.execute", {"run_id": run["run_id"]})
    final = _wait_for_run(database, socket, run["run_id"])
    results = _call(database, socket, "results.list", {
        "filters": {"run_id": run["run_id"], "source_ref": args.source_ref},
        "limit": 100,
        "verify": True,
    })
    print(json.dumps({
        "run_id": run["run_id"],
        "status": final["status"],
        "plan_summary": plan["summary"],
        "result_count": len(results["items"]),
        "selected_item_count": len(feature_set["resolved_items"]),
        "catalog_version": CATALOG_VERSION,
        "tick_dependent_selection_skipped": True,
        "note": "d0 coverage/capacity are computed; d1+ and F06-F08 remain tick-gated.",
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
