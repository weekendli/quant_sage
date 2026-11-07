"""Run selected Step 03-A primitives through the local ResearchService owner.

This example intentionally runs one real contract/day/session scope. It does not
expand the full primitive grid and does not fit baselines or future labels.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from quantsage_discovery.research.l2_primitives import (
    CATALOG_VERSION,
    primitive_template_documents,
    provider_document,
)
from quantsage_discovery.research.rpc import RpcError, rpc_call

SELECTED_ITEMS = [
    "mid",
    "depth_bid_1",
    "depth_ask_1",
    "depth_imbalance_1",
    "microprice_proxy",
    "quote_interarrival",
]


def _call(database: Path, socket: Path, method: str, params: dict[str, Any]) -> Any:
    response = rpc_call(database, method, params, socket_path=socket, timeout=60)
    if not response.get("ok"):
        error = response.get("error") or {}
        raise RpcError(
            f"{method}: {error.get('code', 'RPC_ERROR')}: {error.get('message', 'failed')}"
        )
    return response["data"]


def _optional_get(database: Path, socket: Path, method: str, ref: str) -> dict[str, Any] | None:
    try:
        return _call(database, socket, method, {"ref": ref})
    except RpcError as exc:
        if "UNKNOWN" in str(exc) or "unknown" in str(exc):
            return None
        raise


def _install_catalog(database: Path, socket: Path) -> None:
    provider = provider_document()
    provider_ref = f"{provider['provider_id']}@{provider['version']}"
    existing = _optional_get(database, socket, "catalog.providers.get", provider_ref)
    if existing is None:
        _call(database, socket, "catalog.providers.register", {"document": provider})
    for template in primitive_template_documents():
        template_ref = f"{template['template_id']}@{template['version']}"
        existing = _optional_get(database, socket, "catalog.templates.get", template_ref)
        if existing is None:
            _call(database, socket, "catalog.templates.register", {"document": template})


def _feature_set_document(feature_set_id: str, version: int) -> dict[str, Any]:
    return {
        "schema_version": "feature_set_v1",
        "feature_set_id": feature_set_id,
        "version": version,
        "name": "Step 03-A selected primitives",
        "description": "Tick-independent basic L2 snapshot primitives.",
        "items": [
            {
                "item_id": item_id,
                "alias": item_id,
                "recipe": {
                    "template_ref": f"l2_{item_id}@{CATALOG_VERSION}",
                    "step": 3,
                    "parameters": {},
                },
            }
            for item_id in SELECTED_ITEMS
        ],
        "created_by": {"kind": "agent", "id": "step03a-example"},
        "change_reason": "Freeze one bounded set of tick-independent primitives.",
    }


def _ensure_feature_set(
    database: Path,
    socket: Path,
    feature_set_id: str,
    version: int,
) -> dict[str, Any]:
    ref = f"{feature_set_id}@{version}"
    existing = _optional_get(database, socket, "feature_sets.get", ref)
    if existing is None:
        existing = _call(
            database, socket, "feature_sets.save",
            {"document": _feature_set_document(feature_set_id, version)},
        )
    if existing.get("status") != "frozen":
        existing = _call(
            database, socket, "feature_sets.freeze",
            {"ref": ref, "expected_revision": existing["draft_revision"]},
        )
    return existing


def _study_document(
    study_id: str,
    source_ref: str,
    feature_set_ref: str,
    instrument_id: str,
    start: str,
    end: str,
    sessions: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": "study_v1",
        "study_id": study_id,
        "name": "Real Step 03-A primitive scope",
        "objective": "Compute selected descriptive L2 primitives only.",
        "status": "active",
        "source_refs": [source_ref],
        "feature_set_refs": [feature_set_ref],
        "backlog": [],
        "limits": {
            "allowed_steps": [3],
            "allowed_scope": [{
                "instruments": [instrument_id],
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
        "created_by": {"kind": "agent", "id": "step03a-example"},
        "change_reason": "Create the bounded primitive execution scope.",
    }


def _wait_for_run(
    database: Path,
    socket: Path,
    run_id: str,
    *,
    timeout_seconds: float = 300,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        run = _call(database, socket, "runs.get", {"run_id": run_id})
        if run.get("status") in {"COMPLETE", "PARTIAL", "FAILED", "BLOCKED", "NO_WORK"}:
            return run
        time.sleep(1)
    raise TimeoutError(f"Run {run_id} did not reach a terminal state in time")


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
    parser.add_argument("--study-id", default="real_l2_primitive_study_v3")
    parser.add_argument("--feature-set-id", default="real_l2_primitives")
    parser.add_argument("--feature-set-version", default=3, type=int)
    parser.add_argument("--run-key", default="step03a-real-t2603-20260105-r4")
    args = parser.parse_args()
    sessions = [value for value in args.sessions.split(",") if value]
    database = args.database.resolve()
    socket = args.socket.resolve()

    source = _call(database, socket, "sources.get", {"ref": args.source_ref})
    policy_path = Path(source["access_policy_ref"]["uri"].removeprefix("file://"))
    access_policy = json.loads(policy_path.read_text(encoding="utf-8"))
    split_ref = access_policy["split_refs"][0]

    _install_catalog(database, socket)
    _ensure_feature_set(database, socket, args.feature_set_id, args.feature_set_version)
    feature_set_ref = f"{args.feature_set_id}@{args.feature_set_version}"
    try:
        study = _call(database, socket, "studies.get", {"study_id": args.study_id})
    except RpcError as exc:
        if "unknown" not in str(exc).lower():
            raise
        study = _call(
            database, socket, "studies.create",
            {"document": _study_document(
                args.study_id, args.source_ref, feature_set_ref, args.instrument,
                args.start, args.end, sessions,
            )},
        )

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
        "selected_item_ids": SELECTED_ITEMS,
        "budget": {
            "max_work_units": 100,
            "max_partitions": 100,
            "max_concurrent_jobs": 2,
        },
        "created_by": {"kind": "agent", "id": "step03a-example"},
    }
    plan = _call(database, socket, "runs.plan", {"request": request})
    run = _call(
        database, socket, "runs.submit",
        {
            "request": request,
            "request_key": args.run_key,
            "expected_plan_hash": plan["plan_hash"],
        },
    )
    _call(database, socket, "runs.execute", {"run_id": run["run_id"]})
    final = _wait_for_run(database, socket, run["run_id"])
    results = _call(
        database, socket, "results.list",
        {
            "filters": {"run_id": run["run_id"], "source_ref": args.source_ref},
            "limit": 100,
            "verify": True,
        },
    )
    print(json.dumps({
        "run_id": run["run_id"],
        "status": final["status"],
        "plan_summary": plan["summary"],
        "result_count": len(results["items"]),
        "result_ids": [item["result_id"] for item in results["items"]],
        "selected_items": SELECTED_ITEMS,
        "tick_dependent_items_skipped": ["spread_ticks"],
        "skip_reason": "Real user market metadata has not been supplied.",
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
