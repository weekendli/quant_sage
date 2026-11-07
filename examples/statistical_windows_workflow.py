"""Run selected Step 05-A primitive-only trailing statistical profiles."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from examples.l2_primitives_workflow import _call, _install_catalog, _optional_get, _wait_for_run
from quantsage_discovery.research.l2_primitives import CATALOG_VERSION as PRIMITIVE_VERSION
from quantsage_discovery.research.rpc import RpcError
from quantsage_discovery.research.statistical_windows import (
    CATALOG_VERSION,
    metric_documents,
    metric_set_document,
    provider_document,
    statistical_template_document,
)


def _install(database: Path, socket: Path) -> None:
    resources = [
        ("catalog.providers.get", "catalog.providers.register",
         f"quantsage_statistical_windows@{CATALOG_VERSION}", provider_document()),
        ("catalog.templates.get", "catalog.templates.register",
         f"statistical_trailing@{CATALOG_VERSION}", statistical_template_document()),
        *[("catalog.metrics.get", "catalog.metrics.register",
           f"{document['metric_id']}@{document['version']}", document)
          for document in metric_documents()],
        ("catalog.metric_sets.get", "catalog.metric_sets.register",
         "basic_description@2", metric_set_document()),
    ]
    for get_method, register_method, ref, document in resources:
        try:
            _call(database, socket, get_method, {"ref": ref})
        except RpcError:
            _call(database, socket, register_method, {"document": document})


def _stat_item(item_id: str, metric: str) -> dict:
    return {
        "item_id": item_id,
        "alias": item_id,
        "recipe": {
            "template_ref": f"statistical_trailing@{CATALOG_VERSION}",
            "step": 5,
            "parameters": {},
            "input": {"item_id": "obi_primitive"},
            "profile_window": "10s",
            "sampling": {
                "axis": "clock_time",
                "interval": "1s",
                "evaluation_stride": "5s",
                "max_feature_age": "2s",
            },
            "validity": {"min_samples": 1},
            "metrics": [f"stat.{metric}@{CATALOG_VERSION}"],
        },
    }


def _document() -> dict:
    return {
        "schema_version": "feature_set_v1",
        "feature_set_id": "real_statistical_windows",
        "version": 2,
        "name": "Step 05-A primitive-only profile",
        "description": "Selected trailing mean and standard deviation.",
        "items": [
            {
                "item_id": "obi_primitive", "alias": "obi_primitive",
                "recipe": {
                    "template_ref": f"l2_obi_snapshot@{PRIMITIVE_VERSION}",
                    "step": 3, "parameters": {"space": "d0"},
                },
            },
            _stat_item("obi_mean_10s", "mean"),
            _stat_item("obi_std_10s", "std"),
        ],
        "created_by": {"kind": "agent", "id": "step05a-example"},
        "change_reason": "Freeze selected primitive-only statistical metrics.",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--socket", required=True, type=Path)
    parser.add_argument("--source-ref", default="cffex_treasury_l2_full79@2")
    parser.add_argument("--instrument", default="T2603")
    parser.add_argument("--start", default="2026-01-05T01:30:00+00:00")
    parser.add_argument("--end", default="2026-01-05T03:30:00+00:00")
    parser.add_argument("--session", default="CN_STOCK:2026-01-05:continuous_morning")
    parser.add_argument("--run-key", default="step05a-real-t2603-morning-v2")
    args = parser.parse_args()
    db, sock = args.database.resolve(), args.socket.resolve()
    source = _call(db, sock, "sources.get", {"ref": args.source_ref})
    access = Path(source["access_policy_ref"]["uri"].removeprefix("file://"))
    split_ref = json.loads(access.read_text(encoding="utf-8"))["split_refs"][0]
    _install_catalog(db, sock)
    _install(db, sock)
    feature_set = _optional_get(db, sock, "feature_sets.get", "real_statistical_windows@2")
    if feature_set is None:
        feature_set = _call(db, sock, "feature_sets.save", {"document": _document()})
    if feature_set.get("status") != "frozen":
        feature_set = _call(db, sock, "feature_sets.freeze", {
            "ref": "real_statistical_windows@2",
            "expected_revision": feature_set["draft_revision"],
        })
    study_doc = {
        "schema_version": "study_v1", "study_id": "real_statistical_window_study_v2",
        "name": "Real Step 05-A primitive profile",
        "objective": "Compute selected 05-A descriptions without Step 04.",
        "status": "active", "source_refs": [args.source_ref],
        "feature_set_refs": ["real_statistical_windows@2"], "backlog": [],
        "limits": {
            "allowed_steps": [3, 5],
            "allowed_scope": [{
                "instruments": [args.instrument], "start": args.start,
                "end": args.end, "session_filter": [args.session],
            }],
            "budget": {"max_work_units": 100, "max_partitions": 20,
                       "max_concurrent_jobs": 2},
        },
        "created_by": {"kind": "agent", "id": "step05a-example"},
        "change_reason": "Create bounded primitive-only profile scope.",
    }
    try:
        study = _call(db, sock, "studies.get", {"study_id": study_doc["study_id"]})
    except RpcError as exc:
        if "unknown" not in str(exc).lower():
            raise
        study = _call(db, sock, "studies.create", {"document": study_doc})
    request = {
        "schema_version": "run_request_v1", "study_id": study["study_id"],
        "study_revision": study["revision"],
        "feature_set_ref": "real_statistical_windows@2",
        "source_refs": [args.source_ref], "split_ref": split_ref,
        "scope": {"instruments": [args.instrument], "start": args.start,
                  "end": args.end, "session_filter": [args.session]},
        "allowed_steps": [3, 5], "missing_dependencies": "compute_within_allowed_steps",
        "reuse_policy": "verified_only",
        "selected_item_ids": ["obi_mean_10s", "obi_std_10s"],
        "budget": {"max_work_units": 100, "max_partitions": 20,
                   "max_concurrent_jobs": 2},
        "created_by": {"kind": "agent", "id": "step05a-example"},
    }
    plan = _call(db, sock, "runs.plan", {"request": request})
    run = _call(db, sock, "runs.submit", {
        "request": request, "request_key": args.run_key,
        "expected_plan_hash": plan["plan_hash"],
    })
    _call(db, sock, "runs.execute", {"run_id": run["run_id"]})
    final = _wait_for_run(db, sock, run["run_id"])
    results = _call(db, sock, "results.list", {
        "filters": {"run_id": run["run_id"]}, "limit": 20, "verify": True,
    })
    print(json.dumps({"run_id": run["run_id"], "status": final["status"],
                      "plan_summary": plan["summary"],
                      "result_count": len(results["items"])},
                     indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
