"""Verify old accepted results cannot satisfy remediated business definitions."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any

from quantsage_discovery.core import Registry
from quantsage_discovery.research.business_windows import (
    CATALOG_VERSION as BUSINESS_VERSION,
)
from quantsage_discovery.research.business_windows import register_business_catalog
from quantsage_discovery.research.l2_primitives import (
    CATALOG_VERSION as PRIMITIVE_VERSION,
)
from quantsage_discovery.research.service import ResearchService

SOURCE_REF = "acceptance_cffex_full79@1"
MORNING = "CN_STOCK:2026-01-05:continuous_morning"
SCOPE = {
    "instruments": ["T2603"],
    "start": "2026-01-05T01:30:00+00:00",
    "end": "2026-01-05T03:30:00+00:00",
    "session_filter": [MORNING],
}


def _feature_set(service: ResearchService) -> dict[str, Any]:
    document = {
        "schema_version": "feature_set_v1",
        "feature_set_id": "remediation_cache_isolation",
        "version": 1,
        "name": "Remediated cache isolation",
        "description": "Same source, scope and formula as the historical OBI Run.",
        "items": [
            {
                "item_id": "obi_primitive",
                "alias": "obi_primitive",
                "recipe": {
                    "template_ref": f"l2_obi_snapshot@{PRIMITIVE_VERSION}",
                    "step": 3,
                    "parameters": {"space": "d0"},
                },
            },
            {
                "item_id": "obi_30s",
                "alias": "obi_30s",
                "recipe": {
                    "template_ref": f"business_obi@{BUSINESS_VERSION}",
                    "step": 4,
                    "parameters": {"space": "d0"},
                    "lookback": "30s",
                    "input": {"item_id": "obi_primitive"},
                },
            },
        ],
        "created_by": {"kind": "agent", "id": "cache-isolation-verifier"},
        "change_reason": "Test cache isolation from the historical business version.",
    }
    draft = service.save_feature_set(document)
    return service.freeze_feature_set(
        "remediation_cache_isolation@1",
        expected_revision=draft["draft_revision"],
    )


def _study(service: ResearchService) -> dict[str, Any]:
    return service.create_study({
        "schema_version": "study_v1",
        "study_id": "remediation_cache_isolation_study",
        "name": "Remediated cache isolation",
        "objective": "Prove old erroneous results do not satisfy the new definition.",
        "status": "active",
        "source_refs": [SOURCE_REF],
        "feature_set_refs": ["remediation_cache_isolation@1"],
        "default_feature_set_ref": "remediation_cache_isolation@1",
        "default_scope": SCOPE,
        "backlog": [],
        "limits": {
            "allowed_steps": [3, 4],
            "allowed_scope": [SCOPE],
            "budget": {
                "max_work_units": 20,
                "max_partitions": 5,
                "max_concurrent_jobs": 2,
            },
        },
        "created_by": {"kind": "agent", "id": "cache-isolation-verifier"},
        "change_reason": "Create isolated cache verification scope.",
    })


def _request(
    study: dict[str, Any],
    split_ref: dict[str, str],
) -> dict[str, Any]:
    return {
        "schema_version": "run_request_v1",
        "study_id": study["study_id"],
        "study_revision": study["revision"],
        "feature_set_ref": "remediation_cache_isolation@1",
        "source_refs": [SOURCE_REF],
        "split_ref": split_ref,
        "scope": SCOPE,
        "allowed_steps": [3, 4],
        "missing_dependencies": "compute_within_allowed_steps",
        "reuse_policy": "verified_only",
        "selected_item_ids": ["obi_30s"],
        "budget": {
            "max_work_units": 20,
            "max_partitions": 5,
            "max_concurrent_jobs": 2,
        },
        "created_by": {"kind": "agent", "id": "cache-isolation-verifier"},
    }


def _business_execution(run: dict[str, Any]) -> dict[str, Any]:
    return next(
        item for item in run["item_executions"] if "obi_30s" in item.get("item_ids", [])
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-database", required=True, type=Path)
    parser.add_argument("--workspace", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    args = parser.parse_args()
    workspace = args.workspace.resolve()
    if workspace.exists():
        raise RuntimeError(f"workspace already exists: {workspace}")
    workspace.mkdir(parents=True)
    database = workspace / "research.duckdb"
    shutil.copy2(args.old_database, database)

    registry = Registry(database)
    try:
        old_results = registry.list_feature_results()
        old_result_ids = {result["result_id"] for result in old_results}
        service = ResearchService(registry)
        register_business_catalog(service)
        _feature_set(service)
        study = _study(service)
        source = service.get_source(SOURCE_REF)
        policy_path = Path(source["access_policy_ref"]["uri"].removeprefix("file://"))
        split_ref = json.loads(policy_path.read_text(encoding="utf-8"))["split_refs"][0]
        request = _request(study, split_ref)

        first_plan = service.plan_run(request)
        first_business_plan = next(
            work for work in first_plan["work"] if "obi_30s" in work.get("item_ids", [])
        )
        if first_business_plan["cache_candidate"]:
            raise RuntimeError("remediated definition incorrectly matched a cache candidate")
        first_submission = service.submit_run(
            request,
            request_key="remediation-cache-first",
            expected_plan_hash=first_plan["plan_hash"],
        )
        first_run = service.execute_run(first_submission["run_id"])
        first_business = _business_execution(first_run)
        if first_business["state"] != "SUCCEEDED":
            raise RuntimeError(f"first remediated execution is {first_business['state']}")
        first_result_id = first_business["result_refs"][-1]
        if first_result_id in old_result_ids:
            raise RuntimeError("new definition reused an old FeatureResult")

        second_plan = service.plan_run(request)
        second_business_plan = next(
            work for work in second_plan["work"] if "obi_30s" in work.get("item_ids", [])
        )
        if not second_business_plan["cache_candidate"]:
            raise RuntimeError("new definition did not expose its own cache candidate")
        second_submission = service.submit_run(
            request,
            request_key="remediation-cache-second",
            expected_plan_hash=second_plan["plan_hash"],
        )
        second_run = service.execute_run(second_submission["run_id"])
        second_business = _business_execution(second_run)
        if second_business["state"] != "CACHED":
            raise RuntimeError(f"second remediated execution is {second_business['state']}")
        if second_business["cache_source_run_id"] != first_run["run_id"]:
            raise RuntimeError("new cache did not name the remediated producer Run")
        if second_business["result_refs"] != [first_result_id]:
            raise RuntimeError("new cache did not reuse the remediated result")

        report = {
            "schema_version": "remediation_cache_isolation_v1",
            "old_database": str(args.old_database.resolve()),
            "copied_database": str(database),
            "old_result_count": len(old_result_ids),
            "first_run": {
                "run_id": first_run["run_id"],
                "business_state": first_business["state"],
                "result_id": first_result_id,
                "cache_candidate": first_business_plan["cache_candidate"],
            },
            "second_run": {
                "run_id": second_run["run_id"],
                "business_state": second_business["state"],
                "result_id": second_business["result_refs"][0],
                "cache_source_run_id": second_business["cache_source_run_id"],
                "cache_candidate": second_business_plan["cache_candidate"],
            },
        }
        rendered = json.dumps(report, indent=2, sort_keys=True)
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered + "\n", encoding="utf-8")
        print(rendered)
    finally:
        registry.close()


if __name__ == "__main__":
    main()
