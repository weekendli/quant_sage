"""Run the confirmed F1/F2 counterexamples through serialized research Runs."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from quantsage_discovery.core import Registry
from quantsage_discovery.core.artifacts import artifact_path
from quantsage_discovery.research.business_windows import (
    CATALOG_VERSION as BUSINESS_VERSION,
)
from quantsage_discovery.research.business_windows import register_business_catalog
from quantsage_discovery.research.l2_primitives import (
    CATALOG_VERSION as PRIMITIVE_VERSION,
)
from quantsage_discovery.research.l2_primitives import register_l2_primitive_catalog
from quantsage_discovery.research.service import ResearchService
from quantsage_discovery.research.statistical_windows import (
    CATALOG_VERSION as STATISTICAL_VERSION,
)
from quantsage_discovery.research.statistical_windows import register_statistical_catalog

ROOT = Path(__file__).resolve().parents[3]
SOURCE_DIR = ROOT / "artifacts/step02/adapters/step02-02a-20261107-r3/cffex_treasury_l2-v2"
SOURCE_REF = "remediation_cffex_full79@1"
SESSION = "CN_STOCK:2026-06-12:continuous_morning"
TARGET = datetime(2026, 6, 12, 1, 34, 11, 800000, tzinfo=UTC)
GAP_EVENT_ID = "767c1fecbf12259a834995654f3bb6753b04e8d5806f261a40824530a4c01592"
GAP_RECORD_ID = "5a17031a699f184f651d4eafcefdd65f58329739c48fd2eaa55b07164b5ad2f5"


def _file_ref(path: Path) -> dict[str, str]:
    return {
        "uri": path.resolve().as_uri(),
        "content_hash": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _source_alias(workspace: Path, service: ResearchService) -> dict[str, Any]:
    original_source = json.loads((SOURCE_DIR / "source.json").read_text(encoding="utf-8"))
    original_manifest = json.loads(
        (SOURCE_DIR / "data_source_manifest.json").read_text(encoding="utf-8")
    )
    source_id, version = SOURCE_REF.split("@", 1)
    manifest_document = {
        **original_manifest,
        "source_id": source_id,
        "version": int(version),
    }
    manifest_path = workspace / "source_manifest.json"
    _write_json(manifest_path, manifest_document)
    source_document = {
        **original_source,
        "source_id": source_id,
        "version": int(version),
        "manifest_ref": _file_ref(manifest_path),
    }
    source_path = workspace / "source.json"
    _write_json(source_path, source_document)
    return service.register_source(source_document)


def _feature_set(service: ResearchService) -> dict[str, Any]:
    document = {
        "schema_version": "feature_set_v1",
        "feature_set_id": "remediation_windows",
        "version": 1,
        "name": "F1/F2 remediation windows",
        "description": "Serialized OBI business window and downstream statistical mean.",
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
                "item_id": "obi_1m",
                "alias": "obi_1m",
                "recipe": {
                    "template_ref": f"business_obi@{BUSINESS_VERSION}",
                    "step": 4,
                    "parameters": {"space": "d0"},
                    "lookback": "1m",
                    "input": {"item_id": "obi_primitive"},
                },
            },
            {
                "item_id": "obi_mean_10s",
                "alias": "obi_mean_10s",
                "recipe": {
                    "template_ref": f"statistical_trailing@{STATISTICAL_VERSION}",
                    "step": 5,
                    "parameters": {},
                    "input": {"item_id": "obi_1m"},
                    "profile_window": "10s",
                    "sampling": {
                        "axis": "clock_time",
                        "interval": "1s",
                        "evaluation_stride": "5s",
                        "max_feature_age": "30m",
                    },
                    "validity": {"min_samples": 1},
                    "metrics": [f"stat.mean@{STATISTICAL_VERSION}"],
                },
            },
        ],
        "created_by": {"kind": "agent", "id": "remediation-verifier"},
        "change_reason": "Freeze serialized F1/F2 regressions.",
    }
    draft = service.save_feature_set(document)
    return service.freeze_feature_set(
        "remediation_windows@1", expected_revision=draft["draft_revision"],
    )


def _study(service: ResearchService, split_ref: dict[str, str]) -> dict[str, Any]:
    del split_ref
    scope = {
        "instruments": ["TS2606"],
        "start": "2026-06-12T01:25:00+00:00",
        "end": "2026-06-12T01:45:00+00:00",
        "session_filter": [SESSION],
    }
    return service.create_study({
        "schema_version": "study_v1",
        "study_id": "remediation_study",
        "name": "F1/F2 remediation verification",
        "objective": "Verify gap propagation, recovery and output-scope invariance.",
        "status": "active",
        "source_refs": [SOURCE_REF],
        "feature_set_refs": ["remediation_windows@1"],
        "default_feature_set_ref": "remediation_windows@1",
        "default_scope": scope,
        "backlog": [],
        "limits": {
            "allowed_steps": [3, 4, 5],
            "allowed_scope": [scope],
            "budget": {
                "max_work_units": 20,
                "max_partitions": 5,
                "max_concurrent_jobs": 2,
            },
        },
        "created_by": {"kind": "agent", "id": "remediation-verifier"},
        "change_reason": "Create isolated verification scope.",
    })


def _request(
    study: dict[str, Any],
    split_ref: dict[str, str],
    *,
    start: str,
    end: str,
    selected_item_ids: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": "run_request_v1",
        "study_id": study["study_id"],
        "study_revision": study["revision"],
        "feature_set_ref": "remediation_windows@1",
        "source_refs": [SOURCE_REF],
        "split_ref": split_ref,
        "scope": {
            "instruments": ["TS2606"],
            "start": start,
            "end": end,
            "session_filter": [SESSION],
        },
        "allowed_steps": [3, 4, 5],
        "missing_dependencies": "compute_within_allowed_steps",
        "reuse_policy": "verified_only",
        "selected_item_ids": selected_item_ids,
        "budget": {
            "max_work_units": 20,
            "max_partitions": 5,
            "max_concurrent_jobs": 2,
        },
        "created_by": {"kind": "agent", "id": "remediation-verifier"},
    }


def _execute(
    service: ResearchService,
    request: dict[str, Any],
    request_key: str,
) -> dict[str, Any]:
    plan = service.plan_run(request)
    submitted = service.submit_run(
        request,
        request_key=request_key,
        expected_plan_hash=plan["plan_hash"],
    )
    return service.execute_run(submitted["run_id"])


def _result_for_item(
    registry: Registry,
    run: dict[str, Any],
    item_id: str,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    execution = next(
        item for item in run["item_executions"] if item_id in item.get("item_ids", [])
    )
    result_id = execution["result_refs"][-1]
    result = registry.get_feature_result(result_id)
    refs = {
        descriptor["artifact_role"]: descriptor["artifact_ref"]
        for descriptor in result["artifacts"]
    }
    return result, refs


def _rows(ref: dict[str, Any]) -> list[dict[str, Any]]:
    return pq.read_table(artifact_path(ref["uri"])).to_pylist()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    args = parser.parse_args()
    workspace = args.workspace.resolve()
    workspace.mkdir(parents=True, exist_ok=True)

    registry = Registry(workspace / "research.duckdb")
    try:
        service = ResearchService(registry)
        source = _source_alias(workspace, service)
        register_l2_primitive_catalog(service)
        register_business_catalog(service)
        register_statistical_catalog(service)
        frozen = _feature_set(service)
        policy_path = Path(source["access_policy_ref"]["uri"].removeprefix("file://"))
        split_ref = json.loads(policy_path.read_text(encoding="utf-8"))["split_refs"][0]
        study = _study(service, split_ref)

        wide = _execute(
            service,
            _request(
                study,
                split_ref,
                start="2026-06-12T01:30:00+00:00",
                end="2026-06-12T01:40:00+00:00",
                selected_item_ids=["obi_1m"],
            ),
            "remediation-wide",
        )
        narrow = _execute(
            service,
            _request(
                study,
                split_ref,
                start="2026-06-12T01:36:00+00:00",
                end="2026-06-12T01:40:00+00:00",
                selected_item_ids=["obi_1m"],
            ),
            "remediation-narrow",
        )
        statistical = _execute(
            service,
            _request(
                study,
                split_ref,
                start="2026-06-12T01:33:50+00:00",
                end="2026-06-12T01:37:30+00:00",
                selected_item_ids=["obi_mean_10s"],
            ),
            "remediation-statistical",
        )

        for run in (wide, narrow, statistical):
            if run["status"] != "COMPLETE":
                raise RuntimeError(f"Run {run['run_id']} is {run['status']}")

        wide_result, wide_refs = _result_for_item(registry, wide, "obi_1m")
        narrow_result, narrow_refs = _result_for_item(registry, narrow, "obi_1m")
        stat_result, stat_refs = _result_for_item(registry, statistical, "obi_mean_10s")
        wide_rows = _rows(wide_refs["feature_values"])
        narrow_rows = _rows(narrow_refs["feature_values"])
        wide_audit = {row["event_time"]: row for row in _rows(wide_refs["window_audit"])}
        stat_audit = _rows(stat_refs["statistical_audit"])

        target_value = next(row for row in wide_rows if row["event_time"] == TARGET)
        target_audit = wide_audit[TARGET]
        if target_value["value"] is not None:
            raise RuntimeError("real gap target unexpectedly published a numeric OBI")
        if target_audit["invalid_reason"] != "KNOWN_SOURCE_GAP":
            raise RuntimeError("real gap target lacks KNOWN_SOURCE_GAP")
        if GAP_EVENT_ID not in target_audit["source_gap_event_ids"]:
            raise RuntimeError("real gap target lacks the required gap event lineage")
        if GAP_RECORD_ID not in target_audit["source_gap_evidence_record_ids"]:
            raise RuntimeError("real gap target lacks the quarantined source record lineage")

        control_time = datetime(2026, 6, 12, 1, 31, 24, 800000, tzinfo=UTC)
        control = next(row for row in wide_rows if row["event_time"] == control_time)
        if control["value"] is None or wide_audit[control_time]["invalid_reason"] is not None:
            raise RuntimeError("pre-gap normal control was not valid")
        recovery_time = datetime(2026, 6, 12, 1, 36, 17, 300000, tzinfo=UTC)
        recovery = next(row for row in wide_rows if row["event_time"] == recovery_time)
        if recovery["value"] is None or wide_audit[recovery_time]["invalid_reason"] is not None:
            raise RuntimeError("post-gap recovery did not restore a valid OBI")

        narrow_by_time = {row["event_time"]: row for row in narrow_rows}
        common = []
        for row in wide_rows:
            if row["event_time"] in narrow_by_time:
                common.append((
                    row["event_time"],
                    row["value"],
                    narrow_by_time[row["event_time"]]["value"],
                ))
        if not common or any(left != right for _, left, right in common):
            raise RuntimeError("wide/narrow Runs disagree at common serialized timestamps")

        known_parent_gaps = [
            row for row in stat_audit
            if row["invalid_reason"] == "KNOWN_SOURCE_GAP"
            and GAP_EVENT_ID in (row["source_gap_event_ids"] or [])
        ]
        if not known_parent_gaps:
            raise RuntimeError("statistical audit did not inherit the parent source gap")

        report = {
            "schema_version": "remediation_verification_v1",
            "source_ref": SOURCE_REF,
            "source_content_hash": source["content_hash"],
            "gap_manifest_ref": source["gap_manifest_ref"],
            "feature_set": {
                "ref": "remediation_windows@1",
                "config_hash": frozen["config_hash"],
            },
            "runs": {
                "wide": {
                    "run_id": wide["run_id"],
                    "result_id": wide_result["result_id"],
                    "row_count": wide_result["quality"]["n_total"],
                },
                "narrow": {
                    "run_id": narrow["run_id"],
                    "result_id": narrow_result["result_id"],
                    "row_count": narrow_result["quality"]["n_total"],
                },
                "statistical": {
                    "run_id": statistical["run_id"],
                    "result_id": stat_result["result_id"],
                    "row_count": stat_result["quality"]["n_total"],
                },
            },
            "gap": {
                "target_time": TARGET.isoformat(),
                "value": target_value["value"],
                "invalid_reason": target_audit["invalid_reason"],
                "event_id": GAP_EVENT_ID,
                "quarantined_record_id": GAP_RECORD_ID,
                "control_value": control["value"],
                "recovery_value": recovery["value"],
                "statistical_known_gap_rows": len(known_parent_gaps),
            },
            "output_scope_invariance": {
                "common_rows": len(common),
                "first_common_time": common[0][0].isoformat(),
                "last_common_time": common[-1][0].isoformat(),
                "all_values_equal": True,
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
