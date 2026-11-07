"""Step 03-A hand calculations and real serialized primitive execution."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quantsage_discovery.core.artifacts import artifact_path
from quantsage_discovery.core.registry import Registry
from quantsage_discovery.research import ResearchService
from quantsage_discovery.research.l2_primitives import (
    CATALOG_VERSION,
    L2_PRIMITIVE_AUDIT_SCHEMA,
    compute_l2_primitive,
    register_l2_primitive_catalog,
)
from quantsage_discovery.research.onboarding_adapter import (
    GAP_EVENT_SCHEMA,
    adapt_onboarding_entry,
)
from quantsage_discovery.research.planner import (
    _source_identity_hash,
    _source_requirements_block,
)
from tests.test_onboarding_adapter import _fixture_entry

SESSION_ID = "CN_STOCK:2026-01-05:continuous_morning"
START = datetime(2026, 1, 5, 1, 30, tzinfo=UTC)
READ_SCOPE = {"start": START.isoformat(), "end": (START + timedelta(minutes=1)).isoformat()}


def _quote_table(
    rows: list[dict[str, Any]] | None = None,
    *,
    zero_denominator: bool = False,
) -> pa.Table:
    values = rows or [
        (0.0, "100.00", "100.50", "6.00", "2.00"),
        (1.5, "100.00", "100.25", "4.00", "8.00"),
        (4.0, "99.75", "100.25", "5.00", "5.00"),
        (5.25, "99.75", "100.00", "3.00", "7.00"),
        (9.0, "99.50", "100.00", "2.00", "9.00"),
        (10.0, "99.50", "100.50", "10.00", "10.00"),
        (10.75, "99.25", "100.25", "1.00", "1.00"),
        (14.0, "99.25", "100.50", "12.00", "8.00"),
    ]
    if zero_denominator:
        values = [values[0], (1.0, "100.00", "100.50", "0.00", "0.00")]
    return pa.table({
        "event_time": pa.array(
            [START + timedelta(seconds=item[0]) for item in values],
            type=pa.timestamp("us", tz="UTC"),
        ),
        "available_time": pa.array([None] * len(values), type=pa.timestamp("us", tz="UTC")),
        "instrument_id": pa.array(["T2603"] * len(values), type=pa.string()),
        "trade_date": pa.array(["2026-01-05"] * len(values), type=pa.string()),
        "session_id": pa.array([SESSION_ID] * len(values), type=pa.string()),
        "bid_price_1": pa.array([Decimal(item[1]) for item in values], type=pa.decimal128(9, 3)),
        "ask_price_1": pa.array([Decimal(item[2]) for item in values], type=pa.decimal128(9, 3)),
        "bid_size_1": pa.array([Decimal(item[3]) for item in values], type=pa.decimal128(19, 2)),
        "ask_size_1": pa.array([Decimal(item[4]) for item in values], type=pa.decimal128(19, 2)),
        "quality_flags": pa.array(
            [["END_OF_DAY_FIELDS_UNVERIFIED", "VOLUME_INCREMENT_UNVERIFIED"]
             for _ in values],
            type=pa.list_(pa.string()),
        ),
        "record_id": pa.array([f"record-{index}" for index in range(len(values))], pa.string()),
        "raw_file_sha256": pa.array(["a" * 64] * len(values), pa.string()),
        "raw_row_index": pa.array(list(range(len(values))), pa.int64()),
    })


class StubReader:
    def __init__(self, table: pa.Table, *, tick_size: Decimal | None = Decimal("0.25")):
        self.table = table
        self._tick_size = tick_size
        self.requested_session = None

    def read_quotes(self, instrument_id, trade_date, *, exchange="CFFEX",
                    session_id=None, columns=None, verify_hash=True):
        del instrument_id, trade_date, exchange, columns, verify_hash
        self.requested_session = session_id
        return self.table

    def tick_size(self, instrument_id, trade_date):
        del instrument_id, trade_date
        return self._tick_size

    def gap_events(self, **_filters):
        return GAP_EVENT_SCHEMA.empty_table()


def _feature(primitive_id: str) -> dict[str, Any]:
    return {
        "feature_id": f"feature_{primitive_id}",
        "version": 1,
        "source_requirements": {"primitive_id": primitive_id},
    }


@pytest.mark.parametrize(
    ("primitive_id", "expected"),
    [
        ("mid", [100.25, 100.125, 100.0, 99.875, 99.75, 100.0, 99.75, 99.875]),
        ("spread_ticks", [2.0, 1.0, 2.0, 1.0, 2.0, 4.0, 4.0, 5.0]),
        ("depth_bid_1", [6.0, 4.0, 5.0, 3.0, 2.0, 10.0, 1.0, 12.0]),
        ("depth_ask_1", [2.0, 8.0, 5.0, 7.0, 9.0, 10.0, 1.0, 8.0]),
        (
            "depth_imbalance_1",
            [0.5, -1 / 3, 0.0, -0.4, -7 / 11, 0.0, 0.0, 0.2],
        ),
        (
            "microprice_proxy",
            [100.375, 100.08333333333333, 100.0, 99.825, 99.5909090909091,
             100.0, 99.75, 100.0],
        ),
        (
            "quote_interarrival",
            [None, 1.5, 2.5, 1.25, 3.75, 1.0, 0.75, 3.25],
        ),
    ],
)
def test_eight_row_hand_calculation(primitive_id: str, expected: list[float | None]) -> None:
    reader = StubReader(_quote_table())
    output = compute_l2_primitive(
        reader,
        {"source_id": "fixture", "version": 1, "content_hash": "sha256:" + "1" * 64,
         "availability_mode": "research_only"},
        _feature(primitive_id),
        {
            "exchange": "CFFEX", "instrument_id": "T2603",
            "trade_date": "2026-01-05", "session_id": SESSION_ID,
        },
        READ_SCOPE,
    )
    actual = output.values["value"].to_pylist()
    assert len(actual) == len(expected) == 8
    for value, wanted in zip(actual, expected, strict=True):
        if wanted is None:
            assert value is None
        else:
            assert value == pytest.approx(wanted)
    assert output.audit.schema.equals(L2_PRIMITIVE_AUDIT_SCHEMA, check_metadata=False)
    assert output.audit.num_rows == 8
    assert output.source_evidence["gap_event_ids"] == []
    assert "VOLUME_INCREMENT_UNVERIFIED" in output.source_evidence["warning_flags"]


def test_zero_denominator_and_unknown_tick_have_explicit_masks() -> None:
    partition = {
        "exchange": "CFFEX", "instrument_id": "T2603",
        "trade_date": "2026-01-05", "session_id": SESSION_ID,
    }
    output = compute_l2_primitive(
        StubReader(_quote_table(zero_denominator=True)),
        {"source_id": "fixture", "version": 1, "content_hash": "sha256:" + "1" * 64,
         "availability_mode": "research_only"},
        _feature("depth_imbalance_1"),
        partition,
        READ_SCOPE,
    )
    assert output.values["value"].to_pylist()[1] is None
    assert output.audit["invalid_reason"].to_pylist()[1] == "ZERO_DENOMINATOR"

    spread = compute_l2_primitive(
        StubReader(_quote_table(), tick_size=None),
        {"source_id": "fixture", "version": 1, "content_hash": "sha256:" + "1" * 64,
         "availability_mode": "research_only"},
        _feature("spread_ticks"),
        partition,
        READ_SCOPE,
    )
    assert spread.values["value"].to_pylist() == [None] * 8
    assert spread.invalid_reason_counts == {"UNKNOWN_TICK_SIZE": 8}


@pytest.mark.parametrize("primitive_id", ["mid", "spread_ticks", "depth_imbalance_1",
                                          "quote_interarrival"])
def test_appending_future_rows_does_not_change_prefix(primitive_id: str) -> None:
    full = _quote_table()
    prefix = full.slice(0, 5)
    source = {"source_id": "fixture", "version": 1, "content_hash": "sha256:" + "1" * 64,
              "availability_mode": "research_only"}
    partition = {"exchange": "CFFEX", "instrument_id": "T2603",
                 "trade_date": "2026-01-05", "session_id": SESSION_ID}
    left = compute_l2_primitive(
        StubReader(prefix), source, _feature(primitive_id), partition, READ_SCOPE,
    )
    right = compute_l2_primitive(
        StubReader(full), source, _feature(primitive_id), partition, READ_SCOPE,
    )
    assert left.values["value"].to_pylist() == right.values["value"].to_pylist()[:5]


def test_metadata_reference_changes_source_identity() -> None:
    source = {
        "schema_version": "data_source_v1",
        "source_id": "source",
        "version": 1,
        "manifest_ref": {"uri": "file:///manifest.json", "content_hash": "sha256:" + "1" * 64},
        "content_hash": "sha256:" + "2" * 64,
        "schema_ref": {"uri": "file:///schema.json", "content_hash": "sha256:" + "3" * 64},
        "format": "l2_onboarding_source_v1",
        "instruments": ["T2603"],
        "coverage": {"partitioning": "trade_date_v1", "partitions": []},
        "time_rules": {"timezone": "Asia/Shanghai", "event_time_field": "event_time"},
        "calendar_ref": {"uri": "file:///calendar.json", "content_hash": "sha256:" + "4" * 64},
        "session_policy_ref": {
            "uri": "file:///session.json", "content_hash": "sha256:" + "5" * 64,
        },
        "availability_mode": "research_only",
        "access_policy_ref": {"uri": "file:///access.json", "content_hash": "sha256:" + "6" * 64},
    }
    baseline = _source_identity_hash(source)
    supplemented = deepcopy(source)
    supplemented["version"] = 2
    supplemented["market_metadata_ref"] = {
        "uri": "file:///metadata.json", "content_hash": "sha256:" + "7" * 64,
    }
    assert _source_identity_hash(supplemented) != baseline


def test_trade_only_source_blocks_lob_primitive_with_reason(tmp_path: Path) -> None:
    schema_path = tmp_path / "trade-only-schema.json"
    schema_path.write_text(json.dumps({
        "schema_version": "source_schema_v1",
        "fields": {
            "event_time": "timestamp[us, tz=UTC]",
            "instrument_id": "string",
            "trade_price": "decimal128(9, 3)",
            "trade_size": "decimal128(19, 2)",
        },
    }), encoding="utf-8")
    digest = "sha256:" + hashlib.sha256(schema_path.read_bytes()).hexdigest()
    source = {
        "format": "trade_only",
        "schema_ref": {"uri": schema_path.as_uri(), "content_hash": digest},
    }
    feature = {
        "source_requirements": {
            "source_format": "l2_onboarding_source_v1",
            "required_columns": ["bid_price_1", "ask_price_1"],
        },
    }
    assert _source_requirements_block(feature, source) == "SOURCE_FORMAT_UNSUPPORTED"
    source["format"] = "l2_onboarding_source_v1"
    assert _source_requirements_block(feature, source) == (
        "SOURCE_FIELD_MISSING:ask_price_1,bid_price_1"
    )


def _metadata() -> bytes:
    document = {
        "schema_version": "market_metadata_v1",
        "metadata_id": "l2_primitive_fixture_metadata",
        "version": 1,
        "provided_by": {"kind": "human", "id": "fixture-user"},
        "source": "Synthetic tick for the Step 03-A serialized test.",
        "units": {
            "price_unit": "quoted_price_unit",
            "size_unit": "contract",
            "turnover_unit": "quoted_currency",
        },
        "tick_rules": [
            {
                "product": product,
                "valid_from": "2026-01-01",
                "valid_to": None,
                "tick_size": "0.005",
                "unit": "quoted_price_unit",
            }
            for product in ("T", "TF")
        ],
    }
    return (json.dumps(document, sort_keys=True) + "\n").encode()


def _feature_set() -> dict[str, Any]:
    items = []
    for item_id in ("mid", "spread_ticks", "depth_imbalance_1", "quote_interarrival"):
        items.append({
            "item_id": item_id,
            "alias": item_id,
            "recipe": {
                "template_ref": f"l2_{item_id}@{CATALOG_VERSION}",
                "step": 3,
                "parameters": {},
            },
        })
    return {
        "schema_version": "feature_set_v1",
        "feature_set_id": "l2_primitives",
        "version": 1,
        "name": "Step 03-A primitives",
        "description": "Selected basic L2 primitives.",
        "items": items,
        "created_by": {"kind": "agent", "id": "step03a-test"},
        "change_reason": "Freeze the selected primitive dependency closure.",
    }


def _study(source_ref: str, feature_set_ref: str) -> dict[str, Any]:
    return {
        "schema_version": "study_v1",
        "study_id": "l2_primitives_study",
        "name": "L2 primitive execution",
        "objective": "Verify real Step 03-A source consumption.",
        "status": "active",
        "source_refs": [source_ref],
        "feature_set_refs": [feature_set_ref],
        "backlog": [],
        "limits": {
            "allowed_steps": [3],
            "allowed_scope": [{
                "instruments": ["T2603"],
                "start": "2026-01-05T09:10:00+08:00",
                "end": "2026-01-06T15:15:00+08:00",
            }],
            "budget": {"max_work_units": 100, "max_partitions": 100,
                       "max_concurrent_jobs": 2},
        },
        "created_by": {"kind": "human", "id": "fixture-user"},
        "change_reason": "Create a narrow descriptive primitive study.",
    }


def test_real_selected_primitives_publish_values_audit_and_query(tmp_path: Path) -> None:
    entry, entry_manifest, split_path = _fixture_entry(tmp_path)
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_bytes(_metadata())
    adapted = adapt_onboarding_entry(
        entry,
        entry_manifest,
        split_path,
        tmp_path / "adapted",
        source_id="fixture_l2",
        version=1,
        metadata_path=metadata_path,
    )
    registry = Registry(tmp_path / "research.duckdb")
    try:
        service = ResearchService(registry)
        source = service.register_source(adapted.source_document)
        register_l2_primitive_catalog(service)
        draft = service.save_feature_set(_feature_set())
        frozen = service.freeze_feature_set(
            "l2_primitives@1", expected_revision=draft["draft_revision"],
        )
        service.create_study(_study("fixture_l2@1", "l2_primitives@1"))
        access_policy = json.loads(
            artifact_path(source["access_policy_ref"]["uri"]).read_text(encoding="utf-8")
        )
        request = {
            "schema_version": "run_request_v1",
            "study_id": "l2_primitives_study",
            "study_revision": 1,
            "feature_set_ref": "l2_primitives@1",
            "source_refs": ["fixture_l2@1"],
            "split_ref": access_policy["split_refs"][0],
            "scope": {
                "instruments": ["T2603"],
                "start": "2026-01-05T01:29:00+00:00",
                "end": "2026-01-05T07:15:00+00:00",
                "session_filter": [
                    "CN_STOCK:2026-01-05:opening_auction_matching",
                    "CN_STOCK:2026-01-05:continuous_morning",
                    "CN_STOCK:2026-01-05:continuous_afternoon",
                ],
            },
            "allowed_steps": [3],
            "missing_dependencies": "block",
            "reuse_policy": "verified_only",
            "selected_item_ids": [item["item_id"] for item in frozen["resolved_items"]],
            "budget": {
                "max_work_units": 100,
                "max_partitions": 100,
                "max_concurrent_jobs": 2,
            },
            "created_by": {"kind": "agent", "id": "step03a-test"},
        }
        plan = service.plan_run(request)
        assert plan["status"] == "READY"
        assert len(plan["work"]) == 12
        run = service.submit_run(
            request, request_key="step03a-real-primitives",
            expected_plan_hash=plan["plan_hash"],
        )
        executed = service.execute_run(run["run_id"])
        assert executed["status"] == "COMPLETE"
        results = service.list_results({"run_id": run["run_id"]}, verify=True)
        assert len(results["items"]) == 12
        assert all(len(result["artifacts"]) == 2 for result in results["items"])
        value_roles = {
            descriptor["artifact_role"]
            for result in results["items"] for descriptor in result["artifacts"]
        }
        assert value_roles == {"feature_values", "primitive_audit"}

        mid_ref = next(
            item for item in frozen["resolved_items"] if item["item_id"] == "mid"
        )["outputs"][0]["feature_ref"]
        mid_results = [
            result for result in results["items"] if result["feature_ref"] == mid_ref
        ]
        assert mid_results
        read = service.read_results([mid_results[0]["result_id"]], columns=["value"], limit=100)
        assert read["rows"]
        audit_descriptor = next(
            descriptor for descriptor in mid_results[0]["artifacts"]
            if descriptor["artifact_role"] == "primitive_audit"
        )
        audit_path = artifact_path(audit_descriptor["artifact_ref"]["uri"])
        audit = pq.read_table(audit_path)
        assert audit.schema.equals(L2_PRIMITIVE_AUDIT_SCHEMA, check_metadata=False)
        assert set(audit["record_id"].to_pylist())
        assert mid_results[0]["time_metadata"]["availability_mode"] == "research_only"
        assert mid_results[0]["time_metadata"]["source_evidence"]["tick_size"] == "0.005"
        assert any(
            result["time_metadata"]["source_evidence"]["gap_event_ids"]
            for result in mid_results
        )
    finally:
        registry.close()
