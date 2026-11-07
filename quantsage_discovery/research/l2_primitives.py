"""Trusted Step 03-A snapshot primitives over frozen onboarding DataSources."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from quantsage_discovery.core.errors import ArtifactValidationError

from .onboarding_adapter import OnboardingDataSourceReader
from .providers import FEATURE_VALUES_SCHEMA, make_feature_values_table

PROVIDER_ID = "quantsage_l2_snapshot"
PROVIDER_KEY = "builtin.l2_primitives"
CATALOG_VERSION = "1.0.2"
IMPLEMENTATION_VERSION = "l2_snapshot_primitives_v3"
FORMULA_REF = "gov_bond_l2_snapshot_primitives@1"
L2_PRIMITIVE_AUDIT_TYPE = "l2_primitive_audit"
L2_PRIMITIVE_AUDIT_SCHEMA_VERSION = "l2_primitive_audit_v1"
L2_PRIMITIVE_AUDIT_SCHEMA = pa.schema([
    pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("instrument_id", pa.string(), nullable=False),
    pa.field("trade_date", pa.string(), nullable=False),
    pa.field("session_id", pa.string(), nullable=False),
    pa.field("record_id", pa.string()),
    pa.field("raw_file_sha256", pa.string()),
    pa.field("raw_row_index", pa.int64()),
    pa.field("is_valid", pa.bool_(), nullable=False),
    pa.field("invalid_reason", pa.string()),
    pa.field("dependency_mask", pa.list_(pa.string()), nullable=False),
    pa.field("source_quality_flags", pa.list_(pa.string()), nullable=False),
])

_PRIMITIVES = {
    "mid": {
        "name": "Mid price",
        "description": "Arithmetic midpoint of the valid best bid and ask.",
        "family": "price_dynamics",
        "unit": "quoted_price_unit",
        "columns": ["bid_price_1", "ask_price_1", "bid_size_1", "ask_size_1"],
        "tick_size": False,
        "dependency_mask": ["bid_price_1", "ask_price_1", "valid_l1"],
    },
    "spread_ticks": {
        "name": "Tick-normalised spread",
        "description": "Best ask minus best bid divided by the exact historical tick size.",
        "family": "price_dynamics",
        "unit": "ticks",
        "columns": ["bid_price_1", "ask_price_1", "bid_size_1", "ask_size_1"],
        "tick_size": True,
        "dependency_mask": ["bid_price_1", "ask_price_1", "tick_size", "valid_l1"],
    },
    "depth_bid_1": {
        "name": "Best bid depth",
        "description": "Visible size at the best bid level.",
        "family": "liquidity",
        "unit": "source_size_unit",
        "columns": ["bid_price_1", "ask_price_1", "bid_size_1", "ask_size_1"],
        "tick_size": False,
        "dependency_mask": ["bid_size_1", "valid_l1"],
    },
    "depth_ask_1": {
        "name": "Best ask depth",
        "description": "Visible size at the best ask level.",
        "family": "liquidity",
        "unit": "source_size_unit",
        "columns": ["bid_price_1", "ask_price_1", "bid_size_1", "ask_size_1"],
        "tick_size": False,
        "dependency_mask": ["ask_size_1", "valid_l1"],
    },
    "depth_imbalance_1": {
        "name": "Level-1 depth imbalance",
        "description": "(best-bid size - best-ask size) / their sum.",
        "family": "liquidity",
        "unit": "dimensionless",
        "columns": ["bid_price_1", "ask_price_1", "bid_size_1", "ask_size_1"],
        "tick_size": False,
        "dependency_mask": ["bid_size_1", "ask_size_1", "valid_l1"],
    },
    "microprice_proxy": {
        "name": "Microprice proxy",
        "description": "Opposite-size-weighted touch price; not a realised trade price.",
        "family": "price_dynamics",
        "unit": "quoted_price_unit",
        "columns": ["bid_price_1", "ask_price_1", "bid_size_1", "ask_size_1"],
        "tick_size": False,
        "dependency_mask": ["bid_price_1", "ask_price_1", "bid_size_1", "ask_size_1"],
    },
    "quote_interarrival": {
        "name": "Quote interarrival",
        "description": "Seconds from the previous valid quote observation in the same session.",
        "family": "temporal_structure",
        "unit": "seconds",
        "columns": [],
        "tick_size": False,
        "dependency_mask": ["event_time", "session_id", "stable_source_order"],
    },
}


@dataclass(frozen=True)
class L2PrimitiveOutput:
    """One primary feature-value table plus its row-level source audit."""

    values: pa.Table
    audit: pa.Table
    invalid_reason_counts: dict[str, int]
    provider_reasons: dict[str, int]
    quality_flags: list[str]
    source_evidence: dict[str, Any]


def primitive_template_documents() -> list[dict[str, Any]]:
    """Return immutable Step 03-A catalog documents for selected primitive recipes."""
    templates = []
    for primitive_id, spec in _PRIMITIVES.items():
        templates.append({
            "schema_version": "feature_template_v1",
            "template_id": f"l2_{primitive_id}",
            "version": CATALOG_VERSION,
            "name": spec["name"],
            "description": spec["description"],
            "formula_ref": FORMULA_REF,
            "provider_ref": f"{PROVIDER_ID}@{CATALOG_VERSION}",
            "parameters_schema": {
                "type": "object",
                "properties": {},
                "required": [],
                "additionalProperties": False,
            },
            "defaults": {},
            "output_schema": {"value": "float64"},
            "unit": spec["unit"],
            "status": "ready",
            "step": 3,
            "kind": "primitive",
            "family": spec["family"],
            "time_policy": {},
            "source_requirements": {
                "source_format": "l2_onboarding_source_v1",
                "primitive_id": primitive_id,
                "required_columns": spec["columns"],
                "tick_size": spec["tick_size"],
            },
            "validity": {
                "missing_policy": "na_with_reason",
                "zero_denominator_policy": "na_with_reason",
                "stale_policy": "not_applicable",
                "reason_codes": [
                    "INVALID_L1_BOOK",
                    "UNKNOWN_TICK_SIZE",
                    "ZERO_DENOMINATOR",
                    "SESSION_START_NO_PREVIOUS",
                    "SOURCE_ORDER_INVALID",
                ],
            },
        })
    return templates


def provider_document() -> dict[str, Any]:
    return {
        "schema_version": "provider_v1",
        "provider_id": PROVIDER_ID,
        "version": CATALOG_VERSION,
        "provider_key": PROVIDER_KEY,
        "implementation_version": IMPLEMENTATION_VERSION,
        "supported_steps": [3],
    }


def register_l2_primitive_catalog(service: Any) -> dict[str, Any]:
    """Register provider v1 and every Step 03-A primitive template exactly once."""
    provider = service.register_provider(provider_document())
    templates = [service.register_template(document)
                 for document in primitive_template_documents()]
    return {
        "provider": provider,
        "templates": templates,
        "template_refs": {
            document["template_id"]: f"{document['template_id']}@{document['version']}"
            for document in templates
        },
    }


def validate_l2_primitive_audit_parquet(path: Any) -> None:
    try:
        parquet = pq.ParquetFile(path)
    except Exception as exc:
        raise ArtifactValidationError(f"cannot read L2 primitive audit Parquet: {exc}") from exc
    if not parquet.schema_arrow.equals(L2_PRIMITIVE_AUDIT_SCHEMA, check_metadata=False):
        raise ArtifactValidationError(
            "L2 primitive audit schema does not match l2_primitive_audit_v1"
        )
    for batch in parquet.iter_batches(batch_size=65_536):
        for name in (
            "event_time", "instrument_id", "trade_date", "session_id", "is_valid",
            "dependency_mask", "source_quality_flags",
        ):
            if batch.column(name).null_count:
                raise ArtifactValidationError(f"L2 primitive audit {name} cannot be null")


def compute_l2_primitive(
    reader: OnboardingDataSourceReader,
    source: dict[str, Any],
    feature: dict[str, Any],
    partition: dict[str, Any],
    read_scope: dict[str, Any],
) -> L2PrimitiveOutput:
    """Compute one selected Step 03-A primitive for one exact session partition."""
    primitive_id = feature.get("source_requirements", {}).get("primitive_id")
    spec = _PRIMITIVES.get(primitive_id)
    if spec is None:
        raise ArtifactValidationError(f"unsupported L2 primitive: {primitive_id!r}")
    required = [
        "event_time", "available_time", "instrument_id", "trade_date", "session_id",
        "record_id", "raw_file_sha256", "raw_row_index", "quality_flags",
        *spec["columns"],
    ]
    quotes = reader.read_quotes(
        partition["instrument_id"],
        partition["trade_date"],
        exchange=partition.get("exchange", "CFFEX"),
        session_id=partition["session_id"],
        columns=list(dict.fromkeys(required)),
        verify_hash=True,
    )
    event_type = quotes.schema.field("event_time").type
    quotes = quotes.filter(pc.and_(
        pc.greater_equal(
            quotes["event_time"], pa.scalar(_as_utc(read_scope["start"]), event_type),
        ),
        pc.less(
            quotes["event_time"], pa.scalar(_as_utc(read_scope["end"]), event_type),
        ),
    ))
    tick_size = reader.tick_size(partition["instrument_id"], partition["trade_date"])
    gap_events = reader.gap_events(
        instrument_id=partition["instrument_id"],
        trade_date=partition["trade_date"],
        session_id=partition["session_id"],
    )
    warnings = sorted({
        flag for flags in quotes["quality_flags"].to_pylist() for flag in (flags or [])
    })
    rows = quotes.to_pylist()
    values = []
    audit_rows = []
    invalid_reasons: Counter[str] = Counter()
    provider_reasons: Counter[str] = Counter()
    previous_valid_time: datetime | None = None
    previous_event_time: datetime | None = None
    for index, row in enumerate(rows):
        invalid_reason = _invalid_reason(primitive_id, row, tick_size)
        if (
            invalid_reason is None
            and primitive_id == "quote_interarrival"
            and previous_event_time is not None
            and row["event_time"] < previous_event_time
        ):
            invalid_reason = "SOURCE_ORDER_INVALID"
        value = None
        if invalid_reason is None:
            if primitive_id == "mid":
                value = float((row["bid_price_1"] + row["ask_price_1"]) / Decimal(2))
            elif primitive_id == "spread_ticks":
                value = float((row["ask_price_1"] - row["bid_price_1"]) / tick_size)
            elif primitive_id == "depth_bid_1":
                value = float(row["bid_size_1"])
            elif primitive_id == "depth_ask_1":
                value = float(row["ask_size_1"])
            elif primitive_id == "depth_imbalance_1":
                bid_size, ask_size = row["bid_size_1"], row["ask_size_1"]
                try:
                    value = float((bid_size - ask_size) / (bid_size + ask_size))
                except (ZeroDivisionError, InvalidOperation):
                    invalid_reason = "ZERO_DENOMINATOR"
            elif primitive_id == "microprice_proxy":
                bid, ask = row["bid_price_1"], row["ask_price_1"]
                bid_size, ask_size = row["bid_size_1"], row["ask_size_1"]
                denominator = bid_size + ask_size
                if denominator == 0:
                    invalid_reason = "ZERO_DENOMINATOR"
                else:
                    value = float((ask * bid_size + bid * ask_size) / denominator)
            elif primitive_id == "quote_interarrival":
                if previous_valid_time is None:
                    invalid_reason = "SESSION_START_NO_PREVIOUS"
                    previous_valid_time = row["event_time"]
                else:
                    value = (row["event_time"] - previous_valid_time).total_seconds()
                    previous_valid_time = row["event_time"]
        if invalid_reason is not None:
            invalid_reasons[invalid_reason] += 1
        previous_event_time = row["event_time"] if previous_event_time is None else max(
            previous_event_time, row["event_time"]
        )
        available = _required_available_time(row, primitive_id)
        trade_date_text = (
            row["trade_date"].isoformat()
            if hasattr(row["trade_date"], "isoformat") else str(row["trade_date"])
        )
        values.append({
            "event_time": row["event_time"],
            "available_time": available,
            "instrument_id": row["instrument_id"],
            "trade_date": trade_date_text,
            "session_id": row["session_id"],
            "value": value,
        })
        audit_rows.append({
            "event_time": row["event_time"],
            "instrument_id": row["instrument_id"],
            "trade_date": trade_date_text,
            "session_id": row["session_id"],
            "record_id": row.get("record_id"),
            "raw_file_sha256": row.get("raw_file_sha256"),
            "raw_row_index": row.get("raw_row_index"),
            "is_valid": invalid_reason is None,
            "invalid_reason": invalid_reason,
            "dependency_mask": list(spec["dependency_mask"]),
            "source_quality_flags": list(row.get("quality_flags") or []),
        })
    if values:
        value_table = make_feature_values_table(values)
        audit_table = pa.Table.from_pylist(audit_rows, schema=L2_PRIMITIVE_AUDIT_SCHEMA)
    else:
        value_table = FEATURE_VALUES_SCHEMA.empty_table()
        audit_table = L2_PRIMITIVE_AUDIT_SCHEMA.empty_table()
    quality_flags = []
    if not values:
        quality_flags.append("ZERO_OBSERVATIONS")
    elif not any(row["value"] is not None for row in values):
        quality_flags.append("ALL_NA")
    if warnings:
        quality_flags.append("SOURCE_WARNINGS_PRESENT")
    if invalid_reasons:
        provider_reasons.update(invalid_reasons)
    source_evidence = {
        "source_ref": f"{source['source_id']}@{source['version']}",
        "source_content_hash": source["content_hash"],
        "gap_manifest_ref": source.get("gap_manifest_ref"),
        "row_count": quotes.num_rows,
        "gap_event_ids": sorted(event["event_id"] for event in gap_events.to_pylist()),
        "availability_mode": source["availability_mode"],
        "warning_flags": warnings,
        "tick_size": None if tick_size is None else format(tick_size, "f"),
        "primitive_id": primitive_id,
    }
    return L2PrimitiveOutput(
        values=value_table,
        audit=audit_table,
        invalid_reason_counts=dict(sorted(invalid_reasons.items())),
        provider_reasons=dict(sorted(provider_reasons.items())),
        quality_flags=quality_flags,
        source_evidence=source_evidence,
    )


def _invalid_reason(primitive_id: str, row: dict[str, Any], tick_size: Decimal | None) -> str | None:
    required = {"event_time", "instrument_id", "trade_date", "session_id"}
    if primitive_id != "quote_interarrival":
        required |= {"bid_price_1", "ask_price_1", "bid_size_1", "ask_size_1"}
    if any(row.get(name) is None for name in required):
        return "MISSING_REQUIRED_FIELD"
    if primitive_id != "quote_interarrival":
        if row["bid_price_1"] <= 0 or row["ask_price_1"] <= 0:
            return "INVALID_L1_BOOK"
        if row["bid_price_1"] > row["ask_price_1"]:
            return "INVALID_L1_BOOK"
        if row["bid_size_1"] < 0 or row["ask_size_1"] < 0:
            return "INVALID_L1_BOOK"
    if primitive_id == "spread_ticks" and tick_size is None:
        return "UNKNOWN_TICK_SIZE"
    return None


def _required_available_time(row: dict[str, Any], primitive_id: str) -> datetime | None:
    del primitive_id
    return row.get("available_time")


def _as_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("primitive read scope must be timezone-aware")
    return parsed.astimezone(UTC)
