"""Trusted Step 03-A snapshot primitives over frozen onboarding DataSources."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from quantsage_discovery.core.errors import ArtifactValidationError

from .onboarding_adapter import OnboardingDataSourceReader
from .providers import FEATURE_VALUES_SCHEMA, make_feature_values_table

PROVIDER_ID = "quantsage_l2_snapshot"
PROVIDER_KEY = "builtin.l2_primitives"
CATALOG_VERSION = "2.0.0"
IMPLEMENTATION_VERSION = "l2_snapshot_primitives_v4"
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
    "obi_snapshot": {
        "name": "Snapshot order-book imbalance",
        "description": "Tick-distance space depth imbalance in one L2 snapshot.",
        "family": "F02",
        "unit": "dimensionless",
        "columns": [f"{side}_{field}_{level}" for side in ("bid", "ask")
                    for field in ("price", "size") for level in range(1, 6)],
        "tick_size": False,
        "dependency_mask": ["space_depth_bid", "space_depth_ask", "valid_l1"],
    },
    "wobi_snapshot": {
        "name": "Snapshot weighted order-book imbalance",
        "description": "Distance-weighted tick-space depth imbalance in one snapshot.",
        "family": "F03",
        "unit": "dimensionless",
        "columns": [f"{side}_{field}_{level}" for side in ("bid", "ask")
                    for field in ("price", "size") for level in range(1, 6)],
        "tick_size": False,
        "dependency_mask": ["weighted_depth_bid", "weighted_depth_ask", "valid_l1"],
    },
    "min_coverage_snapshot": {
        "name": "Snapshot minimum size coverage",
        "description": "Weaker-side coverage of a per-snapshot relative target size.",
        "family": "F04",
        "unit": "dimensionless",
        "columns": [f"{side}_{field}_{level}" for side in ("bid", "ask")
                    for field in ("price", "size") for level in range(1, 6)],
        "tick_size": False,
        "dependency_mask": ["space_depth", "depth_ref", "target_size"],
    },
    "capacity_imb_snapshot": {
        "name": "Snapshot size-conditioned capacity imbalance",
        "description": "Imbalance after capping each side's space depth at target size.",
        "family": "F05",
        "unit": "dimensionless",
        "columns": [f"{side}_{field}_{level}" for side in ("bid", "ask")
                    for field in ("price", "size") for level in range(1, 6)],
        "tick_size": False,
        "dependency_mask": ["space_depth", "depth_ref", "target_size"],
    },
    "size_spread_snapshot": {
        "name": "Snapshot size-conditioned spread",
        "description": "Tick spread between buy-walk and sell-walk VWAPs.",
        "family": "F06",
        "unit": "ticks",
        "columns": [f"{side}_{field}_{level}" for side in ("bid", "ask")
                    for field in ("price", "size") for level in range(1, 6)],
        "tick_size": True,
        "dependency_mask": ["full_book", "target_size", "tick_size", "vwap"],
    },
    "fill_dist_asym_snapshot": {
        "name": "Snapshot fill-distance asymmetry",
        "description": "Asymmetry of tick distances to fully fill target size.",
        "family": "F07",
        "unit": "dimensionless",
        "columns": [f"{side}_{field}_{level}" for side in ("bid", "ask")
                    for field in ("price", "size") for level in range(1, 6)],
        "tick_size": True,
        "dependency_mask": ["full_book", "target_size", "tick_size", "fill_distance"],
    },
    "walk_cost_asym_snapshot": {
        "name": "Snapshot walk-cost asymmetry",
        "description": "Asymmetry between buy and sell VWAP execution costs from mid.",
        "family": "F08",
        "unit": "dimensionless",
        "columns": [f"{side}_{field}_{level}" for side in ("bid", "ask")
                    for field in ("price", "size") for level in range(1, 6)],
        "tick_size": True,
        "dependency_mask": ["full_book", "target_size", "tick_size", "vwap", "mid"],
    },
    "visible_depth_bid_5": {
        "name": "Visible best-five bid depth",
        "description": "Sum of visible sizes over all five valid bid levels.",
        "family": "input",
        "unit": "source_size_unit",
        "columns": [f"bid_price_{level}" for level in range(1, 6)]
                   + [f"bid_size_{level}" for level in range(1, 6)],
        "tick_size": False,
        "dependency_mask": ["full_bid_book"],
    },
    "visible_depth_ask_5": {
        "name": "Visible best-five ask depth",
        "description": "Sum of visible sizes over all five valid ask levels.",
        "family": "input",
        "unit": "source_size_unit",
        "columns": [f"ask_price_{level}" for level in range(1, 6)]
                   + [f"ask_size_{level}" for level in range(1, 6)],
        "tick_size": False,
        "dependency_mask": ["full_ask_book"],
    },
    "depth_ref_5": {
        "name": "Best-five reference depth",
        "description": "Average of complete visible bid and ask five-level depth.",
        "family": "input",
        "unit": "source_size_unit",
        "columns": [f"{side}_{field}_{level}" for side in ("bid", "ask")
                    for field in ("price", "size") for level in range(1, 6)],
        "tick_size": False,
        "dependency_mask": ["full_book"],
    },
    "target_size_q25": {
        "name": "Relative target size q25",
        "description": "Twenty-five percent of current best-five reference depth.",
        "family": "input",
        "unit": "source_size_unit",
        "columns": [f"{side}_{field}_{level}" for side in ("bid", "ask")
                    for field in ("price", "size") for level in range(1, 6)],
        "tick_size": False,
        "dependency_mask": ["depth_ref"],
    },
    "target_size_q50": {
        "name": "Relative target size q50",
        "description": "Fifty percent of current best-five reference depth.",
        "family": "input",
        "unit": "source_size_unit",
        "columns": [f"{side}_{field}_{level}" for side in ("bid", "ask")
                    for field in ("price", "size") for level in range(1, 6)],
        "tick_size": False,
        "dependency_mask": ["depth_ref"],
    },
    "target_size_q100": {
        "name": "Relative target size q100",
        "description": "One hundred percent of current best-five reference depth.",
        "family": "input",
        "unit": "source_size_unit",
        "columns": [f"{side}_{field}_{level}" for side in ("bid", "ask")
                    for field in ("price", "size") for level in range(1, 6)],
        "tick_size": False,
        "dependency_mask": ["depth_ref"],
    },
    "active_buy_cumulative": {
        "name": "Cumulative active-buy proxy",
        "description": "Native BUYVOL cumulative field passed through unchanged.",
        "family": "input",
        "unit": "source_size_unit",
        "columns": ["BUYVOL"],
        "tick_size": False,
        "dependency_mask": ["BUYVOL"],
    },
    "active_sell_cumulative": {
        "name": "Cumulative active-sell proxy",
        "description": "Native SELLVOL cumulative field passed through unchanged.",
        "family": "input",
        "unit": "source_size_unit",
        "columns": ["SELLVOL"],
        "tick_size": False,
        "dependency_mask": ["SELLVOL"],
    },
    "buy_cumulative_reset": {
        "name": "Cumulative buy reset marker",
        "description": "One when BUYVOL decreases from the prior observation in the same session.",
        "family": "input",
        "unit": "boolean",
        "columns": ["BUYVOL"],
        "tick_size": False,
        "dependency_mask": ["BUYVOL", "session_order"],
    },
    "sell_cumulative_reset": {
        "name": "Cumulative sell reset marker",
        "description": "One when SELLVOL decreases from the prior observation in the same session.",
        "family": "input",
        "unit": "boolean",
        "columns": ["SELLVOL"],
        "tick_size": False,
        "dependency_mask": ["SELLVOL", "session_order"],
    },
    "trade_volume_interval": {
        "name": "Native interval trade volume",
        "description": "Native TRADEVOLUME passed through without differencing.",
        "family": "input",
        "unit": "source_size_unit",
        "columns": ["volume_increment"],
        "tick_size": False,
        "dependency_mask": ["volume_increment"],
    },
    "tick_size": {
        "name": "Historical contract tick size",
        "description": "Exact market metadata tick selected by contract and trade date.",
        "family": "static",
        "unit": "quoted_price_unit",
        "columns": [],
        "tick_size": True,
        "dependency_mask": ["market_metadata_ref"],
    },
}
_BOOK_COLUMNS = [
    f"{side}_{field}_{level}" for side in ("bid", "ask")
    for field in ("price", "size") for level in range(1, 6)
]
_NON_BOOK_PRIMITIVES = {
    "quote_interarrival", "tick_size", "active_buy_cumulative",
    "active_sell_cumulative", "buy_cumulative_reset", "sell_cumulative_reset",
    "trade_volume_interval",
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
        properties: dict[str, Any] = {}
        required: list[str] = []
        if primitive_id in {"obi_snapshot", "wobi_snapshot", "min_coverage_snapshot",
                            "capacity_imb_snapshot"}:
            properties["space"] = {"type": "string", "enum": ["d0", "d1", "d2", "d3", "d5"]}
            required.append("space")
        if primitive_id in {"min_coverage_snapshot", "capacity_imb_snapshot",
                            "size_spread_snapshot", "fill_dist_asym_snapshot",
                            "walk_cost_asym_snapshot"}:
            properties["size"] = {"type": "string", "enum": ["q25", "q50", "q100"]}
            required.append("size")
        source_requirements = {
            "source_format": "l2_onboarding_source_v1",
            "primitive_id": primitive_id,
            "required_columns": spec["columns"],
            "tick_size": spec["tick_size"],
        }
        if primitive_id in {"obi_snapshot", "wobi_snapshot", "min_coverage_snapshot",
                            "capacity_imb_snapshot"}:
            source_requirements["tick_size_for_nonzero_space"] = True
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
                "properties": properties,
                "required": required,
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
            "source_requirements": source_requirements,
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


def snapshot_catalog_entries() -> list[dict[str, Any]]:
    """Enumerate the 50 Step 03-B F01-F08 public snapshot descriptions."""
    entries = [{
        "factor_name": "spread_ticks",
        "family": "F01",
        "template_ref": f"l2_spread_ticks@{CATALOG_VERSION}",
        "parameters": {},
    }]
    spaces = ("d0", "d1", "d2", "d3", "d5")
    sizes = ("q25", "q50", "q100")
    for space in spaces:
        entries.append({
            "factor_name": f"obi_snapshot__{space}",
            "family": "F02",
            "template_ref": f"l2_obi_snapshot@{CATALOG_VERSION}",
            "parameters": {"space": space},
        })
    for space in spaces:
        entries.append({
            "factor_name": f"wobi_snapshot__{space}",
            "family": "F03",
            "template_ref": f"l2_wobi_snapshot@{CATALOG_VERSION}",
            "parameters": {"space": space},
        })
    for space in spaces:
        for size in sizes:
            entries.append({
                "factor_name": f"min_coverage_snapshot__{space}__{size}",
                "family": "F04",
                "template_ref": f"l2_min_coverage_snapshot@{CATALOG_VERSION}",
                "parameters": {"space": space, "size": size},
            })
    for space in spaces:
        for size in sizes:
            entries.append({
                "factor_name": f"capacity_imb_snapshot__{space}__{size}",
                "family": "F05",
                "template_ref": f"l2_capacity_imb_snapshot@{CATALOG_VERSION}",
                "parameters": {"space": space, "size": size},
            })
    for family, template_id, prefix in (
        ("F06", "l2_size_spread_snapshot", "size_spread_snapshot"),
        ("F07", "l2_fill_dist_asym_snapshot", "fill_dist_asym_snapshot"),
        ("F08", "l2_walk_cost_asym_snapshot", "walk_cost_asym_snapshot"),
    ):
        for size in sizes:
            entries.append({
                "factor_name": f"{prefix}__{size}",
                "family": family,
                "template_ref": f"{template_id}@{CATALOG_VERSION}",
                "parameters": {"size": size},
            })
    return entries


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
    if primitive_id not in _NON_BOOK_PRIMITIVES:
        required.extend(_BOOK_COLUMNS)
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
    previous_buy: Decimal | None = None
    previous_sell: Decimal | None = None
    for index, row in enumerate(rows):
        value, invalid_reason = _compute_value(
            primitive_id,
            feature.get("parameters", {}),
            row,
            tick_size,
            previous_valid_time=previous_valid_time,
            previous_event_time=previous_event_time,
            previous_buy=previous_buy,
            previous_sell=previous_sell,
        )
        if primitive_id == "quote_interarrival" and invalid_reason != "MISSING_REQUIRED_FIELD":
            previous_valid_time = (
                row["event_time"] if previous_valid_time is None
                else max(previous_valid_time, row["event_time"])
            )
        if (
            primitive_id in {"buy_cumulative_reset", "active_buy_cumulative"}
            and "BUYVOL" in row and row["BUYVOL"] is not None
        ):
            previous_buy = row["BUYVOL"]
        if (
            primitive_id in {"sell_cumulative_reset", "active_sell_cumulative"}
            and "SELLVOL" in row and row["SELLVOL"] is not None
        ):
            previous_sell = row["SELLVOL"]
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


def _compute_value(
    primitive_id: str,
    parameters: dict[str, Any],
    row: dict[str, Any],
    tick_size: Decimal | None,
    *,
    previous_valid_time: datetime | None,
    previous_event_time: datetime | None,
    previous_buy: Decimal | None,
    previous_sell: Decimal | None,
) -> tuple[float | None, str | None]:
    identity = {"event_time", "instrument_id", "trade_date", "session_id"}
    if any(row.get(name) is None for name in identity):
        return None, "MISSING_REQUIRED_FIELD"
    if primitive_id == "quote_interarrival":
        if previous_event_time is not None and row["event_time"] < previous_event_time:
            return None, "SOURCE_ORDER_INVALID"
        if previous_valid_time is None:
            return None, "SESSION_START_NO_PREVIOUS"
        return (row["event_time"] - previous_valid_time).total_seconds(), None
    if primitive_id == "tick_size":
        return (None, "UNKNOWN_TICK_SIZE") if tick_size is None else (float(tick_size), None)
    if primitive_id in {"active_buy_cumulative", "active_sell_cumulative",
                        "buy_cumulative_reset", "sell_cumulative_reset",
                        "trade_volume_interval"}:
        return _flow_value(
            primitive_id, row, previous_buy=previous_buy, previous_sell=previous_sell,
        )

    book = _book(row)
    if book is None:
        return None, "MISSING_REQUIRED_FIELD"
    if primitive_id in {"mid", "spread_ticks", "depth_bid_1", "depth_ask_1",
                        "depth_imbalance_1", "microprice_proxy"}:
        if not book.l1_valid:
            return None, "INVALID_L1_BOOK"
        if primitive_id == "mid":
            return float((book.bid[0][0] + book.ask[0][0]) / Decimal(2)), None
        if primitive_id == "spread_ticks":
            if tick_size is None:
                return None, "UNKNOWN_TICK_SIZE"
            return float((book.ask[0][0] - book.bid[0][0]) / tick_size), None
        if primitive_id == "depth_bid_1":
            return float(book.bid[0][1]), None
        if primitive_id == "depth_ask_1":
            return float(book.ask[0][1]), None
        if primitive_id == "depth_imbalance_1":
            return _ratio(
                book.bid[0][1] - book.ask[0][1],
                book.bid[0][1] + book.ask[0][1],
            )
        bid_size, ask_size = book.bid[0][1], book.ask[0][1]
        denominator = bid_size + ask_size
        if denominator == 0:
            return None, "ZERO_DENOMINATOR"
        return float((book.ask[0][0] * bid_size + book.bid[0][0] * ask_size)
                     / denominator), None

    if primitive_id in {"visible_depth_bid_5", "visible_depth_ask_5", "depth_ref_5",
                        "target_size_q25", "target_size_q50", "target_size_q100"}:
        if not book.full_valid:
            return None, "INVALID_DEPTH_BOOK"
        bid_depth = sum((level[1] for level in book.bid), Decimal(0))
        ask_depth = sum((level[1] for level in book.ask), Decimal(0))
        if primitive_id == "visible_depth_bid_5":
            return float(bid_depth), None
        if primitive_id == "visible_depth_ask_5":
            return float(ask_depth), None
        depth_ref = (bid_depth + ask_depth) / Decimal(2)
        if primitive_id == "depth_ref_5":
            return float(depth_ref), None
        alpha = {"target_size_q25": Decimal("0.25"),
                 "target_size_q50": Decimal("0.50"),
                 "target_size_q100": Decimal("1.00")}[primitive_id]
        target = alpha * depth_ref
        return (None, "ZERO_DENOMINATOR") if target <= 0 else (float(target), None)

    space = parameters.get("space")
    if primitive_id in {"obi_snapshot", "wobi_snapshot", "min_coverage_snapshot",
                        "capacity_imb_snapshot"}:
        distance = _space_distance(space)
        if distance is None:
            return None, "INVALID_PARAMETER"
        if distance > 0 and tick_size is None:
            return None, "UNKNOWN_TICK_SIZE"
        if distance == 0:
            if not book.l1_valid:
                return None, "INVALID_L1_BOOK"
            bid_levels = book.bid[:1]
            ask_levels = book.ask[:1]
            deltas_bid = [Decimal(0)]
            deltas_ask = [Decimal(0)]
        else:
            if not book.full_valid or tick_size is None:
                return None, "INVALID_DEPTH_BOOK"
            deltas_bid = [(book.bid[0][0] - price) / tick_size for price, _ in book.bid]
            deltas_ask = [(price - book.ask[0][0]) / tick_size for price, _ in book.ask]
            bid_levels = book.bid
            ask_levels = book.ask
        depth_bid = sum((size for (_, size), delta in zip(bid_levels, deltas_bid, strict=True)
                         if delta <= distance), Decimal(0))
        depth_ask = sum((size for (_, size), delta in zip(ask_levels, deltas_ask, strict=True)
                         if delta <= distance), Decimal(0))
        if primitive_id == "obi_snapshot":
            return _ratio(depth_bid - depth_ask, depth_bid + depth_ask)
        if primitive_id == "wobi_snapshot":
            weighted_bid = sum((size / (Decimal(1) + delta)
                                for (_, size), delta in zip(bid_levels, deltas_bid, strict=True)
                                if delta <= distance), Decimal(0))
            weighted_ask = sum((size / (Decimal(1) + delta)
                                for (_, size), delta in zip(ask_levels, deltas_ask, strict=True)
                                if delta <= distance), Decimal(0))
            return _ratio(weighted_bid - weighted_ask, weighted_bid + weighted_ask)
        target = _target_size(book, parameters.get("size"))
        if target is None:
            return None, "INVALID_PARAMETER"
        if target <= 0:
            return None, "ZERO_DENOMINATOR"
        if primitive_id == "min_coverage_snapshot":
            coverage_bid = min(Decimal(1), depth_bid / target)
            coverage_ask = min(Decimal(1), depth_ask / target)
            return float(min(coverage_bid, coverage_ask)), None
        capped_bid, capped_ask = min(depth_bid, target), min(depth_ask, target)
        return _ratio(capped_bid - capped_ask, capped_bid + capped_ask)

    if primitive_id in {"size_spread_snapshot", "fill_dist_asym_snapshot",
                        "walk_cost_asym_snapshot"}:
        if not book.full_valid:
            return None, "INVALID_DEPTH_BOOK"
        if tick_size is None:
            return None, "UNKNOWN_TICK_SIZE"
        target = _target_size(book, parameters.get("size"))
        if target is None:
            return None, "INVALID_PARAMETER"
        if target <= 0:
            return None, "ZERO_DENOMINATOR"
        buy_walk = _walk(book.ask, target, side="buy")
        sell_walk = _walk(book.bid, target, side="sell")
        if buy_walk is None or sell_walk is None:
            return None, "INSUFFICIENT_VISIBLE_DEPTH"
        if primitive_id == "size_spread_snapshot":
            return float((buy_walk[0] - sell_walk[0]) / tick_size), None
        if primitive_id == "fill_dist_asym_snapshot":
            dist_buy = (buy_walk[1] - book.ask[0][0]) / tick_size
            dist_sell = (book.bid[0][0] - sell_walk[1]) / tick_size
            return _ratio(
                dist_buy - dist_sell,
                Decimal(1) + dist_buy + dist_sell,
            )
        mid = (book.bid[0][0] + book.ask[0][0]) / Decimal(2)
        cost_buy = (buy_walk[0] - mid) / tick_size
        cost_sell = (mid - sell_walk[0]) / tick_size
        return _ratio(cost_buy - cost_sell, cost_buy + cost_sell)
    return None, "UNSUPPORTED_PRIMITIVE"


@dataclass(frozen=True)
class _Book:
    bid: tuple[tuple[Decimal | None, Decimal | None], ...]
    ask: tuple[tuple[Decimal | None, Decimal | None], ...]
    l1_valid: bool
    full_valid: bool


def _book(row: dict[str, Any]) -> _Book | None:
    if any(name not in row for name in _BOOK_COLUMNS):
        return None
    bid = tuple((row[f"bid_price_{level}"], row[f"bid_size_{level}"])
                for level in range(1, 6))
    ask = tuple((row[f"ask_price_{level}"], row[f"ask_size_{level}"])
                for level in range(1, 6))
    if any(value is None for value in bid[0] + ask[0]):
        return None
    l1_valid = (
        bid[0][0] > 0 and ask[0][0] > 0 and bid[0][0] <= ask[0][0]
        and bid[0][1] >= 0 and ask[0][1] >= 0
    )
    full_valid = l1_valid and all(
        price is not None and size is not None and price > 0 and size >= 0
        for price, size in (*bid, *ask)
    ) and all(
        bid[index][0] is not None and bid[index + 1][0] is not None
        and bid[index][0] >= bid[index + 1][0] for index in range(4)
    ) and all(
        ask[index][0] is not None and ask[index + 1][0] is not None
        and ask[index][0] <= ask[index + 1][0] for index in range(4)
    )
    return _Book(bid=bid, ask=ask, l1_valid=l1_valid, full_valid=full_valid)


def _flow_value(
    primitive_id: str,
    row: dict[str, Any],
    *,
    previous_buy: Decimal | None,
    previous_sell: Decimal | None,
) -> tuple[float | None, str | None]:
    column = {
        "active_buy_cumulative": "BUYVOL",
        "buy_cumulative_reset": "BUYVOL",
        "active_sell_cumulative": "SELLVOL",
        "sell_cumulative_reset": "SELLVOL",
        "trade_volume_interval": "volume_increment",
    }[primitive_id]
    value = row.get(column)
    if value is None:
        return None, "MISSING_REQUIRED_FIELD"
    if primitive_id == "trade_volume_interval":
        return (None, "INVALID_NATIVE_INTERVAL") if value < 0 else (float(value), None)
    if primitive_id in {"active_buy_cumulative", "active_sell_cumulative"}:
        return float(value), None
    previous = previous_buy if column == "BUYVOL" else previous_sell
    return float(Decimal(1) if previous is not None and value < previous else Decimal(0)), None


def _space_distance(space: Any) -> Decimal | None:
    if space not in {"d0", "d1", "d2", "d3", "d5"}:
        return None
    return Decimal(str(space)[1:])


def _target_size(book: _Book, size_label: Any) -> Decimal | None:
    alpha = {"q25": Decimal("0.25"), "q50": Decimal("0.50"), "q100": Decimal("1.00")}
    if size_label not in alpha or not book.full_valid:
        return None
    bid_depth = sum((size for _, size in book.bid), Decimal(0))
    ask_depth = sum((size for _, size in book.ask), Decimal(0))
    return alpha[size_label] * (bid_depth + ask_depth) / Decimal(2)


def _walk(
    levels: tuple[tuple[Decimal, Decimal], ...],
    target: Decimal,
    *,
    side: str,
) -> tuple[Decimal, Decimal] | None:
    remaining = target
    notional = Decimal(0)
    distance = None
    for index, (price, size) in enumerate(levels, start=1):
        consumed = min(size, max(Decimal(0), remaining))
        notional += price * consumed
        remaining -= consumed
        if remaining <= 0:
            distance = index - 1
            break
    if remaining > 0 or distance is None:
        return None
    reference = levels[0][0]
    end_price = levels[distance][0]
    del side, reference
    return notional / target, end_price


def _ratio(numerator: Decimal, denominator: Decimal) -> tuple[float | None, str | None]:
    if denominator == 0:
        return None, "ZERO_DENOMINATOR"
    return float(numerator / denominator), None


def _required_available_time(row: dict[str, Any], primitive_id: str) -> datetime | None:
    del primitive_id
    return row.get("available_time")


def _as_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("primitive read scope must be timezone-aware")
    return parsed.astimezone(UTC)
