"""Trusted Step 04 business-window operators over Step 03 FeatureResults."""

from __future__ import annotations

import bisect
import math
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from quantsage_discovery.core.errors import ArtifactValidationError

from .gap_policy import (
    GAP_POLICY_VERSION,
    KNOWN_SOURCE_GAP,
    WindowGapContext,
    gap_event_ids,
    gap_evidence_record_ids,
    matching_gap_events,
)
from .providers import FEATURE_VALUES_SCHEMA, make_feature_values_table

PROVIDER_ID = "quantsage_business_windows"
PROVIDER_KEY = "builtin.business_windows"
CATALOG_VERSION = "2.0.0"
IMPLEMENTATION_VERSION = "business_windows_v2"
FORMULA_REF = "gov_bond_l2_business_windows@2"
BUSINESS_AUDIT_TYPE = "business_window_audit"
BUSINESS_AUDIT_SCHEMA_VERSION = "business_window_audit_v2"
BUSINESS_AUDIT_SCHEMA = pa.schema([
    pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("instrument_id", pa.string(), nullable=False),
    pa.field("trade_date", pa.string(), nullable=False),
    pa.field("session_id", pa.string(), nullable=False),
    pa.field("window_start", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("n_total", pa.int64(), nullable=False),
    pa.field("n_valid", pa.int64(), nullable=False),
    pa.field("valid_ratio", pa.float64()),
    pa.field("invalid_reason", pa.string()),
    pa.field("matched_buy_time", pa.timestamp("us", tz="UTC")),
    pa.field("matched_sell_time", pa.timestamp("us", tz="UTC")),
    pa.field("matched_mid_time", pa.timestamp("us", tz="UTC")),
    pa.field("matched_tick_time", pa.timestamp("us", tz="UTC")),
    pa.field("max_endpoint_age_seconds", pa.float64()),
    pa.field("warning_flags", pa.list_(pa.string()), nullable=False),
    pa.field("source_gap_event_ids", pa.list_(pa.string()), nullable=False),
    pa.field("source_gap_evidence_record_ids", pa.list_(pa.string()), nullable=False),
    pa.field("gap_manifest_uri", pa.string()),
    pa.field("gap_manifest_hash", pa.string()),
    pa.field("gap_policy_version", pa.string(), nullable=False),
])
LOOKBACKS = ("1s", "3s", "1m", "5m", "10m", "15m", "30m", "60m")
SPACES = ("d0", "d1", "d2", "d3", "d5")
SIZES = ("q25", "q50", "q100")

_TEMPLATES = {
    "spread_ticks": ("F01", {}, None, False),
    "obi": ("F02", {"space": SPACES}, None, False),
    "wobi": ("F03", {"space": SPACES}, None, False),
    "min_coverage": ("F04", {"space": SPACES, "size": SIZES}, None, False),
    "capacity_imb": ("F05", {"space": SPACES, "size": SIZES}, None, False),
    "size_spread": ("F06", {"size": SIZES}, None, False),
    "fill_dist_asym": ("F07", {"size": SIZES}, None, True),
    "walk_cost_asym": ("F08", {"size": SIZES}, None, True),
    "active_flow_imb": ("F09", {}, ("buy", "sell", "buy_reset", "sell_reset"), False),
    "volume_to_depth": ("F10", {}, ("volume", "depth"), False),
    "mid_move_ticks": ("F11", {}, ("mid", "tick"), False),
    "flow_impact_eff": (
        "F12", {}, ("buy", "sell", "buy_reset", "sell_reset", "volume", "depth", "mid", "tick"),
        False,
    ),
}


@dataclass(frozen=True)
class BusinessOutput:
    values: pa.Table
    audit: pa.Table
    invalid_reason_counts: dict[str, int]
    provider_reasons: dict[str, int]
    quality_flags: list[str]


def validate_business_audit_parquet(path: Any) -> None:
    try:
        parquet = pq.ParquetFile(path)
    except Exception as exc:
        raise ArtifactValidationError(f"cannot read business audit Parquet: {exc}") from exc
    if not parquet.schema_arrow.equals(BUSINESS_AUDIT_SCHEMA, check_metadata=False):
        raise ArtifactValidationError(
            "business audit schema does not match business_window_audit_v2"
        )


def provider_document() -> dict[str, Any]:
    return {
        "schema_version": "provider_v1",
        "provider_id": PROVIDER_ID,
        "version": CATALOG_VERSION,
        "provider_key": PROVIDER_KEY,
        "implementation_version": IMPLEMENTATION_VERSION,
        "supported_steps": [4],
    }


def business_template_documents() -> list[dict[str, Any]]:
    documents = []
    for family_id, (family, domains, roles, pending) in _TEMPLATES.items():
        properties = {
            name: {"type": "string", "enum": list(values)}
            for name, values in domains.items()
        }
        defaults: dict[str, Any] = {}
        if family_id == "flow_impact_eff":
            properties["epsilon"] = {
                "type": "decimal", "unit": "normalized_flow_per_tick", "minimum": 0,
            }
            defaults["epsilon"] = "0.000000000001"
        requirements: dict[str, Any] = {"requires_max_endpoint_age": family in {"F09", "F11", "F12"}}
        if roles:
            requirements["input_roles"] = list(roles)
            requirements["input_units"] = {
                "buy": "source_size_unit", "sell": "source_size_unit",
                "buy_reset": "boolean", "sell_reset": "boolean",
                "volume": "source_size_unit", "depth": "source_size_unit",
                "mid": "quoted_price_unit", "tick": "quoted_price_unit",
            }
        document = {
            "schema_version": "feature_template_v1",
            "template_id": f"business_{family_id}",
            "version": CATALOG_VERSION,
            "name": f"{family} {family_id}",
            "description": f"Fixed {family} business-window definition.",
            "formula_ref": FORMULA_REF,
            "provider_ref": f"{PROVIDER_ID}@{CATALOG_VERSION}",
            "parameters_schema": {
                "type": "object",
                "properties": properties,
                "required": list(properties),
                "additionalProperties": False,
            },
            "defaults": defaults,
            "output_schema": {"value": "float64"},
            "unit": (
                "ticks" if family_id in {"spread_ticks", "mid_move_ticks", "size_spread"}
                else ("source_size_unit" if family_id == "volume_to_depth" else "dimensionless")
            ),
            "status": "pending_semantics" if pending else "ready",
            "step": 4,
            "kind": "business",
            "family": family,
            "time_policy": {},
            "source_requirements": requirements,
            "validity": {
                "missing_policy": "na_with_reason",
                "zero_denominator_policy": "na_with_reason",
                "stale_policy": "endpoint_age_limited",
                "reason_codes": [
                    "HISTORY_INSUFFICIENT", "NO_VALID_OBSERVATIONS", "WINDOW_RESET",
                    "ENDPOINT_MISSING", "ENDPOINT_AGE_EXCEEDED", "ZERO_DENOMINATOR",
                    KNOWN_SOURCE_GAP,
                ],
            },
        }
        if pending:
            document["reason"] = "Horizon aggregation semantics remain pending."
        documents.append(document)
    return documents


def business_catalog_entries() -> list[dict[str, Any]]:
    entries = []
    for lookback in LOOKBACKS:
        entries.append(_entry("spread_ticks", "F01", {}, lookback, False))
        for space in SPACES:
            entries.append(_entry("obi", "F02", {"space": space}, lookback, False))
            entries.append(_entry("wobi", "F03", {"space": space}, lookback, False))
        for space in SPACES:
            for size in SIZES:
                entries.append(_entry(
                    "min_coverage", "F04", {"space": space, "size": size}, lookback, False,
                ))
                entries.append(_entry(
                    "capacity_imb", "F05", {"space": space, "size": size}, lookback, False,
                ))
        for size in SIZES:
            entries.append(_entry("size_spread", "F06", {"size": size}, lookback, False))
            entries.append(_entry(
                "fill_dist_asym", "F07", {"size": size}, lookback, True,
            ))
            entries.append(_entry(
                "walk_cost_asym", "F08", {"size": size}, lookback, True,
            ))
        for family_id, family in (
            ("active_flow_imb", "F09"), ("volume_to_depth", "F10"),
            ("mid_move_ticks", "F11"), ("flow_impact_eff", "F12"),
        ):
            entries.append(_entry(family_id, family, {}, lookback, False))
    return entries


def _entry(family_id: str, family: str, parameters: dict[str, str],
           lookback: str, pending: bool) -> dict[str, Any]:
    suffix = "".join(f"__{value}" for value in parameters.values())
    return {
        "factor_name": f"{family_id}{suffix}__h{lookback}",
        "family": family,
        "template_ref": f"business_{family_id}@{CATALOG_VERSION}",
        "parameters": parameters,
        "lookback": lookback,
        "status": "pending_semantics" if pending else "ready",
    }


def legacy_factor_name_aliases() -> dict[str, str]:
    """Map only the historical v1 s-space display names to canonical d-space names.

    The aliases are not emitted by the new catalog and do not merge definitions.
    They provide an auditable lookup for references written against catalog v1.
    """
    aliases = {}
    for entry in business_catalog_entries():
        if "space" not in entry["parameters"]:
            continue
        canonical = entry["factor_name"]
        legacy = canonical.replace("__d", "__s", 1)
        aliases[legacy] = canonical
    return dict(sorted(aliases.items()))


def register_business_catalog(service: Any) -> dict[str, Any]:
    provider = service.register_provider(provider_document())
    templates = [service.register_template(item) for item in business_template_documents()]
    return {"provider": provider, "templates": templates,
            "catalog_size": len(business_catalog_entries())}


def compute_business_window(
    feature: dict[str, Any],
    inputs: dict[str, dict[str, Any]],
    partition: dict[str, Any],
    *,
    context: dict[str, Any] | WindowGapContext | None = None,
) -> BusinessOutput:
    family_id = feature["template_ref"].split("@", 1)[0].removeprefix("business_")
    if _TEMPLATES[family_id][3]:
        raise ArtifactValidationError(f"{family_id} horizon aggregation is pending")
    window_context = _window_context(context)
    lookback_ns = _duration_ns(feature.get("time_policy", {}).get("lookback"))
    lookback = timedelta(microseconds=(lookback_ns + 999) // 1_000)
    prepared = {role: _prepared_table(data["table"]) for role, data in inputs.items()}
    if family_id in {"spread_ticks", "obi", "wobi", "min_coverage", "capacity_imb",
                     "size_spread"}:
        return _rolling_mean(
            feature, prepared["input"], partition, lookback, window_context,
            ("input",),
        )
    if family_id in {"active_flow_imb", "flow_impact_eff"}:
        max_age = _max_age(feature)
        return _flow_windows(
            family_id, feature, prepared, partition, lookback, max_age, window_context,
        )
    if family_id == "volume_to_depth":
        return _volume_depth_window(
            prepared, partition, lookback, window_context, ("volume", "depth"),
        )
    if family_id == "mid_move_ticks":
        max_age = _max_age(feature)
        return _mid_move_window(
            prepared, partition, lookback, max_age, window_context, ("mid", "tick"),
        )
    raise ArtifactValidationError(f"unsupported business family: {family_id}")


def _prepared_table(table: pa.Table) -> dict[str, Any]:
    rows = sorted(table.to_pylist(), key=lambda row: row["event_time"])
    times = [row["event_time"] for row in rows]
    values = [row["value"] for row in rows]
    return {"rows": rows, "times": times, "values": values}


def _window_context(
    context: dict[str, Any] | WindowGapContext | None,
) -> WindowGapContext:
    if context is None:
        raise ArtifactValidationError(
            "business windows require explicit true-session/history/gap context"
        )
    if isinstance(context, WindowGapContext):
        return context
    return WindowGapContext.from_document(context)


def _window_gaps(
    context: WindowGapContext,
    partition: dict[str, Any],
    roles: tuple[str, ...],
    start: datetime,
    end: datetime,
    *,
    include_start: bool,
) -> tuple[dict[str, Any], ...]:
    return matching_gap_events(
        context,
        roles=roles,
        exchange=partition.get("exchange"),
        instrument_id=partition["instrument_id"],
        trade_date=str(partition["trade_date"]),
        session_id=partition["session_id"],
        start=start,
        end=end,
        include_start=include_start,
    )


def _warning_flags(
    context: WindowGapContext,
    roles: tuple[str, ...],
    row: dict[str, Any],
) -> list[str]:
    return sorted({
        *context.warning_flags_for(roles),
        *(str(flag) for flag in row.get("quality_flags") or ()),
    })


def _rolling_mean(feature, data, partition, lookback, context, roles) -> BusinessOutput:
    start, end = _as_utc(partition["start"]), _as_utc(partition["end"])
    values, audits = [], []
    reasons: Counter[str] = Counter()
    for index, row in enumerate(data["rows"]):
        event_time = row["event_time"]
        if not start <= event_time < end:
            continue
        low = bisect.bisect_right(data["times"], event_time - lookback)
        high = bisect.bisect_right(data["times"], event_time)
        n_total = high - low
        valid_values = [
            float(value) for value in data["values"][low:high]
            if value is not None and math.isfinite(value)
        ]
        n_valid = len(valid_values)
        reason = None
        value = None
        window_start = event_time - lookback
        gap_events = _window_gaps(
            context, partition, roles, window_start, event_time, include_start=False,
        )
        if context.history_is_insufficient(window_start):
            reason = "HISTORY_INSUFFICIENT"
        elif gap_events:
            reason = KNOWN_SOURCE_GAP
        elif n_total == 0 or n_valid == 0:
            reason = "NO_VALID_OBSERVATIONS"
        else:
            value = math.fsum(valid_values) / n_valid
        if reason:
            reasons[reason] += 1
        values.append(_value_row(row, value, None))
        audits.append(_audit_row(
            row, window_start, n_total, n_valid, reason,
            warning_flags=_warning_flags(context, roles, row),
            gap_events=gap_events,
            gap_context=context,
        ))
    return _output(values, audits, reasons, feature)


def _flow_windows(
    family_id, feature, tables, partition, lookback, max_age, context,
) -> BusinessOutput:
    start, end = _as_utc(partition["start"]), _as_utc(partition["end"])
    buy, sell = tables["buy"], tables["sell"]
    values, audits = [], []
    reasons: Counter[str] = Counter()
    for row in buy["rows"]:
        t = row["event_time"]
        if not start <= t < end:
            continue
        window_start = t - lookback
        reason = None
        b_match = _endpoint(buy, window_start, max_age)
        s_match = _endpoint(sell, window_start, max_age)
        current_b = _current(buy, t)
        current_s = _current(sell, t)
        value = None
        audit_total = 1
        audit_valid = 0
        mid_match = mid_now = tick_now = None
        roles = (
            ("buy", "sell", "buy_reset", "sell_reset")
            if family_id == "active_flow_imb"
            else (
                "buy", "sell", "buy_reset", "sell_reset", "volume", "depth",
                "mid", "tick",
            )
        )
        gap_events = _window_gaps(
            context, partition, roles, window_start, t, include_start=True,
        )
        if context.history_is_insufficient(window_start):
            reason = "HISTORY_INSUFFICIENT"
        elif gap_events:
            reason = KNOWN_SOURCE_GAP
        elif b_match is None or s_match is None or current_b is None or current_s is None:
            reason = "ENDPOINT_MISSING"
        elif b_match[2] > max_age or s_match[2] > max_age:
            reason = "ENDPOINT_AGE_EXCEEDED"
        elif _has_reset(tables["buy_reset"], window_start, t) or \
                _has_reset(tables["sell_reset"], window_start, t):
            reason = "WINDOW_RESET"
        if reason is None:
            b_delta = current_b[0] - b_match[0]
            s_delta = current_s[0] - s_match[0]
            result = b_delta - s_delta
            if family_id == "active_flow_imb":
                denominator = b_delta + s_delta
                value = None if denominator == 0 else result / denominator
                if denominator == 0:
                    reason = "ZERO_DENOMINATOR"
            else:
                volume = tables["volume"]
                depth = tables["depth"]
                low = bisect.bisect_right(volume["times"], window_start)
                high = bisect.bisect_right(volume["times"], t)
                n_total = high - low
                valid_volume = [
                    float(value) for value in volume["values"][low:high]
                    if value is not None and math.isfinite(value)
                ]
                n_valid = len(valid_volume)
                audit_total = n_total
                audit_valid = n_valid
                depth_avg = _window_mean(depth, window_start, t)
                mid_match = _endpoint(tables["mid"], window_start, max_age)
                mid_now = _current(tables["mid"], t)
                tick_now = _current(tables["tick"], t)
                if n_total == 0 or n_valid != n_total or depth_avg is None:
                    reason = "NO_VALID_OBSERVATIONS"
                elif mid_match is None or mid_now is None or tick_now is None:
                    reason = "ENDPOINT_MISSING"
                elif mid_match[2] > max_age:
                    reason = "ENDPOINT_AGE_EXCEEDED"
                elif result == 0 or depth_avg == 0 or tick_now[0] == 0:
                    reason = "ZERO_DENOMINATOR"
                else:
                    signed_move = (1 if result > 0 else -1) * (
                        mid_now[0] - mid_match[0]
                    ) / tick_now[0]
                    epsilon = float(feature["parameters"].get(
                        "epsilon", "0.000000000001",
                    ))
                    value = signed_move / (abs(result) / depth_avg + epsilon)
                audit_valid = int(value is not None)
        if reason:
            reasons[reason] += 1
        values.append(_value_row(row, value, None))
        audits.append(_audit_row(
            row, window_start,
            audit_total, audit_valid, reason,
            matched_buy=b_match, matched_sell=s_match,
            matched_mid=(mid_match if family_id == "flow_impact_eff" else None),
            matched_tick=(tick_now if family_id == "flow_impact_eff" else None),
            warning_flags=_warning_flags(context, roles, row),
            gap_events=gap_events,
            gap_context=context,
        ))
    return _output(values, audits, reasons, feature)


def _volume_depth_window(tables, partition, lookback, context, roles) -> BusinessOutput:
    volume, depth = tables["volume"], tables["depth"]
    start, end = _as_utc(partition["start"]), _as_utc(partition["end"])
    values, audits = [], []
    reasons: Counter[str] = Counter()
    for row in volume["rows"]:
        t = row["event_time"]
        if not start <= t < end:
            continue
        ws = t - lookback
        low, high = bisect.bisect_right(volume["times"], ws), bisect.bisect_right(volume["times"], t)
        n_total = high - low
        valid_volume = [
            float(value) for value in volume["values"][low:high]
            if value is not None and math.isfinite(value)
        ]
        n_valid = len(valid_volume)
        d_low, d_high = bisect.bisect_right(depth["times"], ws), bisect.bisect_right(depth["times"], t)
        valid_depth = [
            float(value) for value in depth["values"][d_low:d_high]
            if value is not None and math.isfinite(value)
        ]
        d_valid = len(valid_depth)
        reason = value = None
        gap_events = _window_gaps(
            context, partition, roles, ws, t, include_start=False,
        )
        if context.history_is_insufficient(ws):
            reason = "HISTORY_INSUFFICIENT"
        elif gap_events:
            reason = KNOWN_SOURCE_GAP
        elif n_total == 0 or n_valid != n_total or d_valid == 0:
            reason = "NO_VALID_OBSERVATIONS"
        else:
            depth_avg = math.fsum(valid_depth) / d_valid
            if depth_avg == 0:
                reason = "ZERO_DENOMINATOR"
            else:
                volume_sum = math.fsum(valid_volume)
                value = volume_sum / depth_avg
        if reason:
            reasons[reason] += 1
        values.append(_value_row(row, value, None))
        audits.append(_audit_row(
            row, ws, n_total, n_valid, reason,
            warning_flags=_warning_flags(context, roles, row),
            gap_events=gap_events,
            gap_context=context,
        ))
    return _output(values, audits, reasons, None)


def _mid_move_window(tables, partition, lookback, max_age, context, roles) -> BusinessOutput:
    mid, tick = tables["mid"], tables["tick"]
    start, end = _as_utc(partition["start"]), _as_utc(partition["end"])
    values, audits = [], []
    reasons: Counter[str] = Counter()
    for row in mid["rows"]:
        t = row["event_time"]
        if not start <= t < end:
            continue
        ws = t - lookback
        match = _endpoint(mid, ws, max_age)
        current = _current(mid, t)
        current_tick = _current(tick, t)
        reason = value = None
        gap_events = _window_gaps(
            context, partition, roles, ws, t, include_start=True,
        )
        if context.history_is_insufficient(ws):
            reason = "HISTORY_INSUFFICIENT"
        elif gap_events:
            reason = KNOWN_SOURCE_GAP
        elif match is None or current is None or current_tick is None:
            reason = "ENDPOINT_MISSING"
        elif match[2] > max_age:
            reason = "ENDPOINT_AGE_EXCEEDED"
        elif current_tick[0] == 0:
            reason = "ZERO_DENOMINATOR"
        else:
            value = (current[0] - match[0]) / current_tick[0]
        if reason:
            reasons[reason] += 1
        values.append(_value_row(row, value, None))
        audits.append(_audit_row(
            row, ws, 1, int(value is not None), reason,
            matched_mid=match, matched_tick=current_tick,
            warning_flags=_warning_flags(context, roles, row),
            gap_events=gap_events,
            gap_context=context,
        ))
    return _output(values, audits, reasons, None)


def _endpoint(data, at, max_age):
    del max_age
    index = bisect.bisect_right(data["times"], at) - 1
    while index >= 0 and data["values"][index] is None:
        index -= 1
    if index < 0:
        return None
    return data["values"][index], data["times"][index], at - data["times"][index]


def _current(data, at):
    index = bisect.bisect_right(data["times"], at) - 1
    if index < 0 or data["times"][index] != at or data["values"][index] is None:
        return None
    return data["values"][index], data["times"][index]


def _has_reset(data, start, end) -> bool:
    low, high = bisect.bisect_right(data["times"], start), bisect.bisect_right(data["times"], end)
    return any((value or 0) > 0.5 for value in data["values"][low:high])


def _window_mean(data, start, end):
    low = bisect.bisect_right(data["times"], start)
    high = bisect.bisect_right(data["times"], end)
    valid_values = [
        float(value) for value in data["values"][low:high]
        if value is not None and math.isfinite(value)
    ]
    if not valid_values:
        return None
    return math.fsum(valid_values) / len(valid_values)


def _value_row(row, value, available):
    return {
        "event_time": row["event_time"],
        "available_time": available,
        "instrument_id": row["instrument_id"],
        "trade_date": row["trade_date"],
        "session_id": row["session_id"],
        "value": value,
    }


def _audit_row(row, window_start, n_total, n_valid, reason, *, matched_buy=None,
               matched_sell=None, matched_mid=None, matched_tick=None,
               warning_flags=None, gap_events=(), gap_context=None) -> dict[str, Any]:
    max_age = max(
        ((item[2] if len(item) > 2 else timedelta(0)).total_seconds()
         for item in (matched_buy, matched_sell, matched_mid, matched_tick)
         if item is not None),
        default=None,
    )
    return {
        "event_time": row["event_time"],
        "instrument_id": row["instrument_id"],
        "trade_date": str(row["trade_date"]),
        "session_id": row["session_id"],
        "window_start": window_start,
        "n_total": n_total,
        "n_valid": n_valid,
        "valid_ratio": (n_valid / n_total) if n_total else None,
        "invalid_reason": reason,
        "matched_buy_time": matched_buy[1] if matched_buy else None,
        "matched_sell_time": matched_sell[1] if matched_sell else None,
        "matched_mid_time": matched_mid[1] if matched_mid else None,
        "matched_tick_time": matched_tick[1] if matched_tick else None,
        "max_endpoint_age_seconds": max_age,
        "warning_flags": list(warning_flags or []),
        "source_gap_event_ids": gap_event_ids(gap_events),
        "source_gap_evidence_record_ids": gap_evidence_record_ids(gap_events),
        "gap_manifest_uri": (
            gap_context.gap_manifest_uri if gap_context is not None else None
        ),
        "gap_manifest_hash": (
            gap_context.gap_manifest_hash if gap_context is not None else None
        ),
        "gap_policy_version": GAP_POLICY_VERSION,
    }


def _output(values, audits, reasons, feature) -> BusinessOutput:
    value_table = make_feature_values_table(values) if values else FEATURE_VALUES_SCHEMA.empty_table()
    audit_table = (pa.Table.from_pylist(audits, schema=BUSINESS_AUDIT_SCHEMA)
                   if audits else BUSINESS_AUDIT_SCHEMA.empty_table())
    flags = []
    if not values:
        flags.append("ZERO_OBSERVATIONS")
    elif all(row["value"] is None for row in values):
        flags.append("ALL_NA")
    return BusinessOutput(
        values=value_table,
        audit=audit_table,
        invalid_reason_counts=dict(sorted(reasons.items())),
        provider_reasons=dict(sorted(reasons.items())),
        quality_flags=flags,
    )


def _duration_ns(value: Any) -> int:
    if not isinstance(value, str) or not value.endswith("ns") or not value[:-2].isdigit():
        raise ArtifactValidationError("business lookback must be normalized nanoseconds")
    return int(value[:-2])


def _max_age(feature: dict[str, Any]) -> timedelta:
    value = feature.get("time_policy", {}).get("max_endpoint_age")
    if value is None:
        raise ArtifactValidationError(
            f"{feature['template_ref']} requires max_endpoint_age"
        )
    return timedelta(microseconds=(_duration_ns(value) + 999) // 1_000)


def _as_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ArtifactValidationError("business scope times must be timezone-aware")
    return parsed.astimezone(UTC)
