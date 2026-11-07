"""Step 05-A trailing clock-grid profiling and basic descriptions."""

from __future__ import annotations

import bisect
import math
import statistics
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

from .business_windows import BUSINESS_AUDIT_SCHEMA, BusinessOutput
from .errors import ResearchValidationError
from .providers import FEATURE_VALUES_SCHEMA, make_feature_values_table

PROVIDER_ID = "quantsage_statistical_windows"
PROVIDER_KEY = "builtin.statistical_windows"
CATALOG_VERSION = "1.0.1"
IMPLEMENTATION_VERSION = "statistical_windows_v2"
FORMULA_REF = "quantsage_statistical_descriptions@1"
INPUT_VIEW_POLICY = "step05_input_whitelist@1"

METRICS = {
    "mean": ("Arithmetic mean", "source_unit"),
    "std": ("Sample standard deviation (ddof=1)", "source_unit"),
    "median": ("Median", "source_unit"),
    "mad": ("Median absolute deviation", "source_unit"),
    "iqr": ("Interquartile range", "source_unit"),
    "q01": ("1% quantile", "source_unit"),
    "q05": ("5% quantile", "source_unit"),
    "q25": ("25% quantile", "source_unit"),
    "q75": ("75% quantile", "source_unit"),
    "q95": ("95% quantile", "source_unit"),
    "q99": ("99% quantile", "source_unit"),
    "min": ("Minimum", "source_unit"),
    "max": ("Maximum", "source_unit"),
    "zero_ratio": ("Zero ratio", "dimensionless"),
    "positive_ratio": ("Positive ratio", "dimensionless"),
    "negative_ratio": ("Negative ratio", "dimensionless"),
    "unchanged_ratio": ("Adjacent unchanged ratio", "dimensionless"),
}

_BASE_DESCRIPTIONS = {
    "mean", "std", "median", "mad", "iqr", "q01", "q05", "q25", "q75",
    "q95", "q99", "min", "max", "unchanged_ratio",
}
_NONNEGATIVE_DESCRIPTIONS = _BASE_DESCRIPTIONS | {"zero_ratio"}
_SIGNED_DESCRIPTIONS = _NONNEGATIVE_DESCRIPTIONS | {"positive_ratio", "negative_ratio"}

# This is deliberately a closed, semantic whitelist rather than a source-kind
# shortcut. Boolean controls, cumulative counters and static tick-size rows are
# not valid profile sequences; derived flow and movement views are allowed.
INPUT_VIEW_WHITELIST = {
    "primitive:mid": ("price_dynamics", "quoted_price_unit", _BASE_DESCRIPTIONS),
    "primitive:spread_ticks": ("price_dynamics", "ticks", _NONNEGATIVE_DESCRIPTIONS),
    "primitive:depth_bid_1": ("liquidity", "source_size_unit", _NONNEGATIVE_DESCRIPTIONS),
    "primitive:depth_ask_1": ("liquidity", "source_size_unit", _NONNEGATIVE_DESCRIPTIONS),
    "primitive:depth_imbalance_1": ("liquidity", "dimensionless", _SIGNED_DESCRIPTIONS),
    "primitive:microprice_proxy": ("price_dynamics", "quoted_price_unit", _BASE_DESCRIPTIONS),
    "primitive:quote_interarrival": (
        "temporal_structure", "seconds", _NONNEGATIVE_DESCRIPTIONS,
    ),
    "primitive:obi_snapshot": ("F02", "dimensionless", _SIGNED_DESCRIPTIONS),
    "primitive:wobi_snapshot": ("F03", "dimensionless", _SIGNED_DESCRIPTIONS),
    "primitive:min_coverage_snapshot": ("F04", "dimensionless", _NONNEGATIVE_DESCRIPTIONS),
    "primitive:capacity_imb_snapshot": ("F05", "dimensionless", _SIGNED_DESCRIPTIONS),
    "primitive:size_spread_snapshot": ("F06", "ticks", _NONNEGATIVE_DESCRIPTIONS),
    "primitive:fill_dist_asym_snapshot": ("F07", "dimensionless", _SIGNED_DESCRIPTIONS),
    "primitive:walk_cost_asym_snapshot": ("F08", "dimensionless", _SIGNED_DESCRIPTIONS),
    "primitive:visible_depth_bid_5": ("input", "source_size_unit", _NONNEGATIVE_DESCRIPTIONS),
    "primitive:visible_depth_ask_5": ("input", "source_size_unit", _NONNEGATIVE_DESCRIPTIONS),
    "primitive:depth_ref_5": ("input", "source_size_unit", _NONNEGATIVE_DESCRIPTIONS),
    "primitive:target_size_q25": ("input", "source_size_unit", _NONNEGATIVE_DESCRIPTIONS),
    "primitive:target_size_q50": ("input", "source_size_unit", _NONNEGATIVE_DESCRIPTIONS),
    "primitive:target_size_q100": ("input", "source_size_unit", _NONNEGATIVE_DESCRIPTIONS),
    "primitive:trade_volume_interval": (
        "input", "source_size_unit", _NONNEGATIVE_DESCRIPTIONS,
    ),
    "business:spread_ticks": ("F01", "ticks", _NONNEGATIVE_DESCRIPTIONS),
    "business:obi": ("F02", "dimensionless", _SIGNED_DESCRIPTIONS),
    "business:wobi": ("F03", "dimensionless", _SIGNED_DESCRIPTIONS),
    "business:min_coverage": ("F04", "dimensionless", _NONNEGATIVE_DESCRIPTIONS),
    "business:capacity_imb": ("F05", "dimensionless", _SIGNED_DESCRIPTIONS),
    "business:size_spread": ("F06", "ticks", _NONNEGATIVE_DESCRIPTIONS),
    "business:fill_dist_asym": ("F07", "dimensionless", _SIGNED_DESCRIPTIONS),
    "business:walk_cost_asym": ("F08", "dimensionless", _SIGNED_DESCRIPTIONS),
    "business:active_flow_imb": ("F09", "dimensionless", _SIGNED_DESCRIPTIONS),
    "business:volume_to_depth": ("F10", "source_size_unit", _NONNEGATIVE_DESCRIPTIONS),
    "business:mid_move_ticks": ("F11", "ticks", _SIGNED_DESCRIPTIONS),
    "business:flow_impact_eff": ("F12", "dimensionless", _SIGNED_DESCRIPTIONS),
}


def provider_document() -> dict[str, Any]:
    return {
        "schema_version": "provider_v1",
        "provider_id": PROVIDER_ID,
        "version": CATALOG_VERSION,
        "provider_key": PROVIDER_KEY,
        "implementation_version": IMPLEMENTATION_VERSION,
        "supported_steps": [5],
    }


def statistical_template_document() -> dict[str, Any]:
    return {
        "schema_version": "feature_template_v1",
        "template_id": "statistical_trailing",
        "version": CATALOG_VERSION,
        "name": "Trailing clock-grid statistical profile",
        "description": "Complete 1s-grid trailing window over one selected input.",
        "formula_ref": FORMULA_REF,
        "provider_ref": f"{PROVIDER_ID}@{CATALOG_VERSION}",
        "parameters_schema": {
            "type": "object", "properties": {}, "required": [],
            "additionalProperties": False,
        },
        "defaults": {},
        "output_schema": {"value": "float64"},
        "unit": "source_unit",
        "status": "ready",
        "step": 5,
        "kind": "statistical",
        "family": "basic_description",
        "time_policy": {},
        "source_requirements": {
            "requires_max_feature_age": True,
            "input_roles": ["input"],
            "input_view_policy": INPUT_VIEW_POLICY,
        },
        "validity": {
            "missing_policy": "na_with_reason",
            "min_samples": 1,
            "reason_codes": [
                "INSUFFICIENT_HISTORY", "WINDOW_NA", "STALE_DATA",
                "CONSTANT_INPUT", "NUMERICAL_FAILURE",
            ],
        },
    }


def metric_documents() -> list[dict[str, Any]]:
    documents = []
    for metric_id, (name, unit) in METRICS.items():
        documents.append({
            "schema_version": "metric_v1",
            "metric_id": f"stat.{metric_id}",
            "version": CATALOG_VERSION,
            "name": name,
            "description": f"Trailing-window {metric_id}.",
            "formula_ref": FORMULA_REF,
            "provider_ref": f"{PROVIDER_ID}@{CATALOG_VERSION}",
            "parameters_schema": {
                "type": "object", "properties": {}, "required": [],
                "additionalProperties": False,
            },
            "defaults": {},
            "output_schema": {"value": "float64"},
            "unit": unit,
            "status": "ready",
        })
    return documents


def metric_set_document() -> dict[str, Any]:
    return {
        "schema_version": "metric_set_v1",
        "metric_set_id": "basic_description",
        "version": 2,
        "metrics": [f"stat.{name}@{CATALOG_VERSION}" for name in METRICS],
    }


def register_statistical_catalog(service: Any) -> dict[str, Any]:
    provider = service.register_provider(provider_document())
    template = service.register_template(statistical_template_document())
    metrics = [service.register_metric(document) for document in metric_documents()]
    metric_set = service.register_metric_set(metric_set_document())
    return {"provider": provider, "template": template,
            "metrics": metrics, "metric_set": metric_set}


def validate_statistical_input(
    input_feature: dict[str, Any],
    metric_refs: list[str],
) -> None:
    """Reject inputs or metrics outside the frozen Step 05-A view policy."""
    kind = input_feature.get("kind")
    if kind == "primitive":
        view_id = input_feature.get("source_requirements", {}).get("primitive_id")
        key = f"primitive:{view_id}"
    elif kind == "business":
        template_id = str(input_feature.get("template_ref", "")).split("@", 1)[0]
        view_id = template_id.removeprefix("business_")
        key = f"business:{view_id}"
    else:
        raise ResearchValidationError(
            "statistical input must be a primitive or business Feature",
            details={"input_feature_ref": _feature_ref(input_feature), "kind": kind},
        )
    expected = INPUT_VIEW_WHITELIST.get(key)
    if expected is None:
        raise ResearchValidationError(
            "statistical input view is not in the explicit Step 05 whitelist",
            details={"input_view": view_id, "input_kind": kind},
        )
    family, unit, admitted_metrics = expected
    if input_feature.get("family") != family or input_feature.get("unit") != unit:
        raise ResearchValidationError(
            "statistical input family or unit does not match the whitelist",
            details={
                "input_view": view_id,
                "expected_family": family,
                "actual_family": input_feature.get("family"),
                "expected_unit": unit,
                "actual_unit": input_feature.get("unit"),
            },
        )
    metric_ids = [ref.split("@", 1)[0].removeprefix("stat.") for ref in metric_refs]
    rejected = sorted(set(metric_ids) - admitted_metrics)
    if rejected:
        raise ResearchValidationError(
            "one or more metrics are not admissible for this statistical input",
            details={
                "input_view": view_id,
                "unsupported_metrics": rejected,
                "admissible_metrics": sorted(admitted_metrics),
            },
        )


def compute_statistical_window(
    feature: dict[str, Any],
    input_data: dict[str, Any],
    partition: dict[str, Any],
    read_scope: dict[str, Any],
) -> BusinessOutput:
    metric_ref = feature.get("metric_ref")
    if not metric_ref or metric_ref.split("@", 1)[0] not in {
        f"stat.{name}" for name in METRICS
    }:
        raise ValueError(f"unsupported statistical metric: {metric_ref}")
    metric_id = metric_ref.split("@", 1)[0].removeprefix("stat.")
    window_ns = _duration_ns(feature["time_policy"]["profile_window"])
    sampling = feature["sampling"]
    interval_ns = _duration_ns(sampling["interval"])
    stride_ns = _duration_ns(sampling["evaluation_stride"])
    max_age_ns = sampling.get("max_feature_age")
    if max_age_ns is None:
        raise ValueError("statistical window requires max_feature_age")
    max_age = timedelta(microseconds=(_duration_ns(max_age_ns) + 999) // 1_000)
    window = timedelta(microseconds=(window_ns + 999) // 1_000)
    interval = timedelta(microseconds=(interval_ns + 999) // 1_000)
    stride = timedelta(microseconds=(stride_ns + 999) // 1_000)
    if interval_ns <= 0 or stride_ns <= 0 or window_ns <= 0:
        raise ValueError("statistical window durations must be positive")
    if window_ns % interval_ns or stride_ns % interval_ns:
        raise ValueError("profile window and stride must be multiples of the sampling interval")
    if not input_data or "table" not in input_data:
        raise ValueError("statistical window requires one resolved input value table")
    rows = sorted(input_data["table"].to_pylist(), key=lambda row: row["event_time"])
    times = [row["event_time"] for row in rows]
    values = [row["value"] for row in rows]
    if "session_start" not in read_scope:
        raise ValueError("statistical window requires the true session_start in read_scope")
    session_start = _as_utc(read_scope["session_start"])
    output_start = _as_utc(partition["start"])
    output_end = _as_utc(partition["end"])
    if not session_start <= output_start < output_end:
        raise ValueError("statistical output scope must lie inside its session")
    eligible_start = max(output_start, session_start + window)
    stride_microseconds = stride_ns // 1_000
    elapsed_microseconds = (eligible_start - session_start) // timedelta(microseconds=1)
    first_step = -(-elapsed_microseconds // stride_microseconds)
    first_grid = session_start + first_step * stride
    grid = []
    current = first_grid
    while current < output_end:
        grid.append(current)
        current += stride
    output_rows, audits = [], []
    reasons: Counter[str] = Counter()
    for grid_time in grid:
        samples = []
        reason = None
        max_age_seen = None
        sample_time = grid_time - window + interval
        expected = window_ns // interval_ns
        while sample_time <= grid_time:
            index = bisect.bisect_right(times, sample_time) - 1
            if index < 0:
                reason = "INSUFFICIENT_HISTORY"
                samples = []
                break
            age = sample_time - times[index]
            max_age_seen = age if max_age_seen is None else max(max_age_seen, age)
            if age > max_age:
                reason = "STALE_DATA"
                samples = []
                break
            value = values[index]
            if value is None or not math.isfinite(value):
                reason = "WINDOW_NA"
                samples = []
                break
            samples.append(float(value))
            sample_time += interval
        result = None if reason else _metric_value(metric_id, samples)
        if result is not None and not math.isfinite(result):
            result, reason = None, "NUMERICAL_FAILURE"
        if result is None and reason is None:
            reason = "CONSTANT_INPUT" if metric_id in {"std"} else "NUMERICAL_FAILURE"
        if reason:
            reasons[reason] += 1
        row = {
            "event_time": grid_time,
            "available_time": None,
            "instrument_id": rows[0]["instrument_id"] if rows else partition["instrument_id"],
            "trade_date": rows[0]["trade_date"] if rows else partition["trade_date"],
            "session_id": partition["session_id"],
            "value": result,
        }
        output_rows.append(row)
        audits.append({
            "event_time": grid_time,
            "instrument_id": row["instrument_id"],
            "trade_date": str(row["trade_date"]),
            "session_id": partition["session_id"],
            "window_start": grid_time - window,
            "n_total": expected,
            "n_valid": 0 if result is None else len(samples),
            "valid_ratio": None if result is None else 1.0,
            "invalid_reason": reason,
            "matched_buy_time": None, "matched_sell_time": None,
            "matched_mid_time": None, "matched_tick_time": None,
            "max_endpoint_age_seconds": (
                None if max_age_seen is None else max_age_seen.total_seconds()
            ),
            "warning_flags": [f"METRIC:{metric_id}"],
        })
    values_table = make_feature_values_table(output_rows) if output_rows else FEATURE_VALUES_SCHEMA.empty_table()
    audit_table = (BUSINESS_AUDIT_SCHEMA.empty_table() if not audits else
                   __import__("pyarrow").Table.from_pylist(audits, schema=BUSINESS_AUDIT_SCHEMA))
    flags = []
    if not output_rows:
        flags.append("ZERO_OBSERVATIONS")
    elif all(row["value"] is None for row in output_rows):
        flags.append("ALL_NA")
    return BusinessOutput(values_table, audit_table, dict(reasons), dict(reasons), flags)


def _metric_value(metric_id: str, values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if metric_id == "mean":
        return math.fsum(values) / len(values)
    if metric_id == "std":
        return statistics.stdev(values) if len(values) >= 2 else None
    if metric_id == "median":
        return statistics.median(values)
    if metric_id == "mad":
        median = statistics.median(values)
        return statistics.median(abs(value - median) for value in values)
    if metric_id == "iqr":
        return _quantile(ordered, 0.75) - _quantile(ordered, 0.25)
    if metric_id.startswith("q"):
        return _quantile(ordered, int(metric_id[1:]) / 100)
    if metric_id == "min":
        return ordered[0]
    if metric_id == "max":
        return ordered[-1]
    if metric_id == "zero_ratio":
        return sum(value == 0 for value in values) / len(values)
    if metric_id == "positive_ratio":
        return sum(value > 0 for value in values) / len(values)
    if metric_id == "negative_ratio":
        return sum(value < 0 for value in values) / len(values)
    if metric_id == "unchanged_ratio":
        return (sum(values[i] == values[i - 1] for i in range(1, len(values)))
                / (len(values) - 1)) if len(values) > 1 else None
    return None


def _quantile(ordered: list[float], probability: float) -> float:
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def _duration_ns(value: Any) -> int:
    if not isinstance(value, str) or not value.endswith("ns") or not value[:-2].isdigit():
        raise ValueError("duration must be normalized nanoseconds")
    return int(value[:-2])


def _as_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("statistical scope times must be aware")
    return parsed.astimezone(UTC)


def _feature_ref(feature: dict[str, Any]) -> str | None:
    feature_id, version = feature.get("feature_id"), feature.get("version")
    return f"{feature_id}@{version}" if feature_id is not None and version is not None else None
