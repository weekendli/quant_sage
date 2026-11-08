"""Step 05-A trailing clock-grid profiling and basic descriptions."""

from __future__ import annotations

import bisect
import json
import math
import statistics
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from quantsage_discovery.core.errors import ArtifactValidationError

from .business_windows import BUSINESS_AUDIT_SCHEMA, BusinessOutput
from .errors import ResearchValidationError
from .gap_policy import (
    GAP_POLICY_VERSION,
    KNOWN_SOURCE_GAP,
    WindowGapContext,
    gap_event_ids,
    gap_evidence_record_ids,
    matching_gap_events,
)
from .providers import FEATURE_VALUES_SCHEMA, make_feature_values_table
from .statistical_backends import (
    CATCH22_NAMES,
    SHAPE_MIN_SAMPLES,
    TSFEATURES_METRICS,
    BackendMetricResult,
    BackendUnavailableError,
    catch22_metric_document,
    metric_backend_document,
    run_catch22_metric_detailed,
    run_tsfeature_metrics_detailed,
    tsfeature_metric_document,
)

PROVIDER_ID = "quantsage_statistical_windows"
PROVIDER_KEY = "builtin.statistical_windows"
CATALOG_VERSION = "2.0.0"
IMPLEMENTATION_VERSION = "statistical_windows_v4"
FORMULA_REF = "quantsage_statistical_descriptions@2"
INPUT_VIEW_POLICY = "step05_input_whitelist@2"
STATISTICAL_AUDIT_TYPE = "statistical_window_audit"
STATISTICAL_AUDIT_SCHEMA_VERSION = "statistical_window_audit_v2"
STATISTICAL_AUDIT_SCHEMA = BUSINESS_AUDIT_SCHEMA.append(pa.field("metric_id", pa.string()))
for _field in (
    pa.field("library_name", pa.string()),
    pa.field("library_metric_name", pa.string()),
    pa.field("library_version", pa.string()),
    pa.field("source_revision", pa.string()),
    pa.field("parameters_json", pa.string()),
    pa.field("input_unit", pa.string()),
    pa.field("output_unit", pa.string()),
    pa.field("backend_kind", pa.string()),
    pa.field("backend_elapsed_ms", pa.float64()),
    pa.field("backend_warnings", pa.list_(pa.string()), nullable=False),
    pa.field("n_unique", pa.int64(), nullable=False),
):
    STATISTICAL_AUDIT_SCHEMA = STATISTICAL_AUDIT_SCHEMA.append(_field)


def validate_statistical_audit_parquet(path: Any) -> None:
    try:
        parquet = pq.ParquetFile(path)
    except Exception as exc:
        raise ArtifactValidationError(
            f"cannot read statistical audit Parquet: {exc}"
        ) from exc
    if not parquet.schema_arrow.equals(STATISTICAL_AUDIT_SCHEMA, check_metadata=False):
        raise ArtifactValidationError(
            "statistical audit schema does not match statistical_window_audit_v2"
        )

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
_CATCH22_DESCRIPTIONS = {f"c22.{name}" for name in CATCH22_NAMES}
_TSFEATURE_COMMON = set(TSFEATURES_METRICS) - {"tsf.arch_r2_l12"}
_COMMON_SHAPE = _CATCH22_DESCRIPTIONS | _TSFEATURE_COMMON
_ARCH_SHAPE = {"tsf.arch_r2_l12"}
_ORIGINAL_UNIT_DESCRIPTIONS = _BASE_DESCRIPTIONS - {"unchanged_ratio"}


def _profile_metrics(base: set[str], *, signed: bool = False, arch: bool = False) -> set[str]:
    metrics = base | _COMMON_SHAPE
    if signed:
        metrics |= _SIGNED_DESCRIPTIONS - _BASE_DESCRIPTIONS - _NONNEGATIVE_DESCRIPTIONS
    if arch:
        metrics |= _ARCH_SHAPE
    return metrics

# This is deliberately a closed, semantic whitelist rather than a source-kind
# shortcut. Boolean controls, cumulative counters and static tick-size rows are
# not valid profile sequences; derived flow and movement views are allowed.
INPUT_VIEW_WHITELIST = {
    "primitive:mid": (
        "price_dynamics",
        "quoted_price_unit",
        frozenset(_ORIGINAL_UNIT_DESCRIPTIONS),
    ),
    "primitive:spread_ticks": (
        "price_dynamics", "ticks", _profile_metrics(_NONNEGATIVE_DESCRIPTIONS),
    ),
    "primitive:depth_bid_1": (
        "liquidity", "source_size_unit", _profile_metrics(_NONNEGATIVE_DESCRIPTIONS),
    ),
    "primitive:depth_ask_1": (
        "liquidity", "source_size_unit", _profile_metrics(_NONNEGATIVE_DESCRIPTIONS),
    ),
    "primitive:depth_imbalance_1": (
        "liquidity", "dimensionless", _profile_metrics(_SIGNED_DESCRIPTIONS, signed=True),
    ),
    "primitive:microprice_proxy": (
        "price_dynamics", "quoted_price_unit", _profile_metrics(_BASE_DESCRIPTIONS),
    ),
    "primitive:quote_interarrival": (
        "temporal_structure", "seconds", _profile_metrics(_NONNEGATIVE_DESCRIPTIONS),
    ),
    "primitive:obi_snapshot": (
        "F02", "dimensionless", _profile_metrics(_SIGNED_DESCRIPTIONS, signed=True),
    ),
    "primitive:wobi_snapshot": (
        "F03", "dimensionless", _profile_metrics(_SIGNED_DESCRIPTIONS, signed=True),
    ),
    "primitive:min_coverage_snapshot": (
        "F04", "dimensionless", _profile_metrics(_NONNEGATIVE_DESCRIPTIONS),
    ),
    "primitive:capacity_imb_snapshot": (
        "F05", "dimensionless", _profile_metrics(_SIGNED_DESCRIPTIONS, signed=True),
    ),
    "primitive:size_spread_snapshot": (
        "F06", "ticks", _profile_metrics(_NONNEGATIVE_DESCRIPTIONS),
    ),
    "primitive:fill_dist_asym_snapshot": (
        "F07", "dimensionless", _profile_metrics(_SIGNED_DESCRIPTIONS, signed=True),
    ),
    "primitive:walk_cost_asym_snapshot": (
        "F08", "dimensionless", _profile_metrics(_SIGNED_DESCRIPTIONS, signed=True),
    ),
    "primitive:visible_depth_bid_5": (
        "input", "source_size_unit", _profile_metrics(_NONNEGATIVE_DESCRIPTIONS),
    ),
    "primitive:visible_depth_ask_5": (
        "input", "source_size_unit", _profile_metrics(_NONNEGATIVE_DESCRIPTIONS),
    ),
    "primitive:depth_ref_5": (
        "input", "source_size_unit", _profile_metrics(_NONNEGATIVE_DESCRIPTIONS),
    ),
    "primitive:target_size_q25": (
        "input", "source_size_unit", _profile_metrics(_NONNEGATIVE_DESCRIPTIONS),
    ),
    "primitive:target_size_q50": (
        "input", "source_size_unit", _profile_metrics(_NONNEGATIVE_DESCRIPTIONS),
    ),
    "primitive:target_size_q100": (
        "input", "source_size_unit", _profile_metrics(_NONNEGATIVE_DESCRIPTIONS),
    ),
    "primitive:trade_volume_interval": (
        "input", "source_size_unit", _profile_metrics(_NONNEGATIVE_DESCRIPTIONS),
    ),
    "business:spread_ticks": (
        "F01", "ticks", _profile_metrics(_NONNEGATIVE_DESCRIPTIONS),
    ),
    "business:obi": (
        "F02", "dimensionless", _profile_metrics(_SIGNED_DESCRIPTIONS, signed=True),
    ),
    "business:wobi": (
        "F03", "dimensionless", _profile_metrics(_SIGNED_DESCRIPTIONS, signed=True),
    ),
    "business:min_coverage": (
        "F04", "dimensionless", _profile_metrics(_NONNEGATIVE_DESCRIPTIONS),
    ),
    "business:capacity_imb": (
        "F05", "dimensionless", _profile_metrics(_SIGNED_DESCRIPTIONS, signed=True),
    ),
    "business:size_spread": (
        "F06", "ticks", _profile_metrics(_NONNEGATIVE_DESCRIPTIONS),
    ),
    "business:fill_dist_asym": (
        "F07", "dimensionless", _profile_metrics(_SIGNED_DESCRIPTIONS, signed=True),
    ),
    "business:walk_cost_asym": (
        "F08", "dimensionless", _profile_metrics(_SIGNED_DESCRIPTIONS, signed=True),
    ),
    "business:active_flow_imb": (
        "F09", "dimensionless", _profile_metrics(_SIGNED_DESCRIPTIONS, signed=True, arch=True),
    ),
    "business:volume_to_depth": (
        "F10", "source_size_unit", _profile_metrics(_NONNEGATIVE_DESCRIPTIONS),
    ),
    "business:mid_move_ticks": (
        "F11", "ticks", _profile_metrics(_SIGNED_DESCRIPTIONS, signed=True, arch=True),
    ),
    "business:flow_impact_eff": (
        "F12", "dimensionless", _profile_metrics(_SIGNED_DESCRIPTIONS, signed=True, arch=True),
    ),
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
                "CONSTANT_INPUT", "NUMERICAL_FAILURE", KNOWN_SOURCE_GAP,
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
    for name in CATCH22_NAMES:
        backend = catch22_metric_document(f"c22.{name}")
        documents.append({
            "schema_version": "metric_v1",
            "metric_id": backend["metric_id"],
            "version": CATALOG_VERSION,
            "name": backend["name"],
            "description": (
                f"pycatch22 {backend['library_version']} canonical long-name metric."
            ),
            "formula_ref": f"pycatch22_{backend['library_version']}@1",
            "provider_ref": f"{PROVIDER_ID}@{CATALOG_VERSION}",
            "parameters_schema": {
                "type": "object", "properties": {}, "required": [],
                "additionalProperties": False,
            },
            "defaults": {},
            "output_schema": {"value": "float64"},
            "unit": backend["unit"],
            "status": "ready",
        })
    for metric_id in TSFEATURES_METRICS:
        backend = tsfeature_metric_document(metric_id)
        documents.append({
            "schema_version": "metric_v1",
            "metric_id": metric_id,
            "version": CATALOG_VERSION,
            "name": backend["name"],
            "description": (
                f"Fixed R tsfeatures {backend['library_version']} metric "
                f"{backend['library_metric_name']}."
            ),
            "formula_ref": f"tsfeatures_{backend['library_version']}@1",
            "provider_ref": f"{PROVIDER_ID}@{CATALOG_VERSION}",
            "parameters_schema": {
                "type": "object", "properties": {}, "required": [],
                "additionalProperties": False,
            },
            "defaults": {},
            "output_schema": {"value": "float64"},
            "unit": backend["unit"],
            "status": "ready",
        })
    return documents


def metric_set_document() -> dict[str, Any]:
    return {
        "schema_version": "metric_set_v1",
        "metric_set_id": "basic_description",
        "version": 3,
        "metrics": [f"stat.{name}@{CATALOG_VERSION}" for name in METRICS],
    }


def shape_metric_set_documents() -> list[dict[str, Any]]:
    catch22 = [f"c22.{name}@{CATALOG_VERSION}" for name in CATCH22_NAMES]
    tsfeatures = [
        f"{metric_id}@{CATALOG_VERSION}" for metric_id in TSFEATURES_METRICS
    ]
    return [
        {
            "schema_version": "metric_set_v1",
            "metric_set_id": "catch22_classic_22",
            "version": 1,
            "metrics": catch22,
        },
        {
            "schema_version": "metric_set_v1",
            "metric_set_id": "tsfeatures_selected_8",
            "version": 1,
            "metrics": tsfeatures,
        },
        {
            "schema_version": "metric_set_v1",
            "metric_set_id": "catch22_plus_selected_tsfeatures_30",
            "version": 1,
            "metrics": catch22 + tsfeatures,
        },
    ]


def register_statistical_catalog(service: Any) -> dict[str, Any]:
    provider = service.register_provider(provider_document())
    template = service.register_template(statistical_template_document())
    metrics = [service.register_metric(document) for document in metric_documents()]
    metric_set = service.register_metric_set(metric_set_document())
    shape_metric_sets = [
        service.register_metric_set(document) for document in shape_metric_set_documents()
    ]
    return {"provider": provider, "template": template,
            "metrics": metrics, "metric_set": metric_set,
            "shape_metric_sets": shape_metric_sets}


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
    input_feature: dict[str, Any] | None = None,
    input_audit: pa.Table | None = None,
    *,
    gap_context: dict[str, Any] | WindowGapContext | None = None,
) -> BusinessOutput:
    metric_ref = feature.get("metric_ref")
    metric_key = metric_ref.split("@", 1)[0] if metric_ref else None
    supported_metrics = (
        {f"stat.{name}" for name in METRICS}
        | {f"c22.{name}" for name in CATCH22_NAMES}
        | set(TSFEATURES_METRICS)
    )
    if metric_key not in supported_metrics:
        raise ValueError(f"unsupported statistical metric: {metric_ref}")
    if input_feature is not None:
        validate_statistical_input(input_feature, [metric_ref])
    context = _gap_context(gap_context)
    metric_id = metric_key.removeprefix("stat.")
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
    min_samples = int(feature.get("validity", {}).get("min_samples", 1))
    if metric_key.startswith(("c22.", "tsf.")) and min_samples < SHAPE_MIN_SAMPLES:
        raise ValueError(
            f"{metric_key} requires validity.min_samples >= {SHAPE_MIN_SAMPLES}"
        )
    rows = sorted(input_data["table"].to_pylist(), key=lambda row: row["event_time"])
    times = [row["event_time"] for row in rows]
    values = [row["value"] for row in rows]
    audit_by_time = (
        {row["event_time"]: row for row in input_audit.to_pylist()}
        if input_audit is not None and "event_time" in input_audit.column_names
        else {}
    )
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
    pending: list[tuple[int, list[float]]] = []
    reasons: Counter[str] = Counter()
    evidence = _audit_evidence(metric_key, feature, input_feature)
    for grid_time in grid:
        samples = []
        reason = None
        max_age_seen = None
        valid_sample_count = 0
        sample_gaps: tuple[dict[str, Any], ...] = ()
        inherited_gap_event_ids: set[str] = set()
        inherited_evidence_ids: set[str] = set()
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
            sample_gaps = _sample_gaps(
                context, partition, sample_time - interval, sample_time,
            )
            if sample_gaps:
                reason = KNOWN_SOURCE_GAP
                samples = []
                break
            if age > max_age:
                reason = "STALE_DATA"
                samples = []
                break
            value = values[index]
            if value is None or not math.isfinite(value):
                input_evidence = audit_by_time.get(times[index], {})
                if input_evidence.get("invalid_reason") == KNOWN_SOURCE_GAP:
                    reason = KNOWN_SOURCE_GAP
                    inherited_gap_event_ids.update(
                        input_evidence.get("source_gap_event_ids") or (),
                    )
                    inherited_evidence_ids.update(
                        input_evidence.get("source_gap_evidence_record_ids") or (),
                    )
                else:
                    reason = "WINDOW_NA"
                samples = []
                break
            samples.append(float(value))
            valid_sample_count += 1
            sample_time += interval
        if reason is None and len(samples) < min_samples:
            reason = "INSUFFICIENT_HISTORY"
        row = {
            "event_time": grid_time,
            "available_time": None,
            "instrument_id": rows[0]["instrument_id"] if rows else partition["instrument_id"],
            "trade_date": rows[0]["trade_date"] if rows else partition["trade_date"],
            "session_id": partition["session_id"],
            "value": None,
        }
        output_rows.append(row)
        audits.append({
            "event_time": grid_time,
            "instrument_id": row["instrument_id"],
            "trade_date": str(row["trade_date"]),
            "session_id": partition["session_id"],
            "window_start": grid_time - window,
            "n_total": expected,
            "n_valid": valid_sample_count,
            "valid_ratio": valid_sample_count / expected if expected else None,
            "invalid_reason": reason,
            "matched_buy_time": None, "matched_sell_time": None,
            "matched_mid_time": None, "matched_tick_time": None,
            "max_endpoint_age_seconds": (
                None if max_age_seen is None else max_age_seen.total_seconds()
            ),
            "warning_flags": [f"METRIC:{metric_id}"],
            "metric_id": evidence["metric_id"],
            "library_name": evidence["library_name"],
            "library_metric_name": evidence["library_metric_name"],
            "library_version": evidence["library_version"],
            "source_revision": evidence["source_revision"],
            "parameters_json": evidence["parameters_json"],
            "input_unit": evidence["input_unit"],
            "output_unit": evidence["output_unit"],
            "backend_kind": evidence["backend_kind"],
            "backend_elapsed_ms": None,
            "backend_warnings": [],
            "n_unique": len(set(samples)),
            "source_gap_event_ids": sorted({
                *gap_event_ids(sample_gaps), *inherited_gap_event_ids,
            }),
            "source_gap_evidence_record_ids": sorted({
                *gap_evidence_record_ids(sample_gaps), *inherited_evidence_ids,
            }),
            "gap_manifest_uri": (
                context.gap_manifest_uri if context is not None else None
            ),
            "gap_manifest_hash": (
                context.gap_manifest_hash if context is not None else None
            ),
            "gap_policy_version": GAP_POLICY_VERSION,
        })
        if context is not None:
            audits[-1]["warning_flags"] = sorted({
                *audits[-1]["warning_flags"],
                *context.warning_flags_for(("input",)),
            })
        if reason is None:
            pending.append((len(output_rows) - 1, samples))
        else:
            reasons[reason] += 1

    backend_warning = None
    if metric_key.startswith("c22."):
        try:
            for row_index, samples in pending:
                result = run_catch22_metric_detailed(metric_key, samples)
                _assign_metric_result(
                    output_rows[row_index], audits[row_index], result, reasons,
                )
        except BackendUnavailableError:
            backend_warning = "BACKEND_UNAVAILABLE"
            for row_index, _ in pending:
                _assign_metric_result(
                    output_rows[row_index], audits[row_index],
                    BackendMetricResult(
                        None, "NUMERICAL_FAILURE",
                        ("BACKEND_UNAVAILABLE",), 0.0,
                    ),
                    reasons,
                )
    elif metric_key.startswith("tsf."):
        try:
            results = run_tsfeature_metrics_detailed(
                metric_key, [samples for _, samples in pending],
            )
            for (row_index, _), result in zip(
                pending, results, strict=True,
            ):
                _assign_metric_result(
                    output_rows[row_index], audits[row_index], result, reasons,
                )
        except BackendUnavailableError:
            backend_warning = "BACKEND_UNAVAILABLE"
            for row_index, _ in pending:
                _assign_metric_result(
                    output_rows[row_index], audits[row_index],
                    BackendMetricResult(
                        None, "NUMERICAL_FAILURE",
                        ("BACKEND_UNAVAILABLE",), 0.0,
                    ),
                    reasons,
                )
    else:
        for row_index, samples in pending:
            started = time.perf_counter()
            value = _metric_value(metric_id, samples)
            elapsed_ms = (time.perf_counter() - started) * 1_000
            failure = None
            if value is not None and not math.isfinite(value):
                value, failure = None, "NUMERICAL_FAILURE"
            if value is None and failure is None:
                failure = (
                    "INSUFFICIENT_HISTORY"
                    if len(samples) < _metric_min_samples(metric_id)
                    else ("CONSTANT_INPUT" if metric_id == "std" else "NUMERICAL_FAILURE")
                )
            result = BackendMetricResult(value, failure, (), elapsed_ms)
            _assign_metric_result(
                output_rows[row_index], audits[row_index], result, reasons,
            )

    values_table = make_feature_values_table(output_rows) if output_rows else FEATURE_VALUES_SCHEMA.empty_table()
    audit_table = (STATISTICAL_AUDIT_SCHEMA.empty_table() if not audits else
                   pa.Table.from_pylist(audits, schema=STATISTICAL_AUDIT_SCHEMA))
    flags = []
    if not output_rows:
        flags.append("ZERO_OBSERVATIONS")
    elif all(row["value"] is None for row in output_rows):
        flags.append("ALL_NA")
    if backend_warning is not None:
        flags.append(backend_warning)
    return BusinessOutput(values_table, audit_table, dict(reasons), dict(reasons), flags)


def _assign_metric_result(
    row: dict[str, Any],
    audit: dict[str, Any],
    result: BackendMetricResult,
    reasons: Counter[str],
) -> None:
    audit["backend_elapsed_ms"] = result.elapsed_ms
    audit["backend_warnings"] = list(result.warnings)
    if result.value is not None:
        row["value"] = result.value
        audit["invalid_reason"] = None
        return
    reason = result.reason or "NUMERICAL_FAILURE"
    audit["invalid_reason"] = reason
    reasons[reason] += 1


def _audit_evidence(
    metric_key: str,
    feature: dict[str, Any],
    input_feature: dict[str, Any] | None,
) -> dict[str, Any]:
    if metric_key.startswith(("c22.", "tsf.")):
        document = metric_backend_document(metric_key)
        backend = document.get("backend", {})
        backend_kind = backend.get("kind", "python_subprocess")
        parameters = document.get("parameters", {})
    else:
        document = {
            "metric_id": metric_key,
            "library_name": "quantsage_python",
            "library_metric_name": metric_key,
            "library_version": IMPLEMENTATION_VERSION,
            "source_revision": "project",
        }
        backend_kind = "python"
        parameters = feature.get("parameters", {})
    return {
        "metric_id": document["metric_id"],
        "library_name": document["library_name"],
        "library_metric_name": document["library_metric_name"],
        "library_version": document["library_version"],
        "source_revision": document["source_revision"],
        "parameters_json": json.dumps(parameters, sort_keys=True, separators=(",", ":")),
        "input_unit": (input_feature or {}).get("unit"),
        "output_unit": feature.get("unit"),
        "backend_kind": backend_kind,
    }


def _metric_min_samples(metric_id: str) -> int:
    if metric_id in {"std", "unchanged_ratio"}:
        return 2
    return 1


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


def _gap_context(
    context: dict[str, Any] | WindowGapContext | None,
) -> WindowGapContext | None:
    if context is None:
        return None
    if isinstance(context, WindowGapContext):
        return context
    return WindowGapContext.from_document(context)


def _sample_gaps(
    context: WindowGapContext | None,
    partition: dict[str, Any],
    start: datetime,
    end: datetime,
) -> tuple[dict[str, Any], ...]:
    if context is None:
        return ()
    return matching_gap_events(
        context,
        roles=("input",),
        exchange=partition.get("exchange"),
        instrument_id=partition["instrument_id"],
        trade_date=str(partition["trade_date"]),
        session_id=partition["session_id"],
        start=start,
        end=end,
        include_start=False,
    )


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
