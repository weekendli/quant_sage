"""Trusted synthetic providers for execution-contract validation only.

These implementations exercise serialization, dependency wiring, lineage, and recovery.
They do not implement any government-bond futures formula.
"""

from __future__ import annotations

import math
from collections import Counter, deque
from datetime import UTC, datetime, timedelta
from itertools import groupby
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from quantsage_discovery.core.artifacts import artifact_path
from quantsage_discovery.core.errors import ArtifactValidationError
from quantsage_discovery.core.types import ArtifactRef

FEATURE_VALUES_SCHEMA_VERSION = "feature_values_v1"
FEATURE_VALUES_ARTIFACT_TYPE = "feature_values"
FEATURE_VALUES_SCHEMA = pa.schema([
    pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("available_time", pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("instrument_id", pa.string(), nullable=False),
    pa.field("trade_date", pa.string(), nullable=False),
    pa.field("session_id", pa.string(), nullable=False),
    pa.field("value", pa.float64(), nullable=True),
])


def validate_feature_values_parquet(path) -> None:
    """Validate the exact long-form numeric schema used by synthetic providers."""
    try:
        schema = pq.read_schema(path)
    except Exception as exc:
        raise ArtifactValidationError(f"cannot read feature-value Parquet schema: {exc}") from exc
    if schema.names != FEATURE_VALUES_SCHEMA.names:
        raise ArtifactValidationError(
            f"feature-value columns must be {FEATURE_VALUES_SCHEMA.names!r}, found {schema.names!r}"
        )
    for actual, expected in zip(schema, FEATURE_VALUES_SCHEMA, strict=True):
        if actual.type != expected.type:
            raise ArtifactValidationError(
                f"feature-value column {expected.name!r} must be {expected.type}, found {actual.type}"
            )
    try:
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(
            batch_size=65_536,
            columns=["event_time", "instrument_id", "trade_date", "session_id", "value"],
        ):
            for name in ("event_time", "instrument_id", "trade_date", "session_id"):
                if batch.column(name).null_count:
                    raise ArtifactValidationError(
                        f"feature-value column {name!r} cannot contain nulls"
                    )
            if any(value is not None and not math.isfinite(value)
                   for value in batch.column("value").to_pylist()):
                raise ArtifactValidationError("feature values must be finite numbers or null")
    except ArtifactValidationError:
        raise
    except Exception as exc:
        raise ArtifactValidationError(f"cannot read feature-value Parquet rows: {exc}") from exc


def validate_feature_result_payload(refs: list[ArtifactRef], result: dict[str, Any]) -> None:
    """Verify actual serialized observations against the declared result identity and quality."""
    scope = result["scope"]
    start, end = _timestamp(scope["start"]), _timestamp(scope["end"])
    n_total = n_valid = 0
    for ref in refs:
        path = artifact_path(ref.uri)
        validate_feature_values_parquet(path)
        parquet = pq.ParquetFile(path)
        count = 0
        first = last = None
        for batch in parquet.iter_batches(batch_size=65_536):
            for row in batch.to_pylist():
                time = row["event_time"]
                if not start <= time < end:
                    raise ArtifactValidationError("result contains observations outside its scope")
                if any(row[field] != scope[field]
                       for field in ("instrument_id", "trade_date", "session_id")):
                    raise ArtifactValidationError("result observation partition does not match scope")
                first = time if first is None else min(first, time)
                last = time if last is None else max(last, time)
                count += 1
                n_valid += row["value"] is not None
        if ref.row_count != count:
            raise ArtifactValidationError("artifact row_count does not match actual Parquet rows")
        if ((first is None and (ref.time_min is not None or ref.time_max is not None))
                or (first is not None and (
                    ref.time_min is None or ref.time_max is None
                    or _timestamp(ref.time_min) != first or _timestamp(ref.time_max) != last
                ))):
            raise ArtifactValidationError("artifact time bounds do not match actual observations")
        n_total += count
    quality = result["quality"]
    if quality["n_total"] != n_total or quality["n_valid"] != n_valid:
        raise ArtifactValidationError("FeatureResult quality counts do not match actual observations")


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ArtifactValidationError("result time bounds must be timezone-aware")
    return parsed.astimezone(UTC)


def make_feature_values_table(rows: list[dict[str, Any]]) -> pa.Table:
    """Build a typed table without inferring time or numeric field types from rows."""
    arrays = []
    for field in FEATURE_VALUES_SCHEMA:
        values = [row.get(field.name) for row in rows]
        if field.name in {"event_time", "available_time"}:
            values = [
                None if value is None else value.astimezone(UTC).replace(tzinfo=UTC)
                for value in values
            ]
        arrays.append(pa.array(values, type=field.type))
    return pa.Table.from_arrays(arrays, schema=FEATURE_VALUES_SCHEMA)


def normalize_feature_values_table(table: pa.Table, scope: dict[str, Any]) -> pa.Table:
    """Normalize a raw/dependency value column into the fixed synthetic result schema."""
    if "event_time" not in table.column_names or "value" not in table.column_names:
        raise ArtifactValidationError("synthetic inputs require event_time and value columns")
    time_type = table.schema.field("event_time").type
    if not pa.types.is_timestamp(time_type) or not time_type.tz:
        raise ArtifactValidationError("synthetic event_time must be a timezone-aware timestamp")
    value_type = table.schema.field("value").type
    if not (pa.types.is_integer(value_type) or pa.types.is_floating(value_type)):
        raise ArtifactValidationError("synthetic value must be an integer or floating-point column")
    if "available_time" in table.column_names:
        available_type = table.schema.field("available_time").type
        if not pa.types.is_timestamp(available_type) or not available_type.tz:
            raise ArtifactValidationError("available_time must be a timezone-aware timestamp")

    event_times = table["event_time"].cast(pa.timestamp("us", tz="UTC")).to_pylist()
    raw_available = (table["available_time"].cast(pa.timestamp("us", tz="UTC")).to_pylist()
                     if "available_time" in table.column_names else [None] * table.num_rows)
    values = table["value"].cast(pa.float64()).to_pylist()
    rows = []
    for index, (event_time, available_time, value) in enumerate(
        zip(event_times, raw_available, values, strict=True)
    ):
        if event_time is None:
            raise ArtifactValidationError(f"synthetic event_time is null at row {index}")
        rows.append({
            "event_time": event_time,
            "available_time": available_time,
            "instrument_id": (table["instrument_id"][index].as_py()
                              if "instrument_id" in table.column_names
                              else scope["instrument_id"]),
            "trade_date": (table["trade_date"][index].as_py()
                           if "trade_date" in table.column_names
                           else scope["trade_date"]),
            "session_id": (table["session_id"][index].as_py()
                           if "session_id" in table.column_names
                           else scope["session_id"]),
            "value": value,
        })
    return make_feature_values_table(rows)


def transform_values(provider_key: str, feature: dict[str, Any],
                     inputs: pa.Table) -> tuple[pa.Table, dict[str, int]]:
    """Apply an explicitly synthetic identity or arithmetic transformation."""
    if provider_key not in {"builtin.arithmetic", "builtin.identity"}:
        raise ArtifactValidationError(f"untrusted or unsupported provider key: {provider_key}")
    rows = inputs.to_pylist()
    reasons: Counter[str] = Counter()
    normalized_values: list[float | None] = []
    for row in rows:
        value = row["value"]
        if value is None:
            normalized_values.append(None)
        elif not math.isfinite(value):
            normalized_values.append(None)
            reasons["NONFINITE_INPUT"] += 1
        else:
            normalized_values.append(float(value))

    if provider_key == "builtin.identity":
        return make_feature_values_table([
            {**row, "value": normalized_values[index]} for index, row in enumerate(rows)
        ]), dict(reasons)

    parameters = feature.get("parameters", {})
    scale = _finite_number(parameters.get("scale", 1.0), "scale")
    offset = _finite_number(parameters.get("offset", 0.0), "offset")
    feature_kind = feature.get("kind")
    metric_ref = feature.get("metric_ref") or parameters.get("metric_ref", "")
    metric_id = metric_ref.split("@", 1)[0].lower() if isinstance(metric_ref, str) else ""

    if feature_kind == "statistical":
        if metric_id not in {"metric_mean", "metric_max"}:
            raise ArtifactValidationError(f"unsupported synthetic statistical metric: {metric_ref}")
        return _trailing_statistics(rows, normalized_values, feature, metric_id, scale, offset)

    output = []
    for index, row in enumerate(rows):
        value = normalized_values[index]
        if value is not None:
            value = value * scale + offset
            if not math.isfinite(value):
                value = None
                reasons["NONFINITE_OUTPUT"] += 1
        output.append({**row, "value": value})
    return make_feature_values_table(output), dict(reasons)


def _trailing_statistics(rows, values, feature, metric_id, scale, offset):
    """Synthetic event-anchor statistics; production grid sampling belongs to Step 05."""
    window = feature.get("time_policy", {}).get("profile_window")
    if (not isinstance(window, str) or not window.endswith("ns")
            or not window[:-2].isdigit() or int(window[:-2]) < 1):
        raise ArtifactValidationError("synthetic trailing statistics need a frozen profile_window")
    duration = timedelta(microseconds=(int(window[:-2]) + 999) // 1_000)
    minimum = feature.get("validity", {}).get("min_samples", 1)
    indexed = [({**row, "value": value}, index)
               for index, (row, value) in enumerate(zip(rows, values, strict=True))]

    def partition(entry):
        row = entry[0]
        return row["instrument_id"], row["trade_date"], row["session_id"]

    indexed.sort(key=lambda entry: (*partition(entry), entry[0]["event_time"], entry[1]))
    output = []
    reasons: Counter[str] = Counter()
    for _, entries in groupby(indexed, key=partition):
        active = deque()
        for event_time, peers in groupby(entries, key=lambda entry: entry[0]["event_time"]):
            peers = list(peers)
            active.extend(row for row, _ in peers)
            while active and active[0]["event_time"] <= event_time - duration:
                active.popleft()
            reason = None
            result = None
            if any(row["value"] is None for row in active):
                reason = "WINDOW_HAS_NA"
            elif len(active) < minimum:
                reason = "INSUFFICIENT_SAMPLES"
            else:
                try:
                    result = (math.fsum(row["value"] for row in active) / len(active)
                              if "mean" in metric_id else
                              max(row["value"] for row in active))
                    result = result * scale + offset
                    if not math.isfinite(result):
                        reason, result = "NONFINITE_OUTPUT", None
                except OverflowError:
                    reason = "NONFINITE_OUTPUT"
            # Unknown contributor availability stays unknown; event_time is
            # never substituted. Preserve the latest actual contributing time.
            available = (max(row["available_time"] for row in active)
                         if all(row["available_time"] is not None for row in active) else None)
            for anchor, _ in peers:
                output.append({**anchor, "value": result, "available_time": available})
                if reason:
                    reasons[reason] += 1
    return make_feature_values_table(output), dict(reasons)


def _finite_number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise ArtifactValidationError(f"synthetic parameter {name!r} must be a finite number")
    return float(value)
