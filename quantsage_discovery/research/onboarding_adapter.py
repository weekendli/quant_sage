"""Map a frozen Step 02 onboarding entry to an immutable research DataSource.

The adapter treats the published onboarding entry and its manifests as the
source of truth. It does not rewrite normalized quotes, quarantine rows,
calendar data, or session evidence. Instead it creates a bounded source index,
a versioned research-only access manifest, and explicit gap/continuity evidence
that downstream stage providers can consume by reference.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from itertools import pairwise
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from quantsage_discovery.core.utils import canonical_json
from quantsage_discovery.ingest.calendar import sha256_file
from quantsage_discovery.ingest.sessions import (
    SessionRules,
    session_segment_bounds,
)

from .errors import ResearchValidationError
from .schemas import (
    validate_gap_manifest_document,
    validate_market_metadata_document,
    validate_source_document,
)

ADAPTER_SCHEMA_VERSION = "onboarding_data_source_adapter_v1"
SOURCE_INDEX_SCHEMA_VERSION = "l2_onboarding_source_index_v1"
GAP_EVENTS_SCHEMA_VERSION = "gap_events_v1"
QUALITY_SUMMARY_SCHEMA_VERSION = "onboarding_quality_summary_v1"
GAP_EVENT_SCHEMA = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("event_kind", pa.string(), nullable=False),
    pa.field("certainty", pa.string(), nullable=False),
    pa.field("exchange", pa.string(), nullable=False),
    pa.field("instrument_id", pa.string()),
    pa.field("trade_date", pa.date32()),
    pa.field("session_id", pa.string()),
    pa.field("start_time", pa.timestamp("us", tz="UTC")),
    pa.field("end_time", pa.timestamp("us", tz="UTC")),
    pa.field("boundary", pa.string(), nullable=False),
    pa.field("reason_codes", pa.list_(pa.string()), nullable=False),
    pa.field("affected_fields", pa.list_(pa.string()), nullable=False),
    pa.field("evidence_uri", pa.string(), nullable=False),
    pa.field("evidence_sha256", pa.string(), nullable=False),
    pa.field("evidence_record_id", pa.string()),
    pa.field("raw_row_index", pa.int64()),
    pa.field("raw_row_group_index", pa.int32()),
    pa.field("row_count", pa.int64(), nullable=False),
])
SOURCE_INTERVAL_FIELDS = {
    "volume": "TRADEVOLUME",
    "turnover": "TRADEAMOUNT",
    "cumulative_volume": "TOTALVOLUME",
    "cumulative_turnover": "TOTALAMOUNT",
}


class OnboardingAdapterError(ResearchValidationError):
    """An onboarding entry cannot be safely adapted to a research DataSource."""


@dataclass(frozen=True)
class AdaptedOnboardingSource:
    """Files created for one immutable DataSource version."""

    source_document: dict[str, Any]
    source_document_path: Path
    output_directory: Path
    source_index_path: Path
    gap_manifest_path: Path
    gap_events_path: Path
    summary: dict[str, Any]


@dataclass(frozen=True)
class _SessionInterval:
    exchange: str
    instrument_id: str
    trade_date: date
    session_id: str
    start: datetime
    end: datetime


def _fail(path: str, message: str) -> OnboardingAdapterError:
    return OnboardingAdapterError(f"{path}: {message}", details={"path": path})


def _json_bytes(value: Any) -> bytes:
    return (canonical_json(value) + "\n").encode("utf-8")


def _write_json(path: Path, value: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_json_bytes(value))
    return path


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise _fail(label, f"cannot read valid JSON from {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise _fail(label, "must contain a JSON object")
    return value


def _uri_path(uri: str, label: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise _fail(label, f"only local file:// references are supported: {uri}")
    return Path(unquote(parsed.path)).resolve()


def _ref_path(ref: dict[str, Any], label: str) -> Path:
    if not isinstance(ref, dict) or not isinstance(ref.get("uri"), str):
        raise _fail(label, "must contain a file URI")
    return _uri_path(ref["uri"], label)


def _file_ref(path: Path) -> dict[str, str]:
    resolved = path.resolve()
    return {"uri": resolved.as_uri(), "content_hash": sha256_file(resolved)}


def _bare_ref(ref: dict[str, Any]) -> dict[str, str]:
    return {
        "uri": _require_text(ref.get("uri"), "artifact_ref.uri"),
        "content_hash": _require_text(ref.get("content_hash"), "artifact_ref.content_hash"),
    }


def _verify_ref(ref: dict[str, Any], label: str) -> Path:
    path = _ref_path(ref, label)
    if not path.is_file():
        raise _fail(label, f"referenced file does not exist: {path}")
    digest = ref.get("content_hash", ref.get("sha256"))
    if not isinstance(digest, str) or sha256_file(path) != digest:
        raise _fail(label, "content hash changed or does not match the frozen reference")
    return path


def _entry_ref(entry_path: Path, entry_manifest_path: Path) -> tuple[dict[str, str], dict[str, Any]]:
    entry = _file_ref(entry_path)
    outer = _read_json(entry_manifest_path, "entry manifest")
    if outer.get("artifact_type") not in {"onboarding_entry", "onboarding.json"}:
        raise _fail("entry manifest.artifact_type", "must identify an onboarding entry")
    if outer.get("uri") != entry["uri"]:
        raise _fail("entry manifest.uri", "does not reference the supplied onboarding entry")
    if outer.get("content_hash") != entry["content_hash"]:
        raise _fail("entry manifest.content_hash", "does not match the onboarding entry")
    return entry, outer


def _resolve_relative_path(entry_path: Path, value: str) -> Path:
    parsed = urlparse(value)
    if parsed.scheme:
        return _uri_path(value, "inputs.schema_ref")
    candidate = Path(value).expanduser()
    choices = [candidate] if candidate.is_absolute() else [Path.cwd() / candidate,
                                                          entry_path.parent / candidate]
    for item in choices:
        if item.is_file():
            return item.resolve()
    raise _fail("inputs.schema_ref", f"cannot resolve source schema: {value}")


def _require_text(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(path, "must be a non-empty string")
    return value


def _require_int(value: Any, path: str) -> int:
    if type(value) is not int or value < 0:
        raise _fail(path, "must be a non-negative integer")
    return value


def _normalized_schema(
    entry: dict[str, Any],
    entry_path: Path,
) -> tuple[dict[str, Any], Path]:
    inputs = entry.get("inputs")
    if not isinstance(inputs, dict):
        raise _fail("inputs", "must be an object")
    schema_ref = _require_text(inputs.get("schema_ref"), "inputs.schema_ref")
    schema_path = _resolve_relative_path(entry_path, schema_ref)
    expected_hash = _require_text(inputs.get("schema_sha256"), "inputs.schema_sha256")
    if sha256_file(schema_path) != expected_hash:
        raise _fail("inputs.schema_sha256", "source schema content changed")
    schema = _read_json(schema_path, "source schema")
    raw_fields = schema.get("sample", {}).get("fields")
    mapping = inputs.get("mapping")
    if not isinstance(raw_fields, list) or not raw_fields:
        raise _fail("source schema", "must define sample.fields")
    if not isinstance(mapping, dict):
        raise _fail("inputs.mapping", "must be an object")

    fields: dict[str, str] = {}
    for index, raw in enumerate(raw_fields):
        if not isinstance(raw, dict):
            raise _fail(f"source schema fields[{index}]", "must be an object")
        name = _require_text(raw.get("name"), f"source schema fields[{index}].name")
        dtype = _require_text(raw.get("arrow_type"), f"source schema fields[{index}].arrow_type")
        target = mapping.get(name, name)
        if not isinstance(target, str) or not target.strip():
            raise _fail(f"inputs.mapping.{name}", "must map to a non-empty field name")
        if target == "event_time":
            dtype = "timestamp[us, tz=UTC]"
        fields[target] = dtype

    derived = {
        "record_id": "string",
        "raw_file_sha256": "string",
        "raw_row_index": "int64",
        "raw_row_group_index": "int32",
        "session_id": "string",
        "available_time": "timestamp[us, tz=UTC]",
        "quality_flags": "list<string>",
    }
    collision = sorted(set(fields) & set(derived))
    if collision:
        raise _fail("source schema mapping", f"creates duplicate derived fields: {collision}")
    fields.update(derived)
    required = {
        "event_time", "trade_date", "instrument_id", "exchange",
        "bid_price_1", "ask_price_1", "bid_size_1", "ask_size_1",
        "raw_file_sha256", "raw_row_index", "session_id", "quality_flags",
    }
    missing = sorted(required - set(fields))
    if missing:
        raise _fail("source schema mapping", f"missing normalized quote fields: {missing}")
    if not fields["event_time"].startswith("timestamp[us, tz=UTC]"):
        raise _fail("source schema mapping.event_time", "must be UTC timestamp[us]")
    return {"schema_version": "source_schema_v1", "fields": fields}, schema_path


def _artifact_refs(entry: dict[str, Any]) -> tuple[list[dict[str, Any]],
                                                   list[dict[str, Any]],
                                                   dict[str, Any],
                                                   dict[str, Any]]:
    artifacts = entry.get("artifacts")
    if not isinstance(artifacts, dict):
        raise _fail("artifacts", "must be an object")
    quotes = artifacts.get("quotes")
    quarantine = artifacts.get("quarantine")
    calendar = artifacts.get("calendar")
    sessions = artifacts.get("session_rules")
    if not isinstance(quotes, list) or not quotes:
        raise _fail("artifacts.quotes", "must be a non-empty array")
    if not isinstance(quarantine, list):
        raise _fail("artifacts.quarantine", "must be an array")
    if not isinstance(calendar, dict) or not isinstance(sessions, dict):
        raise _fail("artifacts", "calendar and session_rules must be objects")
    return quotes, quarantine, calendar, sessions


def _calendar(
    calendar_ref: dict[str, Any],
    provenance: dict[str, Any],
) -> tuple[dict[str, Any], list[date]]:
    path = _verify_ref(calendar_ref, "artifacts.calendar")
    try:
        parquet = pq.ParquetFile(path)
        if parquet.schema_arrow.names != ["calendar_id", "trade_date"]:
            raise _fail("artifacts.calendar", "must contain calendar_id and trade_date")
        table = parquet.read()
    except OnboardingAdapterError:
        raise
    except Exception as exc:
        raise _fail("artifacts.calendar", f"cannot read calendar Parquet: {exc}") from exc
    rows = table.to_pylist()
    if not rows:
        raise _fail("artifacts.calendar", "calendar cannot be empty")
    calendar_ids = {row["calendar_id"] for row in rows}
    if len(calendar_ids) != 1:
        raise _fail("artifacts.calendar", "calendar snapshot must contain exactly one calendar_id")
    calendar_id = next(iter(calendar_ids))
    days = [row["trade_date"] for row in rows]
    if any(not isinstance(day, date) for day in days):
        raise _fail("artifacts.calendar", "trade_date must use Arrow date32")
    if len(days) != len(set(days)):
        raise _fail("artifacts.calendar", "duplicate calendar dates are not allowed")
    if any(previous >= current for previous, current in pairwise(days)):
        raise _fail("artifacts.calendar", "trade dates must be strictly increasing")
    if provenance.get("row_count") is not None and int(provenance["row_count"]) != len(days):
        raise _fail("inputs.calendar.provenance.row_count", "does not match calendar.parquet")
    if provenance.get("calendar_id") not in {None, calendar_id}:
        raise _fail("inputs.calendar.provenance.calendar_id", "does not match calendar.parquet")
    value = {
        "schema_version": "calendar_v1",
        "calendar_id": calendar_id,
        "timezone": _require_text(provenance.get("timezone", "Asia/Shanghai"),
                                  "inputs.calendar.provenance.timezone"),
        "trading_dates": [day.isoformat() for day in days],
    }
    return value, days


def _session_ids_by_partition(quality: dict[str, Any]) -> dict[tuple[str, str, str], list[str]]:
    scopes = quality.get("session_scopes")
    if not isinstance(scopes, list):
        raise _fail("quality_report.session_scopes", "must be an array")
    result: dict[tuple[str, str, str], set[str]] = {}
    for index, item in enumerate(scopes):
        if not isinstance(item, dict):
            raise _fail(f"quality_report.session_scopes[{index}]", "must be an object")
        accepted = _require_int(item.get("accepted_rows", 0),
                                f"quality_report.session_scopes[{index}].accepted_rows")
        if not accepted:
            continue
        exchange = _require_text(item.get("exchange"),
                                 f"quality_report.session_scopes[{index}].exchange")
        instrument = _require_text(item.get("instrument_id"),
                                   f"quality_report.session_scopes[{index}].instrument_id")
        trade_day = _require_text(item.get("trade_date"),
                                  f"quality_report.session_scopes[{index}].trade_date")
        session_id = _require_text(item.get("session_id"),
                                   f"quality_report.session_scopes[{index}].session_id")
        result.setdefault((exchange, instrument, trade_day), set()).add(session_id)
    return {key: sorted(values) for key, values in result.items()}


def _session_policy(
    entry: dict[str, Any],
    partition_sessions: dict[tuple[str, str, str], list[str]],
    calendar_id: str,
    calendar_dates: list[date],
    session_rules: SessionRules,
) -> tuple[dict[str, Any], list[_SessionInterval]]:
    intervals: list[_SessionInterval] = []
    for (exchange, instrument, trade_day), session_ids in sorted(partition_sessions.items()):
        day = date.fromisoformat(trade_day)
        for session_id in session_ids:
            prefix = f"{calendar_id}:{trade_day}:"
            if not session_id.startswith(prefix):
                raise _fail("quality_report.session_scopes.session_id",
                            f"session id does not match its trade date: {session_id}")
            segment = session_id.removeprefix(prefix)
            bounds = session_segment_bounds(
                instrument, day, segment, calendar_dates, session_rules,
            )
            if bounds is None:
                raise _fail(
                    "session_rules",
                    f"no evidenced interval for {instrument} {trade_day} {segment}",
                )
            intervals.append(_SessionInterval(
                exchange=exchange,
                instrument_id=instrument,
                trade_date=day,
                session_id=session_id,
                start=bounds[0],
                end=bounds[1],
            ))
    if not intervals:
        raise _fail("quality_report.session_scopes", "no accepted session scopes were published")
    unique: dict[tuple[str, str, date, str], _SessionInterval] = {}
    for item in intervals:
        unique[(item.exchange, item.instrument_id, item.trade_date, item.session_id)] = item
    ordered = sorted(
        unique.values(),
        key=lambda item: (item.exchange, item.instrument_id, item.trade_date, item.start, item.session_id),
    )
    policy = {
        "schema_version": "session_policy_v1",
        "session_policy_id": (
            "onboarding_session_policy_" + entry["artifacts"]["session_rules"]["content_hash"][7:19]
        ),
        "sessions": sorted({item.session_id for item in ordered}),
        "intervals": [{
            "exchange": item.exchange,
            "instrument_id": item.instrument_id,
            "trade_date": item.trade_date.isoformat(),
            "session_id": item.session_id,
            "start": item.start.isoformat(),
            "end": item.end.isoformat(),
        } for item in ordered],
    }
    return policy, ordered


def _coverage(
    entry: dict[str, Any],
    partition_sessions: dict[tuple[str, str, str], list[str]],
) -> tuple[list[str], dict[str, Any], list[dict[str, Any]]]:
    partitions = entry.get("partitions")
    if not isinstance(partitions, list) or not partitions:
        raise _fail("partitions", "must be a non-empty array")
    normalized = []
    instruments: set[str] = set()
    seen: set[tuple[str, str, str]] = set()
    for index, item in enumerate(partitions):
        path = f"partitions[{index}]"
        if not isinstance(item, dict):
            raise _fail(path, "must be an object")
        exchange = _require_text(item.get("exchange"), f"{path}.exchange")
        instrument = _require_text(item.get("instrument_id"), f"{path}.instrument_id")
        trade_day = _require_text(item.get("trade_date"), f"{path}.trade_date")
        key = (exchange, instrument, trade_day)
        if key in seen:
            raise _fail(path, "duplicate exchange/instrument/trade_date partition")
        seen.add(key)
        sessions = partition_sessions.get(key)
        if not sessions:
            raise _fail(path, "accepted rows have no evidenced session mapping")
        instruments.add(instrument)
        normalized.append({
            "exchange": exchange,
            "instrument_id": instrument,
            "trade_date": trade_day,
            "session_ids": sessions,
        })
    normalized.sort(key=lambda item: (item["exchange"], item["instrument_id"], item["trade_date"]))
    return sorted(instruments), {"partitioning": "trade_date_v1", "partitions": normalized}, normalized


def _research_split(
    original_split: dict[str, Any],
    *,
    data_snapshot_id: str,
    source_timezone: str,
    instruments: list[str],
    sessions: list[str],
    start_day: date,
    end_day: date,
    original_ref: dict[str, str],
) -> dict[str, Any]:
    local_zone = ZoneInfo(source_timezone)
    start = datetime.combine(start_day, time.min, tzinfo=local_zone).astimezone(UTC)
    end = datetime.combine(end_day + timedelta(days=1), time.min, tzinfo=local_zone).astimezone(UTC)
    identity = {
        "source_snapshot": data_snapshot_id,
        "original_split": original_ref,
        "instruments": instruments,
        "sessions": sessions,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "purpose": "descriptive_research_only",
    }
    split_id = "split_" + hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()
    return {
        "schema_version": "split_manifest_v1",
        "split_id": split_id,
        "status": "UNASSIGNED",
        "method": "unassigned_research_only",
        "role": "research_only",
        "source_data_snapshot_id": data_snapshot_id,
        "source_split_ref": original_ref,
        "source_date_range": original_split.get("source_date_range"),
        "read_ranges": [{
            "instruments": instruments,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "session_filter": sessions,
        }],
        "fitting_allowed": False,
        "hypothesis_stages_allowed": False,
        "allowed_stages": [3, 4, 5],
        "notes": [
            "Derived solely to authorize descriptive Steps 03-05 over the frozen source.",
            "This manifest assigns no train/development/holdout role and permits no fitting.",
            "The original unassigned Step 02 split remains the immutable provenance record.",
        ],
    }


def _quality_summary(
    entry: dict[str, Any],
    reconciliation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    quality = entry.get("quality_report")
    passport = entry.get("passport")
    if not isinstance(quality, dict) or not isinstance(passport, dict):
        raise _fail("onboarding entry", "quality_report and passport must be objects")
    summary = {
        "schema_version": QUALITY_SUMMARY_SCHEMA_VERSION,
        "data_snapshot_id": entry["data_snapshot_id"],
        "split_id": entry.get("split_id"),
        "input_rows": quality.get("input_rows"),
        "accepted_rows": quality.get("accepted_rows"),
        "quarantined_rows": quality.get("quarantined_rows"),
        "quarantine_reason_counts": quality.get("quarantine_reason_counts", {}),
        "quality_flag_counts": quality.get("quality_flag_counts", {}),
        "cumulative_reset_rows": quality.get("cumulative_reset_rows", 0),
        "volume_turnover_increment_mismatch_rows": quality.get(
            "volume_turnover_increment_mismatch_rows",
        ),
        "volume_increment_mismatch_rows": quality.get("volume_increment_mismatch_rows"),
        "turnover_increment_mismatch_rows": quality.get("turnover_increment_mismatch_rows"),
        "calendar_dates_without_source_rows": quality.get(
            "calendar_dates_without_source_rows", quality.get("expected_missing_dates", []),
        ),
        "rules": quality.get("rules", []),
        "duplicate_classification": quality.get("duplicate_classification", {}),
        "input_constraints": passport.get("input_constraints", {}),
        "instrument_metadata": passport.get("instrument_metadata", []),
        "availability_status": passport.get("availability_status"),
        "research_only": passport.get("research_only"),
        "warnings": entry.get("warnings", []),
    }
    if reconciliation is not None:
        summary.update({
            "volume_increment_mismatch_rows": reconciliation["volume_increment_mismatch_rows"],
            "turnover_increment_mismatch_rows": reconciliation["turnover_increment_mismatch_rows"],
            "volume_cumulative_reset_rows": reconciliation["volume_cumulative_reset_rows"],
            "turnover_cumulative_reset_rows": reconciliation["turnover_cumulative_reset_rows"],
        })
    return summary


def _increment_reconciliation(entry: dict[str, Any]) -> dict[str, Any]:
    """Recompute volume and turnover reconciliation separately from frozen raw files."""
    raw_files = entry.get("inputs", {}).get("raw_files")
    if not isinstance(raw_files, list) or not raw_files:
        raise _fail("inputs.raw_files", "increment reconciliation requires raw file refs")
    totals = {
        "volume_increment_mismatch_rows": 0,
        "volume_cumulative_reset_rows": 0,
        "turnover_increment_mismatch_rows": 0,
        "turnover_cumulative_reset_rows": 0,
        "cumulative_reset_rows": 0,
    }
    query = (
        "WITH ordered AS (SELECT ingest_run_id, source_file, SYMBOL, TRADINGDATE, "
        "file_row_number::BIGINT AS raw_row_index, TOTALVOLUME, TOTALAMOUNT, "
        "TRADEVOLUME, TRADEAMOUNT, "
        "lag(TOTALVOLUME) OVER (PARTITION BY ingest_run_id, source_file, SYMBOL, "
        "TRADINGDATE ORDER BY raw_row_index) AS prior_volume, "
        "lag(TOTALAMOUNT) OVER (PARTITION BY ingest_run_id, source_file, SYMBOL, "
        "TRADINGDATE ORDER BY raw_row_index) AS prior_amount "
        "FROM read_parquet(?, file_row_number=true, hive_partitioning=false)) "
        "SELECT count(*) FILTER (WHERE prior_volume IS NOT NULL AND "
        "TOTALVOLUME >= prior_volume AND TRADEVOLUME IS NOT NULL AND "
        "TOTALVOLUME - prior_volume <> TRADEVOLUME), "
        "count(*) FILTER (WHERE prior_volume IS NOT NULL AND TOTALVOLUME < prior_volume), "
        "count(*) FILTER (WHERE prior_amount IS NOT NULL AND "
        "TOTALAMOUNT >= prior_amount AND TRADEAMOUNT IS NOT NULL AND "
        "TOTALAMOUNT - prior_amount <> TRADEAMOUNT), "
        "count(*) FILTER (WHERE prior_amount IS NOT NULL AND TOTALAMOUNT < prior_amount), "
        "count(*) FILTER (WHERE (prior_volume IS NOT NULL AND TOTALVOLUME < prior_volume) "
        "OR (prior_amount IS NOT NULL AND TOTALAMOUNT < prior_amount)) "
        "FROM ordered"
    )
    source_refs = []
    with duckdb.connect(":memory:") as connection:
        connection.execute("SET memory_limit='2GB'")
        connection.execute("SET threads=2")
        for index, raw in enumerate(raw_files):
            path = _ref_path(raw, f"inputs.raw_files[{index}]")
            if not path.is_file() or sha256_file(path) != raw["sha256"]:
                raise _fail(
                    f"inputs.raw_files[{index}]",
                    "raw source file is missing or its frozen hash changed",
                )
            values = connection.execute(query, [path.as_posix()]).fetchone()
            (
                volume_mismatch,
                volume_reset,
                turnover_mismatch,
                turnover_reset,
                cumulative_reset,
            ) = (int(value or 0) for value in values)
            totals["volume_increment_mismatch_rows"] += volume_mismatch
            totals["volume_cumulative_reset_rows"] += volume_reset
            totals["turnover_increment_mismatch_rows"] += turnover_mismatch
            totals["turnover_cumulative_reset_rows"] += turnover_reset
            totals["cumulative_reset_rows"] += cumulative_reset
            source_refs.append({
                "uri": raw["uri"],
                "content_hash": raw["sha256"],
            })
    frozen_combined = entry["quality_report"].get("volume_turnover_increment_mismatch_rows")
    computed_combined = (
        totals["volume_increment_mismatch_rows"]
        + totals["turnover_increment_mismatch_rows"]
    )
    if frozen_combined is not None and int(frozen_combined) != computed_combined:
        raise _fail(
            "increment_reconciliation",
            "recomputed volume+turnover mismatches do not match the frozen report",
        )
    return {
        "schema_version": "increment_reconciliation_v1",
        **totals,
        "combined_increment_mismatch_rows": computed_combined,
        "frozen_combined_increment_mismatch_rows": frozen_combined,
        "matches_frozen_report": frozen_combined is None or int(frozen_combined) == computed_combined,
        "grouping": [
            "ingest_run_id", "source_file", "SYMBOL", "TRADINGDATE", "raw_row_index",
        ],
        "formula": (
            "Count separately where non-reset cumulative volume/amount change differs "
            "from native TRADEVOLUME/TRADEAMOUNT; values remain uncorrected."
        ),
        "source_refs": source_refs,
    }


def _event_id(kind: str, *parts: Any) -> str:
    payload = canonical_json([kind, *parts]).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _session_for_time(
    intervals_by_scope: dict[tuple[str, str, date], list[_SessionInterval]],
    exchange: str,
    instrument: str,
    trade_day: date,
    event_time: datetime,
) -> str | None:
    for interval in intervals_by_scope.get((exchange, instrument, trade_day), []):
        if interval.start <= event_time < interval.end:
            return interval.session_id
    return None


def _affected_fields(reason_codes: Iterable[str]) -> list[str]:
    reasons = set(reason_codes)
    fields: list[str] = []
    if "INVALID_L1_BOOK" in reasons:
        fields.extend(["bid_price_1", "ask_price_1", "bid_size_1", "ask_size_1"])
    if reasons & {"DUPLICATE_EVENT_KEY_IDENTICAL", "DUPLICATE_EVENT_KEY_CONFLICTING"}:
        fields.append("business_payload")
    if "DUPLICATE_SOURCE_RECORD" in reasons:
        fields.append("source_provenance")
    if "SESSION_UNCLASSIFIED" in reasons:
        fields.append("source_observation")
    if "MISSING_REQUIRED_FIELD" in reasons:
        fields.append("required_identity_or_time")
    if "UNIX_TIME_MISMATCH" in reasons:
        fields.append("source_event_unix_ms")
    if "EVENT_DATE_MISMATCH" in reasons:
        fields.append("event_time")
    if "NON_TRADING_DATE" in reasons:
        fields.append("trade_date")
    return sorted(set(fields or ["source_observation"]))


def _quarantine_events(
    entry: dict[str, Any],
    session_intervals: list[_SessionInterval],
) -> list[dict[str, Any]]:
    refs = entry["artifacts"]["quarantine"]
    if not refs:
        return []
    paths = [_verify_ref(ref, f"artifacts.quarantine[{index}]") for index, ref in enumerate(refs)]
    path_to_ref = {
        _uri_path(ref["uri"], f"artifacts.quarantine[{index}]"): ref
        for index, ref in enumerate(refs)
    }
    intervals_by_scope: dict[tuple[str, str, date], list[_SessionInterval]] = {}
    for interval in session_intervals:
        intervals_by_scope.setdefault(
            (interval.exchange, interval.instrument_id, interval.trade_date), [],
        ).append(interval)
    source_timezone = _require_text(entry["inputs"].get("source_timezone"),
                                    "inputs.source_timezone")
    ZoneInfo(source_timezone)
    sql_paths = "[" + ",".join(
        "'" + path.as_posix().replace("'", "''") + "'" for path in paths
    ) + "]"
    with duckdb.connect(":memory:") as connection:
        connection.execute("SET memory_limit='2GB'")
        connection.execute("SET threads=2")
        connection.execute(
            f"CREATE TEMP TABLE _quarantine AS "
            f"SELECT * FROM read_parquet({sql_paths}, union_by_name=true, filename=true)"
        )
        rows = connection.execute(
            "SELECT MARKET, SYMBOL, TRADINGDATE, TRADINGTIME, reason_codes, "
            "record_id, raw_file_sha256, raw_row_index, raw_row_group_index, filename "
            "FROM _quarantine ORDER BY filename, raw_row_index"
        ).to_arrow_table().to_pylist()
    events = []
    for index, row in enumerate(rows):
        local_time = row["TRADINGTIME"]
        event_time = None
        if isinstance(local_time, datetime):
            event_time = local_time.replace(tzinfo=ZoneInfo(source_timezone)).astimezone(UTC)
        exchange = row.get("MARKET") or "CFFEX"
        instrument = row.get("SYMBOL")
        trade_day = row.get("TRADINGDATE")
        session_id = None
        if instrument and isinstance(trade_day, date) and event_time is not None:
            session_id = _session_for_time(
                intervals_by_scope, exchange, instrument, trade_day, event_time,
            )
        filename = Path(str(row["filename"])).resolve()
        ref = path_to_ref.get(filename)
        if ref is None:
            raise _fail("artifacts.quarantine", f"cannot map quarantine row source: {filename}")
        reasons = list(row.get("reason_codes") or [])
        events.append({
            "event_id": _event_id(
                "QUARANTINED_RECORD", row.get("record_id"), reasons, index,
            ),
            "event_kind": "QUARANTINED_RECORD",
            "certainty": "OBSERVED",
            "exchange": exchange,
            "instrument_id": instrument,
            "trade_date": trade_day,
            "session_id": session_id,
            "start_time": event_time,
            "end_time": event_time,
            "boundary": "point",
            "reason_codes": reasons,
            "affected_fields": _affected_fields(reasons),
            "evidence_uri": ref["uri"],
            "evidence_sha256": ref["content_hash"],
            "evidence_record_id": row.get("record_id"),
            "raw_row_index": row.get("raw_row_index"),
            "raw_row_group_index": row.get("raw_row_group_index"),
            "row_count": 1,
        })
    return events


def _source_gap_and_continuity_events(
    entry: dict[str, Any],
    entry_ref: dict[str, str],
    session_intervals: list[_SessionInterval],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    quality = entry["quality_report"]
    missing = quality.get(
        "calendar_dates_without_source_rows", quality.get("expected_missing_dates", []),
    )
    if not isinstance(missing, list):
        raise _fail("quality_report.calendar_dates_without_source_rows", "must be an array")
    gaps = []
    events = []
    for raw_day in sorted(set(missing)):
        day = date.fromisoformat(_require_text(raw_day, "calendar_dates_without_source_rows[]"))
        gaps.append({
            "exchange": "CFFEX",
            "trade_date": day.isoformat(),
            "reason": "SOURCE_DATE_MISSING",
            "certainty": "CONFIRMED_SOURCE_ABSENCE",
        })
        events.append({
            "event_id": _event_id("SOURCE_DATE_MISSING", "CFFEX", day.isoformat()),
            "event_kind": "SOURCE_DATE_MISSING",
            "certainty": "CONFIRMED_SOURCE_ABSENCE",
            "exchange": "CFFEX",
            "instrument_id": None,
            "trade_date": day,
            "session_id": None,
            "start_time": None,
            "end_time": None,
            "boundary": "whole_trade_date",
            "reason_codes": ["SOURCE_DATE_MISSING"],
            "affected_fields": ["source_observation"],
            "evidence_uri": entry_ref["uri"],
            "evidence_sha256": entry_ref["content_hash"],
            "evidence_record_id": None,
            "raw_row_index": None,
            "raw_row_group_index": None,
            "row_count": 1,
        })

    intervals_by_scope: dict[tuple[str, str, date], list[_SessionInterval]] = {}
    for interval in session_intervals:
        intervals_by_scope.setdefault(
            (interval.exchange, interval.instrument_id, interval.trade_date), [],
        ).append(interval)
    for partition in entry["partitions"]:
        first = datetime.fromisoformat(
            _require_text(partition["event_time_min"], "partitions[].event_time_min")
        ).astimezone(UTC)
        exchange = partition["exchange"]
        instrument = partition["instrument_id"]
        trade_day = date.fromisoformat(partition["trade_date"])
        session_id = _session_for_time(
            intervals_by_scope, exchange, instrument, trade_day, first,
        )
        ref = partition["artifact_ref"]
        events.append({
            "event_id": _event_id(
                "SOURCE_INTERVAL_START_UNKNOWN", exchange, instrument, trade_day.isoformat(),
            ),
            "event_kind": "SOURCE_INTERVAL_START_UNKNOWN",
            "certainty": "UNKNOWN",
            "exchange": exchange,
            "instrument_id": instrument,
            "trade_date": trade_day,
            "session_id": session_id,
            "start_time": first,
            "end_time": first,
            "boundary": "first_observed_before_unknown",
            "reason_codes": ["SOURCE_INTERVAL_START_UNKNOWN"],
            "affected_fields": ["continuity"],
            "evidence_uri": ref["uri"],
            "evidence_sha256": ref["content_hash"],
            "evidence_record_id": None,
            "raw_row_index": None,
            "raw_row_group_index": None,
            "row_count": 1,
        })
    return gaps, events


def _write_gap_events(path: Path, rows: list[dict[str, Any]]) -> None:
    table = pa.Table.from_pylist(rows, schema=GAP_EVENT_SCHEMA)
    table = table.sort_by([
        ("exchange", "ascending"),
        ("instrument_id", "ascending"),
        ("trade_date", "ascending"),
        ("start_time", "ascending"),
        ("event_kind", "ascending"),
        ("event_id", "ascending"),
    ])
    pq.write_table(table, path, compression="zstd", row_group_size=100_000)


def _source_index(
    entry: dict[str, Any],
    entry_ref: dict[str, str],
    outer_manifest_ref: dict[str, str],
    session_policy_ref: dict[str, str],
    gap_events_ref: dict[str, str],
) -> dict[str, Any]:
    return {
        "schema_version": SOURCE_INDEX_SCHEMA_VERSION,
        "adapter_schema_version": ADAPTER_SCHEMA_VERSION,
        "data_snapshot_id": entry["data_snapshot_id"],
        "source_split_id": entry.get("split_id"),
        "onboarding_entry": entry_ref,
        "onboarding_manifest": outer_manifest_ref,
        "source_timezone": entry["inputs"]["source_timezone"],
        "calendar_id": entry["passport"]["calendar_id"],
        "partitions": entry["partitions"],
        "quarantine": entry["artifacts"]["quarantine"],
        "calendar": entry["artifacts"]["calendar"],
        "session_rules": entry["artifacts"]["session_rules"],
        "session_policy": session_policy_ref,
        "gap_events": gap_events_ref,
        "availability_status": entry["passport"].get("availability_status", "unknown"),
        "research_only": True,
    }


def _manifest(
    *,
    source_id: str,
    version: int,
    content_hash: str,
    data_artifacts: list[dict[str, str]],
    bindings: dict[str, dict[str, str]],
) -> dict[str, Any]:
    return {
        "schema_version": "data_source_manifest_v1",
        "source_id": source_id,
        "version": version,
        "content_hash": content_hash,
        "data_artifacts": data_artifacts,
        "bindings": bindings,
    }


def adapt_onboarding_entry(
    entry_path: str | Path,
    entry_manifest_path: str | Path,
    split_manifest_path: str | Path,
    output_directory: str | Path,
    *,
    source_id: str,
    version: int,
    source_timezone: str = "Asia/Shanghai",
    metadata_path: str | Path | None = None,
    created_by: dict[str, str] | None = None,
    verify_data_hashes: bool = True,
    derive_increment_reconciliation: bool = True,
) -> AdaptedOnboardingSource:
    """Create one immutable DataSource version from a published onboarding entry."""
    entry_file = Path(entry_path).expanduser().resolve()
    entry_manifest_file = Path(entry_manifest_path).expanduser().resolve()
    split_file = Path(split_manifest_path).expanduser().resolve()
    output = Path(output_directory).expanduser().resolve()
    if output.exists():
        raise _fail("output_directory", f"refusing to overwrite existing path: {output}")
    if type(version) is not int or version < 1:
        raise _fail("version", "must be an integer >= 1")
    entry_ref, outer_manifest = _entry_ref(entry_file, entry_manifest_file)
    entry = _read_json(entry_file, "onboarding entry")
    if entry.get("status") not in {"READY", "READY_WITH_WARNINGS"}:
        raise _fail("status", "onboarding entry is not a READY publication")
    if entry.get("stage_name") != "onboard":
        raise _fail("stage_name", "must be onboard")
    if outer_manifest.get("data_snapshot_id") != entry.get("data_snapshot_id"):
        raise _fail("entry manifest.data_snapshot_id", "does not match the onboarding entry")
    if outer_manifest.get("split_id") != entry.get("split_id"):
        raise _fail("entry manifest.split_id", "does not match the onboarding entry")
    if entry.get("inputs", {}).get("source_timezone") != source_timezone:
        raise _fail("source_timezone", "does not match the onboarding entry")
    original_split = _read_json(split_file, "split manifest")
    if original_split.get("schema_version") not in {"split_manifest_v1", "1.0.0"}:
        raise _fail("split_manifest.schema_version", "is not a recognized split manifest")
    if original_split.get("split_id") != entry.get("split_id"):
        raise _fail("split_manifest.split_id", "does not match the onboarding entry")
    if original_split.get("source_data_snapshot_id") != entry.get("data_snapshot_id"):
        raise _fail("split_manifest.source_data_snapshot_id", "does not match the onboarding source")

    quality = entry.get("quality_report")
    if not isinstance(quality, dict):
        raise _fail("quality_report", "must be an object")
    input_rows = _require_int(quality.get("input_rows"), "quality_report.input_rows")
    accepted_rows = _require_int(quality.get("accepted_rows"), "quality_report.accepted_rows")
    quarantined_rows = _require_int(
        quality.get("quarantined_rows"), "quality_report.quarantined_rows",
    )
    if not accepted_rows:
        raise _fail("quality_report.accepted_rows", "zero accepted records cannot be adapted")
    if input_rows != accepted_rows + quarantined_rows:
        raise _fail("quality_report", "input rows do not reconcile to accepted plus quarantined")

    quotes, quarantine, calendar_ref, session_rules_ref = _artifact_refs(entry)
    partition_rows = sum(_require_int(item.get("row_count"), "partitions[].row_count")
                         for item in entry.get("partitions", []))
    if partition_rows != accepted_rows:
        raise _fail("partitions", "partition row counts do not reconcile to accepted_rows")
    if len(quotes) != len(entry["partitions"]):
        raise _fail("artifacts.quotes", "must contain exactly one reference per quote partition")

    if verify_data_hashes:
        for index, ref in enumerate(quotes):
            _verify_ref(ref, f"artifacts.quotes[{index}]")
    else:
        for index, ref in enumerate(quotes):
            path = _ref_path(ref, f"artifacts.quotes[{index}]")
            if not path.is_file():
                raise _fail(f"artifacts.quotes[{index}]", "referenced partition is missing")

    session_rules_path = _verify_ref(session_rules_ref, "artifacts.session_rules")
    session_rules = load_session_rules_from_frozen(session_rules_path)
    provenance = entry.get("inputs", {}).get("calendar", {})
    if not isinstance(provenance, dict):
        raise _fail("inputs.calendar", "must be an object")
    calendar, calendar_dates = _calendar(calendar_ref, provenance.get("provenance", provenance))
    partition_sessions = _session_ids_by_partition(quality)
    session_policy, session_intervals = _session_policy(
        entry, partition_sessions, calendar["calendar_id"], calendar_dates, session_rules,
    )
    instruments, coverage, normalized_partitions = _coverage(entry, partition_sessions)
    calendar_days = [date.fromisoformat(item["trade_date"]) for item in normalized_partitions]
    start_day, end_day = min(calendar_days), max(calendar_days)
    original_split_ref = _file_ref(split_file)
    research_split = _research_split(
        original_split,
        data_snapshot_id=entry["data_snapshot_id"],
        source_timezone=source_timezone,
        instruments=instruments,
        sessions=session_policy["sessions"],
        start_day=start_day,
        end_day=end_day,
        original_ref=original_split_ref,
    )

    metadata_document = None
    metadata_bytes = None
    if metadata_path is not None:
        metadata_file = Path(metadata_path).expanduser().resolve()
        try:
            metadata_bytes = metadata_file.read_bytes()
            metadata_document = json.loads(metadata_bytes)
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise _fail("metadata_path", f"cannot read valid JSON: {exc}") from exc
        metadata_document = validate_market_metadata_document(metadata_document, coverage)

    actor = created_by or {"kind": "agent", "id": "quantsage-onboarding-adapter"}
    normalized_schema, _ = _normalized_schema(entry, entry_file)
    reconciliation = (
        _increment_reconciliation(entry) if derive_increment_reconciliation else None
    )
    quality_summary = _quality_summary(entry, reconciliation)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.mkdir()
    try:
        schema_path = _write_json(output / "source_schema.json", normalized_schema)
        calendar_path = _write_json(output / "calendar.json", calendar)
        session_policy_path = _write_json(output / "session_policy.json", session_policy)
        split_path = _write_json(output / "research_only_split.json", research_split)
        quality_path = _write_json(output / "quality_summary.json", quality_summary)
        reconciliation_path = None
        if reconciliation is not None:
            reconciliation_path = _write_json(
                output / "increment_reconciliation.json",
                reconciliation,
            )
        access_policy_path = _write_json(output / "access_policy.json", {
            "schema_version": "access_policy_v1",
            "access_policy_id": "research_only_descriptive_v1",
            "allowed_uses": ["research"],
            "split_refs": [_file_ref(split_path)],
        })
        copied_metadata_path = None
        if metadata_bytes is not None:
            copied_metadata_path = output / "market_metadata.json"
            copied_metadata_path.write_bytes(metadata_bytes)

        gap_events_path = output / "gap_events.parquet"
        event_rows = _quarantine_events(entry, session_intervals) if verify_data_hashes else []
        if not verify_data_hashes and quarantine:
            event_rows = _quarantine_events(entry, session_intervals)
        source_gaps, source_evidence_events = _source_gap_and_continuity_events(
            entry, entry_ref, session_intervals,
        )
        _write_gap_events(gap_events_path, [*event_rows, *source_evidence_events])
        gap_events_ref = _file_ref(gap_events_path)
        source_index = _source_index(
            entry, entry_ref, _file_ref(entry_manifest_file), _file_ref(session_policy_path),
            gap_events_ref,
        )
        source_index_path = _write_json(output / "source_index.json", source_index)
        gap_manifest = {
            "schema_version": "gap_manifest_v1",
            "policy_version": "gap_policy_v1",
            "source": {
                "data_snapshot_id": entry["data_snapshot_id"],
                "split_id": entry.get("split_id"),
                "onboarding_entry": entry_ref,
                "onboarding_manifest": _file_ref(entry_manifest_file),
            },
            "summary": {
                "input_rows": input_rows,
                "accepted_rows": accepted_rows,
                "quarantined_rows": quarantined_rows,
                "source_date_gap_count": len(source_gaps),
                "quarantine_event_count": len(event_rows),
                "source_interval_start_unknown_count": (
                    len(source_evidence_events) - len(source_gaps)
                ),
                "cumulative_reset_rows": _require_int(
                    quality.get("cumulative_reset_rows", 0),
                    "quality_report.cumulative_reset_rows",
                ),
                "volume_increment_mismatch_rows": quality.get(
                    "volume_increment_mismatch_rows",
                ),
                "turnover_increment_mismatch_rows": quality.get(
                    "turnover_increment_mismatch_rows",
                ),
                "combined_increment_mismatch_rows": quality.get(
                    "volume_turnover_increment_mismatch_rows",
                ),
            },
            "source_date_gaps": source_gaps,
            "raw_interval_semantics": {
                "volume_field": SOURCE_INTERVAL_FIELDS["volume"],
                "turnover_field": SOURCE_INTERVAL_FIELDS["turnover"],
                "differencing": "forbidden",
                "start_status": "unknown",
                "unknown_start_policy": (
                    "Do not use a previous accepted quote as the native interval start."
                ),
                "warning_only": True,
            },
            "warning_semantics": {
                "volume_reconciliation": (
                    "Unverified native volume/increment reconciliation is warning-only and "
                    "does not independently block or invalidate values."
                ),
                "calendar_closures": "Not a gap when represented by the frozen calendar/session rules.",
                "no_update": "Unknown; absence of an update is not evidence of packet loss or zero volume.",
            },
            "events_schema_version": GAP_EVENTS_SCHEMA_VERSION,
            "events_ref": gap_events_ref,
            "adapter_evidence": {
                "source_index": _file_ref(source_index_path),
                "session_policy": _file_ref(session_policy_path),
            },
            "generated_from": [
                entry_ref,
                _file_ref(entry_manifest_file),
                _bare_ref(calendar_ref),
                _bare_ref(session_rules_ref),
                *(_bare_ref(ref) for ref in quarantine),
            ],
        }
        if reconciliation_path is not None:
            gap_manifest["adapter_evidence"]["increment_reconciliation"] = _file_ref(
                reconciliation_path
            )
            gap_manifest["summary"].update({
                "volume_increment_mismatch_rows": reconciliation[
                    "volume_increment_mismatch_rows"
                ],
                "turnover_increment_mismatch_rows": reconciliation[
                    "turnover_increment_mismatch_rows"
                ],
                "combined_increment_mismatch_rows": reconciliation[
                    "combined_increment_mismatch_rows"
                ],
                "cumulative_reset_rows": reconciliation["cumulative_reset_rows"],
            })
        # Optional keys in summary and warning_semantics may be None; normalize them
        # out so the schema remains a compact integer evidence contract.
        for key in (
            "volume_increment_mismatch_rows", "turnover_increment_mismatch_rows",
            "combined_increment_mismatch_rows",
        ):
            if gap_manifest["summary"][key] is None:
                gap_manifest["summary"].pop(key)
        gap_manifest_path = _write_json(output / "gap_manifest.json", gap_manifest)
        validate_gap_manifest_document(gap_manifest)

        data_artifacts = [
            _file_ref(entry_manifest_file),
            _file_ref(entry_file),
        ]
        content_hash = "sha256:" + hashlib.sha256(
            canonical_json(data_artifacts).encode("utf-8")
        ).hexdigest()
        bindings = {
            "schema_ref": _file_ref(schema_path),
            "calendar_ref": _file_ref(calendar_path),
            "session_policy_ref": _file_ref(session_policy_path),
            "access_policy_ref": _file_ref(access_policy_path),
            "gap_manifest_ref": _file_ref(gap_manifest_path),
            "quality_ref": _file_ref(quality_path),
        }
        if copied_metadata_path is not None:
            bindings["market_metadata_ref"] = _file_ref(copied_metadata_path)
        manifest_path = _write_json(
            output / "data_source_manifest.json",
            _manifest(
                source_id=source_id,
                version=version,
                content_hash=content_hash,
                data_artifacts=data_artifacts,
                bindings=bindings,
            ),
        )
        source_document = {
            "schema_version": "data_source_v1",
            "source_id": source_id,
            "version": version,
            "name": "CFFEX government bond futures L2 onboarding source",
            "manifest_ref": _file_ref(manifest_path),
            "content_hash": content_hash,
            "schema_ref": bindings["schema_ref"],
            "format": "l2_onboarding_source_v1",
            "instruments": instruments,
            "coverage": coverage,
            "time_rules": {"timezone": source_timezone, "event_time_field": "event_time"},
            "calendar_ref": bindings["calendar_ref"],
            "session_policy_ref": bindings["session_policy_ref"],
            "availability_mode": "research_only",
            "access_policy_ref": bindings["access_policy_ref"],
            "gap_manifest_ref": bindings["gap_manifest_ref"],
            "quality_ref": bindings["quality_ref"],
            "created_by": actor,
        }
        if "market_metadata_ref" in bindings:
            source_document["market_metadata_ref"] = bindings["market_metadata_ref"]
        validate_source_document(source_document)
        source_document_path = _write_json(output / "source.json", source_document)
        final_source = _read_json(source_document_path, "generated source")
        summary = {
            "source_ref": f"{source_id}@{version}",
            "source_content_hash": content_hash,
            "instruments": len(instruments),
            "coverage_partitions": len(coverage["partitions"]),
            "session_intervals": len(session_policy["intervals"]),
            "accepted_rows": accepted_rows,
            "quarantined_rows": quarantined_rows,
            "gap_events": len(event_rows) + len(source_evidence_events),
            "source_date_gaps": len(source_gaps),
            "metadata_bound": metadata_document is not None,
            "increment_reconciliation_bound": reconciliation is not None,
            "output_directory": str(output),
        }
        return AdaptedOnboardingSource(
            source_document=final_source,
            source_document_path=source_document_path,
            output_directory=output,
            source_index_path=output / "source_index.json",
            gap_manifest_path=output / "gap_manifest.json",
            gap_events_path=output / "gap_events.parquet",
            summary=summary,
        )
    except Exception:
        # The tree contains only newly generated adapter files. Remove it on
        # failure so no caller can mistake a partial source for a publication.
        try:
            for child in sorted(output.rglob("*"), reverse=True):
                if child.is_file() or child.is_symlink():
                    child.unlink()
                elif child.is_dir():
                    child.rmdir()
            output.rmdir()
        except OSError:
            pass
        raise


def load_session_rules_from_frozen(path: Path) -> SessionRules:
    """Load a frozen session policy through the same rule engine as Step 02."""
    frozen = _read_json(path, "artifacts.session_rules")
    if frozen.get("version") is None or not isinstance(frozen.get("rules"), list):
        raise _fail("artifacts.session_rules", "must contain a version and rules")
    evidence = {
        "evidence_version": frozen["version"],
        "session_rules": {
            "timezone": frozen.get("timezone"),
            "rules": frozen["rules"],
        },
    }
    return SessionRules(
        version=str(frozen["version"]),
        timezone=_require_text(frozen.get("timezone"), "artifacts.session_rules.timezone"),
        rules=tuple(frozen["rules"]),
        evidence=evidence,
    )


class OnboardingDataSourceReader:
    """Bounded reader over one adapted, frozen onboarding DataSource."""

    def __init__(self, source_document: dict[str, Any], *, verify_hashes: bool = True):
        self._source = validate_source_document(source_document)
        self._verify_hashes = verify_hashes
        self._index = self._load_source_index()
        self._partition_index = {
            (item["exchange"], item["instrument_id"], item["trade_date"]): item
            for item in self._index["partitions"]
        }

    def _verify(self, ref: dict[str, Any], label: str) -> Path:
        path = _ref_path(ref, label)
        if self._verify_hashes and sha256_file(path) != ref["content_hash"]:
            raise _fail(label, "content hash changed")
        if not path.is_file():
            raise _fail(label, "referenced file is missing")
        return path

    def _load_source_index(self) -> dict[str, Any]:
        manifest_path = self._verify(
            self._source["manifest_ref"], "source.manifest_ref",
        )
        _read_json(manifest_path, "source manifest")
        gap_path = self._verify(self._source["gap_manifest_ref"], "source.gap_manifest_ref")
        gap_manifest = _read_json(gap_path, "gap manifest")
        ref = gap_manifest.get("adapter_evidence", {}).get("source_index")
        if not isinstance(ref, dict):
            raise _fail("gap manifest", "does not contain an onboarding source index ref")
        index_path = self._verify(ref, "gap manifest source index")
        value = _read_json(index_path, "source index")
        if value.get("schema_version") != SOURCE_INDEX_SCHEMA_VERSION:
            raise _fail("source index", "has an unexpected schema version")
        return value

    def partition(self, instrument_id: str, trade_date: str, *, exchange: str = "CFFEX",
                  verify_hash: bool = True) -> dict[str, Any]:
        key = (exchange, instrument_id, trade_date)
        item = self._partition_index.get(key)
        if item is None:
            raise _fail("partition", f"source does not contain {exchange}/{instrument_id}/{trade_date}")
        ref = item["artifact_ref"]
        path = _ref_path(ref, "partition.artifact_ref")
        if verify_hash and sha256_file(path) != ref["content_hash"]:
            raise _fail("partition.artifact_ref", "content hash changed")
        return item

    def read_quotes(
        self,
        instrument_id: str,
        trade_date: str,
        *,
        exchange: str = "CFFEX",
        session_id: str | None = None,
        columns: list[str] | None = None,
        verify_hash: bool = True,
    ) -> pa.Table:
        item = self.partition(
            instrument_id, trade_date, exchange=exchange, verify_hash=verify_hash,
        )
        path = _ref_path(item["artifact_ref"], "partition.artifact_ref")
        requested = list(columns) if columns is not None else None
        projected = list(requested) if requested is not None else None
        if projected is not None:
            for required in ("instrument_id", "trade_date", "exchange"):
                if required not in projected:
                    projected.append(required)
        table = pq.read_table(path, columns=projected)
        mask = pc.and_(
            pc.equal(table["instrument_id"], pa.scalar(instrument_id)),
            pc.equal(table["trade_date"], pa.scalar(date.fromisoformat(trade_date))),
        )
        if "exchange" in table.column_names:
            mask = pc.and_(mask, pc.equal(table["exchange"], pa.scalar(exchange)))
        if session_id is not None:
            if "session_id" not in table.column_names:
                raise _fail("read_quotes", "session_id was not projected")
            mask = pc.and_(mask, pc.equal(table["session_id"], pa.scalar(session_id)))
        table = table.filter(mask)
        if requested is not None:
            table = table.select(requested)
        return table

    def tick_size(self, instrument_id: str, trade_date: str) -> Decimal | None:
        ref = self._source.get("market_metadata_ref")
        if ref is None:
            return None
        metadata = validate_market_metadata_document(
            _read_json(self._verify(ref, "source.market_metadata_ref"), "market metadata"),
        )
        product = next(
            (candidate for candidate in ("TL", "TF", "TS", "T")
             if instrument_id.startswith(candidate)),
            None,
        )
        if product is None:
            return None
        day = date.fromisoformat(trade_date)
        product_rules = [
            rule for rule in metadata["tick_rules"] if rule["product"] == product
        ]
        for selector in (instrument_id, None):
            for rule in product_rules:
                if rule.get("instrument_id") != selector:
                    continue
                start = date.fromisoformat(rule["valid_from"])
                end = None if rule["valid_to"] is None else date.fromisoformat(rule["valid_to"])
                if start <= day and (end is None or day < end):
                    return Decimal(rule["tick_size"])
        return None

    def units(self) -> dict[str, str] | None:
        """Return user-declared price/size/turnover units, when supplied."""
        ref = self._source.get("market_metadata_ref")
        if ref is None:
            return None
        metadata = validate_market_metadata_document(
            _read_json(self._verify(ref, "source.market_metadata_ref"), "market metadata"),
        )
        units = metadata.get("units")
        return dict(units) if isinstance(units, dict) else None

    def gap_events(
        self,
        *,
        instrument_id: str | None = None,
        trade_date: str | None = None,
        session_id: str | None = None,
    ) -> pa.Table:
        ref = self._index["gap_events"]
        path = self._verify(ref, "source index gap_events")
        table = pq.read_table(path)
        mask = None
        for name, value in (
            ("instrument_id", instrument_id),
            ("trade_date", trade_date),
            ("session_id", session_id),
        ):
            if value is None:
                continue
            scalar = (pa.scalar(date.fromisoformat(value)) if name == "trade_date"
                      else pa.scalar(value))
            expression = pc.equal(table[name], scalar)
            mask = expression if mask is None else pc.and_(mask, expression)
        return table.filter(mask) if mask is not None else table
