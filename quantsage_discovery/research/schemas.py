"""Strict JSON validation for the 01-B Study and DataSource contracts."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from itertools import pairwise
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pyarrow as pa
import pyarrow.parquet as pq

from quantsage_discovery.core.utils import canonical_json

from .errors import ResearchValidationError

_HASH_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_REF_RE = re.compile(r"^([A-Za-z0-9][A-Za-z0-9_.-]*)@([1-9][0-9]*)$")
_GAP_EVENT_TYPES = {
    "event_id": pa.string(),
    "event_kind": pa.string(),
    "certainty": pa.string(),
    "exchange": pa.string(),
    "instrument_id": pa.string(),
    "trade_date": pa.date32(),
    "session_id": pa.string(),
    "start_time": pa.timestamp("us", tz="UTC"),
    "end_time": pa.timestamp("us", tz="UTC"),
    "boundary": pa.string(),
    "reason_codes": pa.list_(pa.string()),
    "affected_fields": pa.list_(pa.string()),
    "evidence_uri": pa.string(),
    "evidence_sha256": pa.string(),
    "evidence_record_id": pa.string(),
    "raw_row_index": pa.int64(),
    "raw_row_group_index": pa.int32(),
    "row_count": pa.int64(),
}


def _invalid(path: str, message: str) -> ResearchValidationError:
    return ResearchValidationError(f"{path}: {message}", details={"path": path})


def _object(value: Any, *, required: set[str], optional: set[str], path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _invalid(path, "must be an object")
    if any(not isinstance(key, str) for key in value):
        raise _invalid(path, "object keys must be strings")
    missing = required - value.keys()
    unknown = value.keys() - required - optional
    if missing:
        raise _invalid(path, f"missing required fields: {', '.join(sorted(missing))}")
    if unknown:
        raise _invalid(path, f"unknown fields: {', '.join(sorted(unknown))}")
    return value


def _text(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _invalid(path, "must be a non-empty string")
    if value != value.strip():
        raise _invalid(path, "must not have surrounding whitespace")
    return value


def _string_list(value: Any, path: str, *, nonempty: bool = True) -> list[str]:
    if not isinstance(value, list) or (nonempty and not value):
        raise _invalid(path, "must be a non-empty array of strings" if nonempty else
                       "must be an array of strings")
    result = [_text(item, f"{path}[{index}]") for index, item in enumerate(value)]
    if len(set(result)) != len(result):
        raise _invalid(path, "values must be unique")
    return result


def _sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _file_sha256(path: Path, label: str) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise _invalid(label, f"cannot read referenced file {path}: {exc}") from exc
    return "sha256:" + digest.hexdigest()


def _validate_ref(value: Any, path: str) -> dict[str, str]:
    ref = _object(value, required={"uri", "content_hash"}, optional=set(), path=path)
    uri = _text(ref["uri"], f"{path}.uri")
    digest = _text(ref["content_hash"], f"{path}.content_hash")
    if not _HASH_RE.fullmatch(digest):
        raise _invalid(f"{path}.content_hash", "must be sha256:<64 lowercase hex digits>")
    parsed = urlparse(uri)
    if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
        raise _invalid(f"{path}.uri", "01-B fixture refs must use a local file:// URI")
    if not parsed.path:
        raise _invalid(f"{path}.uri", "file URI must include a path")
    return {"uri": uri, "content_hash": digest}


def _local_path(ref: dict[str, str], path: str) -> Path:
    uri = urlparse(ref["uri"])
    return Path(unquote(uri.path)).resolve()


def _read_ref(ref: dict[str, str], path: str, *, verify_content: bool = True) -> bytes:
    file_path = _local_path(ref, path)
    try:
        payload = file_path.read_bytes()
    except OSError as exc:
        raise _invalid(path, f"cannot read referenced file {ref['uri']}: {exc}") from exc
    if verify_content and _sha256(payload) != ref["content_hash"]:
        raise _invalid(path, "referenced file content hash does not match")
    return payload


def _read_json_ref(ref: dict[str, str], path: str) -> dict[str, Any]:
    payload = _read_ref(ref, path)
    try:
        value = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise _invalid(path, "referenced file must contain valid JSON") from exc
    if not isinstance(value, dict):
        raise _invalid(path, "referenced JSON must be an object")
    return value


def _actor(value: Any, path: str) -> dict[str, str]:
    actor = _object(value, required={"kind", "id"}, optional=set(), path=path)
    if not isinstance(actor["kind"], str) or actor["kind"] not in {"human", "agent"}:
        raise _invalid(f"{path}.kind", "must be 'human' or 'agent'")
    return {"kind": actor["kind"], "id": _text(actor["id"], f"{path}.id")}


def parse_versioned_ref(value: Any, path: str) -> tuple[str, int]:
    text = _text(value, path)
    match = _REF_RE.fullmatch(text)
    if not match:
        raise _invalid(path, "must be an exact version ref such as source_id@1")
    return match.group(1), int(match.group(2))


def _date(value: Any, path: str) -> date:
    if not isinstance(value, str):
        raise _invalid(path, "must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise _invalid(path, "must be an ISO date YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise _invalid(path, "must be a canonical ISO date YYYY-MM-DD")
    return parsed


def _timestamp(value: Any, path: str) -> datetime:
    if not isinstance(value, str):
        raise _invalid(path, "must be an ISO timestamp with a timezone")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise _invalid(path, "must be an ISO timestamp with a timezone") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _invalid(path, "must include a timezone offset")
    return parsed


def _coverage(value: Any, instruments: list[str], calendar: dict[str, Any]) -> dict[str, Any]:
    coverage = _object(value, required={"partitioning", "partitions"}, optional=set(),
                       path="source.coverage")
    if coverage["partitioning"] != "trade_date_v1":
        raise _invalid("source.coverage.partitioning", "must be trade_date_v1")
    if not isinstance(coverage["partitions"], list) or not coverage["partitions"]:
        raise _invalid("source.coverage.partitions", "must be a non-empty array")
    calendar_dates = set(calendar["trading_dates"])
    seen: set[tuple[str | None, str, str]] = set()
    normalized = []
    for index, item in enumerate(coverage["partitions"]):
        path = f"source.coverage.partitions[{index}]"
        item = _object(item, required={"instrument_id", "trade_date", "session_ids"},
                       optional={"exchange"}, path=path)
        exchange = None if "exchange" not in item else _text(item["exchange"], f"{path}.exchange")
        instrument = _text(item["instrument_id"], f"{path}.instrument_id")
        trade_date = _date(item["trade_date"], f"{path}.trade_date").isoformat()
        sessions = _string_list(item["session_ids"], f"{path}.session_ids")
        if instrument not in instruments:
            raise _invalid(f"{path}.instrument_id", "must be declared in source.instruments")
        if trade_date not in calendar_dates:
            raise _invalid(f"{path}.trade_date", "is not present in the referenced calendar")
        key = (exchange, instrument, trade_date)
        if key in seen:
            raise _invalid(path, "duplicate instrument/trade_date partition")
        seen.add(key)
        normalized_item = {"instrument_id": instrument, "trade_date": trade_date,
                           "session_ids": sessions}
        if exchange is not None:
            normalized_item["exchange"] = exchange
        normalized.append(normalized_item)
    normalized.sort(key=lambda item: (item.get("exchange", ""), item["instrument_id"],
                                      item["trade_date"]))
    return {"partitioning": "trade_date_v1", "partitions": normalized}


def validate_market_metadata_document(
    metadata: Any,
    coverage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate an immutable market-metadata document.

    Tick rules are evaluated by product with a longest-prefix match and
    half-open effective dates. When source coverage is supplied, every
    instrument/date partition must be covered for tick-dependent work.
    """
    required = {"metadata_id", "version", "provided_by", "source", "tick_rules"}
    optional = {"schema_version", "units"}
    _object(metadata, required=required, optional=optional, path="market_metadata")
    if "schema_version" in metadata and metadata["schema_version"] != "market_metadata_v1":
        raise _invalid("market_metadata.schema_version", "must be market_metadata_v1")
    _text(metadata["metadata_id"], "market_metadata.metadata_id")
    if type(metadata["version"]) is not int or metadata["version"] < 1:
        raise _invalid("market_metadata.version", "must be an integer >= 1")
    _actor(metadata["provided_by"], "market_metadata.provided_by")
    _text(metadata["source"], "market_metadata.source")
    if "units" in metadata:
        units = _object(
            metadata["units"],
            required={"price_unit", "size_unit", "turnover_unit"},
            optional=set(),
            path="market_metadata.units",
        )
        for field, value in units.items():
            _text(value, f"market_metadata.units.{field}")
    if not isinstance(metadata["tick_rules"], list) or not metadata["tick_rules"]:
        raise _invalid("market_metadata.tick_rules", "must be a non-empty array")
    ranges: dict[tuple[str, str | None], list[tuple[date, date | None]]] = {}
    normalized_rules = []
    for index, rule in enumerate(metadata["tick_rules"]):
        path = f"market_metadata.tick_rules[{index}]"
        rule = _object(rule, required={"product", "valid_from", "valid_to", "tick_size", "unit"},
                       optional={"instrument_id"}, path=path)
        product = _text(rule["product"], f"{path}.product")
        if product not in {"T", "TF", "TS", "TL"}:
            raise _invalid(f"{path}.product", "must be T, TF, TS, or TL")
        instrument_id = None
        if "instrument_id" in rule:
            instrument_id = _text(rule["instrument_id"], f"{path}.instrument_id")
            if not instrument_id.startswith(product):
                raise _invalid(
                    f"{path}.instrument_id",
                    "must start with the declared product prefix",
                )
        start = _date(rule["valid_from"], f"{path}.valid_from")
        end = None if rule["valid_to"] is None else _date(rule["valid_to"], f"{path}.valid_to")
        if end is not None and end <= start:
            raise _invalid(path, "valid_to is an exclusive date after valid_from")
        tick_text = _text(rule["tick_size"], f"{path}.tick_size")
        try:
            tick_size = Decimal(tick_text)
        except InvalidOperation as exc:
            raise _invalid(f"{path}.tick_size", "must be a positive decimal string") from exc
        if not tick_size.is_finite() or tick_size <= 0:
            raise _invalid(f"{path}.tick_size", "must be a positive decimal string")
        if rule["unit"] != "quoted_price_unit":
            raise _invalid(f"{path}.unit", "must be quoted_price_unit")
        ranges.setdefault((product, instrument_id), []).append((start, end))
        normalized_rules.append({"product": product, "valid_from": start.isoformat(),
                                 "valid_to": None if end is None else end.isoformat(),
                                 "tick_size": tick_text, "unit": rule["unit"]})
        if instrument_id is not None:
            normalized_rules[-1]["instrument_id"] = instrument_id
    for (product, instrument_id), selector_ranges in ranges.items():
        selector_ranges.sort(key=lambda interval: interval[0])
        for previous, following in pairwise(selector_ranges):
            if previous[1] is None or following[0] < previous[1]:
                raise _invalid("market_metadata.tick_rules",
                               "overlapping effective dates for "
                               f"{instrument_id or f'product {product}'}")

    if coverage is None:
        return {**metadata, "tick_rules": normalized_rules}

    for partition in coverage["partitions"]:
        instrument = partition["instrument_id"]
        products = [product for product in ("TL", "TF", "TS", "T")
                    if instrument.startswith(product)]
        if not products:
            raise _invalid("market_metadata.tick_rules",
                           f"no tick rule product prefix covers {instrument}")
        product = max(products, key=len)
        day = date.fromisoformat(partition["trade_date"])
        candidates = (
            ranges.get((product, instrument), [])
            + ranges.get((product, None), [])
        )
        if not candidates:
            raise _invalid("market_metadata.tick_rules",
                           f"no tick rule exists for product {product} covering {instrument}")
        if not any(start <= day and (end is None or day < end)
                   for start, end in candidates):
            raise _invalid("market_metadata.tick_rules",
                           f"tick rules do not cover {instrument} on {day.isoformat()}")
    return {**metadata, "tick_rules": normalized_rules}


def _market_metadata(ref: dict[str, str], coverage: dict[str, Any]) -> dict[str, Any]:
    return validate_market_metadata_document(
        _read_json_ref(ref, "source.market_metadata_ref"),
        coverage,
    )


def validate_gap_manifest_document(manifest: Any) -> dict[str, Any]:
    """Validate the versioned gap/reset/interval evidence manifest."""
    required = {
        "schema_version", "policy_version", "source", "summary", "source_date_gaps",
        "raw_interval_semantics", "events_schema_version", "events_ref", "generated_from",
    }
    optional = {"warning_semantics", "adapter_evidence"}
    manifest = _object(
        manifest, required=required, optional=optional, path="gap_manifest",
    )
    if manifest["schema_version"] != "gap_manifest_v1":
        raise _invalid("gap_manifest.schema_version", "must be gap_manifest_v1")
    if manifest["policy_version"] != "gap_policy_v1":
        raise _invalid("gap_manifest.policy_version", "must be gap_policy_v1")
    if manifest["events_schema_version"] != "gap_events_v1":
        raise _invalid("gap_manifest.events_schema_version", "must be gap_events_v1")
    source = _object(
        manifest["source"],
        required={"data_snapshot_id", "onboarding_entry", "onboarding_manifest"},
        optional={"split_id"},
        path="gap_manifest.source",
    )
    _text(source["data_snapshot_id"], "gap_manifest.source.data_snapshot_id")
    for field in ("onboarding_entry", "onboarding_manifest"):
        _validate_ref(source[field], f"gap_manifest.source.{field}")
    if source.get("split_id") is not None:
        _text(source["split_id"], "gap_manifest.source.split_id")

    summary = _object(
        manifest["summary"],
        required={
            "input_rows", "accepted_rows", "quarantined_rows", "source_date_gap_count",
            "quarantine_event_count", "source_interval_start_unknown_count",
            "cumulative_reset_rows",
        },
        optional={
            "volume_increment_mismatch_rows", "turnover_increment_mismatch_rows",
            "combined_increment_mismatch_rows",
        },
        path="gap_manifest.summary",
    )
    for name, value in summary.items():
        if type(value) is not int or value < 0:
            raise _invalid(f"gap_manifest.summary.{name}", "must be a non-negative integer")
    if summary["input_rows"] != summary["accepted_rows"] + summary["quarantined_rows"]:
        raise _invalid("gap_manifest.summary", "input_rows must equal accepted_rows + quarantined_rows")

    gaps = manifest["source_date_gaps"]
    if not isinstance(gaps, list):
        raise _invalid("gap_manifest.source_date_gaps", "must be an array")
    seen_dates = set()
    for index, gap in enumerate(gaps):
        path = f"gap_manifest.source_date_gaps[{index}]"
        gap = _object(
            gap,
            required={"exchange", "trade_date", "reason", "certainty"},
            optional={"instrument_id", "session_id"},
            path=path,
        )
        _text(gap["exchange"], f"{path}.exchange")
        day = _date(gap["trade_date"], f"{path}.trade_date").isoformat()
        if gap["reason"] != "SOURCE_DATE_MISSING":
            raise _invalid(f"{path}.reason", "must be SOURCE_DATE_MISSING")
        if gap["certainty"] != "CONFIRMED_SOURCE_ABSENCE":
            raise _invalid(f"{path}.certainty", "must be CONFIRMED_SOURCE_ABSENCE")
        key = (gap["exchange"], gap.get("instrument_id"), day, gap.get("session_id"))
        if key in seen_dates:
            raise _invalid(path, "duplicate source-date gap")
        seen_dates.add(key)

    interval = _object(
        manifest["raw_interval_semantics"],
        required={
            "volume_field", "turnover_field", "differencing", "start_status",
            "unknown_start_policy",
        },
        optional={"warning_only"},
        path="gap_manifest.raw_interval_semantics",
    )
    if interval["volume_field"] != "TRADEVOLUME" or interval["turnover_field"] != "TRADEAMOUNT":
        raise _invalid(
            "gap_manifest.raw_interval_semantics",
            "native interval fields must be TRADEVOLUME and TRADEAMOUNT",
        )
    if interval["differencing"] != "forbidden":
        raise _invalid(
            "gap_manifest.raw_interval_semantics.differencing",
            "must be forbidden",
        )
    if interval["start_status"] not in {"known", "unknown"}:
        raise _invalid(
            "gap_manifest.raw_interval_semantics.start_status",
            "must be known or unknown",
        )
    if interval.get("warning_only") is not True:
        raise _invalid(
            "gap_manifest.raw_interval_semantics.warning_only",
            "volume reconciliation warnings must remain warning-only",
        )

    events_ref = _validate_ref(manifest["events_ref"], "gap_manifest.events_ref")
    event_path = _local_path(events_ref, "gap_manifest.events_ref")
    if _file_sha256(event_path, "gap_manifest.events_ref") != events_ref["content_hash"]:
        raise _invalid("gap_manifest.events_ref", "referenced gap event file hash does not match")
    try:
        event_file = pq.ParquetFile(event_path)
        event_schema = event_file.schema_arrow
    except Exception as exc:
        raise _invalid("gap_manifest.events_ref", f"cannot read gap events Parquet: {exc}") from exc
    expected_fields = [
        pa.field(name, dtype, nullable=(name not in {
            "event_id", "event_kind", "certainty", "exchange", "boundary",
            "reason_codes", "affected_fields", "evidence_uri", "evidence_sha256",
            "row_count",
        }))
        for name, dtype in _GAP_EVENT_TYPES.items()
    ]
    if event_schema.names != list(_GAP_EVENT_TYPES):
        raise _invalid("gap_manifest.events_ref", "gap event fields do not match gap_events_v1")
    expected_schema = pa.schema(expected_fields)
    if not event_schema.equals(expected_schema, check_metadata=False):
        raise _invalid("gap_manifest.events_ref", "gap event types do not match gap_events_v1")
    expected_event_rows = (
        summary["source_date_gap_count"]
        + summary["quarantine_event_count"]
        + summary["source_interval_start_unknown_count"]
    )
    if event_file.metadata.num_rows != expected_event_rows:
        raise _invalid(
            "gap_manifest.events_ref",
            "event row count does not match the summary event partitions",
        )
    generated_from = manifest["generated_from"]
    if not isinstance(generated_from, list) or not generated_from:
        raise _invalid("gap_manifest.generated_from", "must be a non-empty array of refs")
    seen_refs = set()
    for index, raw_ref in enumerate(generated_from):
        ref = _validate_ref(raw_ref, f"gap_manifest.generated_from[{index}]")
        key = (ref["uri"], ref["content_hash"])
        if key in seen_refs:
            raise _invalid("gap_manifest.generated_from", "refs must be unique")
        seen_refs.add(key)
        if _file_sha256(_local_path(ref, f"gap_manifest.generated_from[{index}]"),
                        f"gap_manifest.generated_from[{index}]") != ref["content_hash"]:
            raise _invalid(f"gap_manifest.generated_from[{index}]", "content hash does not match")
    if "adapter_evidence" in manifest:
        adapter_evidence = manifest["adapter_evidence"]
        if not isinstance(adapter_evidence, dict) or not adapter_evidence:
            raise _invalid(
                "gap_manifest.adapter_evidence",
                "must be a non-empty object of artifact refs",
            )
        for name, raw_ref in adapter_evidence.items():
            path = f"gap_manifest.adapter_evidence.{name}"
            _text(name, f"{path}.name")
            ref = _validate_ref(raw_ref, path)
            if _file_sha256(_local_path(ref, path), path) != ref["content_hash"]:
                raise _invalid(path, "content hash does not match")
    return manifest


def _gap_manifest(ref: dict[str, str]) -> dict[str, Any]:
    return validate_gap_manifest_document(_read_json_ref(ref, "source.gap_manifest_ref"))


def validate_source_document(document: Any) -> dict[str, Any]:
    required = {
        "schema_version", "source_id", "version", "name", "manifest_ref", "content_hash",
        "schema_ref", "format", "instruments", "coverage", "time_rules", "calendar_ref",
        "session_policy_ref", "availability_mode", "access_policy_ref", "created_by",
    }
    optional = {"market_metadata_ref", "gap_manifest_ref", "quality_ref"}
    source = _object(document, required=required, optional=optional, path="source")
    if source["schema_version"] != "data_source_v1":
        raise _invalid("source.schema_version", "must be data_source_v1")
    source_id = _text(source["source_id"], "source.source_id")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", source_id):
        raise _invalid("source.source_id", "must be a stable identifier")
    if type(source["version"]) is not int or source["version"] < 1:
        raise _invalid("source.version", "must be an integer >= 1")
    _text(source["name"], "source.name")
    content_hash = _text(source["content_hash"], "source.content_hash")
    if not _HASH_RE.fullmatch(content_hash):
        raise _invalid("source.content_hash", "must be sha256:<64 lowercase hex digits>")
    created_by = _actor(source["created_by"], "source.created_by")

    refs = {}
    for field in ("manifest_ref", "schema_ref", "calendar_ref", "session_policy_ref",
                  "access_policy_ref", "market_metadata_ref", "gap_manifest_ref", "quality_ref"):
        if field in source:
            refs[field] = _validate_ref(source[field], f"source.{field}")

    instruments = _string_list(source["instruments"], "source.instruments")
    if not isinstance(source["format"], str) or not source["format"].strip():
        raise _invalid("source.format", "must be a non-empty string")
    time_rules = _object(source["time_rules"], required={"timezone", "event_time_field"},
                         optional={"available_time_field", "availability_assumption"},
                         path="source.time_rules")
    timezone = _text(time_rules["timezone"], "source.time_rules.timezone")
    try:
        ZoneInfo(timezone)
    except ZoneInfoNotFoundError as exc:
        raise _invalid("source.time_rules.timezone", "must be a known IANA timezone") from exc
    event_field = _text(time_rules["event_time_field"], "source.time_rules.event_time_field")
    available_field = time_rules.get("available_time_field")
    if available_field is not None:
        available_field = _text(available_field, "source.time_rules.available_time_field")
    mode = source["availability_mode"]
    if not isinstance(mode, str) or mode not in {"observed", "assumed_latency", "research_only"}:
        raise _invalid("source.availability_mode", "must be observed, assumed_latency, or research_only")
    assumption = time_rules.get("availability_assumption")
    if mode == "observed" and not available_field:
        raise _invalid("source.time_rules.available_time_field",
                       "is required when availability_mode is observed")
    if mode == "assumed_latency":
        assumption = _object(assumption, required={"latency", "unit"}, optional=set(),
                             path="source.time_rules.availability_assumption")
        if (type(assumption["latency"]) not in {int, float}
                or not Decimal(str(assumption["latency"])).is_finite()
                or assumption["latency"] <= 0):
            raise _invalid("source.time_rules.availability_assumption.latency",
                           "must be a positive number")
        if assumption["unit"] not in {"milliseconds", "seconds", "minutes"}:
            raise _invalid("source.time_rules.availability_assumption.unit",
                           "must be milliseconds, seconds, or minutes")
        if available_field:
            raise _invalid("source.time_rules", "assumed_latency cannot also name available_time_field")
    elif assumption is not None:
        raise _invalid("source.time_rules.availability_assumption",
                       "is only valid when availability_mode is assumed_latency")

    schema = _read_json_ref(refs["schema_ref"], "source.schema_ref")
    schema = _object(schema, required={"schema_version", "fields"}, optional=set(),
                     path="source_schema")
    if schema["schema_version"] != "source_schema_v1" or not isinstance(schema["fields"], dict):
        raise _invalid("source.schema_ref", "must reference source_schema_v1 with a fields object")
    for name, descriptor in schema["fields"].items():
        _text(name, "source_schema.fields.name")
        _text(descriptor, f"source_schema.fields.{name}")
    if event_field not in schema["fields"]:
        raise _invalid("source.time_rules.event_time_field", "is not declared by source.schema_ref")
    if available_field and available_field not in schema["fields"]:
        raise _invalid("source.time_rules.available_time_field",
                       "is not declared by source.schema_ref")
    for field in (event_field, available_field):
        if field and not schema["fields"][field].startswith("timestamp"):
            raise _invalid(f"source_schema.fields.{field}", "must declare a timestamp type")

    calendar = _read_json_ref(refs["calendar_ref"], "source.calendar_ref")
    calendar = _object(calendar, required={"schema_version", "calendar_id", "timezone", "trading_dates"},
                       optional=set(), path="calendar")
    if calendar["schema_version"] != "calendar_v1":
        raise _invalid("source.calendar_ref", "must reference calendar_v1")
    _text(calendar["calendar_id"], "calendar.calendar_id")
    if calendar["timezone"] != timezone:
        raise _invalid("source.calendar_ref", "calendar timezone must match source.time_rules")
    if not isinstance(calendar["trading_dates"], list) or not calendar["trading_dates"]:
        raise _invalid("calendar.trading_dates", "must be a non-empty array")
    calendar["trading_dates"] = [_date(day, "calendar.trading_dates[]").isoformat()
                                  for day in calendar["trading_dates"]]
    if len(set(calendar["trading_dates"])) != len(calendar["trading_dates"]):
        raise _invalid("calendar.trading_dates", "values must be unique")
    calendar["trading_dates"].sort()

    sessions = _read_json_ref(refs["session_policy_ref"], "source.session_policy_ref")
    sessions = _object(sessions, required={"schema_version", "session_policy_id", "sessions"},
                       optional={"intervals"}, path="session_policy")
    if sessions["schema_version"] != "session_policy_v1":
        raise _invalid("source.session_policy_ref", "must reference session_policy_v1")
    _text(sessions["session_policy_id"], "session_policy.session_policy_id")
    valid_sessions = _string_list(sessions["sessions"], "session_policy.sessions")
    coverage = _coverage(source["coverage"], instruments, calendar)
    calendar_dates = set(calendar["trading_dates"])
    if any(session not in valid_sessions for partition in coverage["partitions"]
           for session in partition["session_ids"]):
        raise _invalid("source.coverage.partitions", "contains a session not defined by session policy")
    if "intervals" in sessions:
        if not isinstance(sessions["intervals"], list):
            raise _invalid("session_policy.intervals", "must be an array")
        interval_groups: dict[tuple[str | None, str, str], list[tuple[datetime, datetime]]] = {}
        interval_keys: set[tuple[str | None, str, str, str]] = set()
        for index, interval in enumerate(sessions["intervals"]):
            path = f"session_policy.intervals[{index}]"
            interval = _object(interval, required={"instrument_id", "trade_date", "session_id",
                                                   "start", "end"}, optional={"exchange"}, path=path)
            exchange = (None if "exchange" not in interval else
                        _text(interval["exchange"], f"{path}.exchange"))
            instrument = _text(interval["instrument_id"], f"{path}.instrument_id")
            day = _date(interval["trade_date"], f"{path}.trade_date").isoformat()
            session_id = _text(interval["session_id"], f"{path}.session_id")
            if instrument not in instruments or day not in calendar_dates or session_id not in valid_sessions:
                raise _invalid(path, "must match a declared instrument, trading date, and session")
            start = _timestamp(interval["start"], f"{path}.start")
            end = _timestamp(interval["end"], f"{path}.end")
            if start >= end:
                raise _invalid(path, "start must be before end")
            key = (exchange, instrument, day, session_id)
            if key in interval_keys:
                raise _invalid(path, "duplicate instrument/trade_date/session interval")
            interval_keys.add(key)
            interval_groups.setdefault((exchange, instrument, day), []).append((start, end))
        for key, values in interval_groups.items():
            values.sort(key=lambda pair: pair[0])
            if any(current[0] < previous[1] for previous, current in pairwise(values)):
                raise _invalid("session_policy.intervals",
                               f"overlapping intervals for {key[1]} {key[2]}")
        for partition in coverage["partitions"]:
            for session_id in partition["session_ids"]:
                exchange = partition.get("exchange")
                exact = (exchange, partition["instrument_id"], partition["trade_date"], session_id)
                shared = (None, partition["instrument_id"], partition["trade_date"], session_id)
                if exact not in interval_keys and shared not in interval_keys:
                    raise _invalid("source.coverage.partitions",
                                   "session has no corresponding declared interval")

    access = _read_json_ref(refs["access_policy_ref"], "source.access_policy_ref")
    access = _object(access, required={"schema_version", "access_policy_id", "allowed_uses", "split_refs"},
                     optional=set(), path="access_policy")
    if access["schema_version"] != "access_policy_v1":
        raise _invalid("source.access_policy_ref", "must reference access_policy_v1")
    _text(access["access_policy_id"], "access_policy.access_policy_id")
    _string_list(access["allowed_uses"], "access_policy.allowed_uses")
    if not isinstance(access["split_refs"], list):
        raise _invalid("access_policy.split_refs", "must be an array")
    split_refs = [_validate_ref(item, f"access_policy.split_refs[{index}]")
                  for index, item in enumerate(access["split_refs"])]
    seen_split_refs = set()
    for index, split_ref in enumerate(split_refs):
        key = (split_ref["uri"], split_ref["content_hash"])
        if key in seen_split_refs:
            raise _invalid("access_policy.split_refs", "values must be unique")
        seen_split_refs.add(key)
        split_doc = _read_json_ref(split_ref, f"access_policy.split_refs[{index}]")
        if split_doc.get("schema_version") != "split_manifest_v1":
            raise _invalid(f"access_policy.split_refs[{index}]", "must reference split_manifest_v1")
        _text(split_doc.get("split_id"), f"access_policy.split_refs[{index}].split_id")

    if "market_metadata_ref" in refs:
        _market_metadata(refs["market_metadata_ref"], coverage)
    if "gap_manifest_ref" in refs:
        _gap_manifest(refs["gap_manifest_ref"])
    if "quality_ref" in refs:
        _read_json_ref(refs["quality_ref"], "source.quality_ref")

    if not isinstance(source["coverage"], dict):
        raise _invalid("source.coverage", "must be an object")
    if not isinstance(source["format"], str):
        raise _invalid("source.format", "must be a string")

    manifest_ref = refs["manifest_ref"]
    manifest_payload = _read_ref(manifest_ref, "source.manifest_ref")
    try:
        manifest = json.loads(manifest_payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise _invalid("source.manifest_ref", "must reference valid JSON") from exc
    manifest = _object(manifest,
                       required={"schema_version", "source_id", "version", "content_hash",
                                 "data_artifacts", "bindings"},
                       optional=set(), path="source_manifest")
    if manifest["schema_version"] != "data_source_manifest_v1":
        raise _invalid("source.manifest_ref", "must reference data_source_manifest_v1")
    if manifest["source_id"] != source_id or manifest["version"] != source["version"]:
        raise _invalid("source.manifest_ref", "source id/version do not match the source document")
    if not isinstance(manifest["data_artifacts"], list) or not manifest["data_artifacts"]:
        raise _invalid("source_manifest.data_artifacts", "must be a non-empty array")
    data_artifacts = [_validate_ref(item, f"source_manifest.data_artifacts[{index}]")
                      for index, item in enumerate(manifest["data_artifacts"])]
    artifact_keys = [(item["uri"], item["content_hash"]) for item in data_artifacts]
    if len(set(artifact_keys)) != len(artifact_keys):
        raise _invalid("source_manifest.data_artifacts", "references must be unique")
    computed_content_hash = _sha256(canonical_json(data_artifacts).encode("utf-8"))
    if source["content_hash"] != computed_content_hash or manifest["content_hash"] != computed_content_hash:
        raise _invalid("source.content_hash", "must match the canonical data artifact identity")
    for index, artifact_ref in enumerate(data_artifacts):
        artifact_path = _local_path(artifact_ref, f"source_manifest.data_artifacts[{index}]")
        try:
            artifact_path.stat()
        except OSError as exc:
            raise _invalid(f"source_manifest.data_artifacts[{index}]",
                           f"cannot stat referenced data artifact: {exc}") from exc
        if _file_sha256(artifact_path, f"source_manifest.data_artifacts[{index}]") != \
                artifact_ref["content_hash"]:
            raise _invalid(f"source_manifest.data_artifacts[{index}]",
                           "referenced data artifact content hash does not match")

    binding_fields = ("schema_ref", "calendar_ref", "session_policy_ref", "access_policy_ref",
                      "market_metadata_ref", "gap_manifest_ref", "quality_ref")
    bindings = {field: refs[field] for field in binding_fields if field in refs}
    if manifest["bindings"] != bindings:
        raise _invalid("source_manifest.bindings", "must exactly match DataSource artifact refs")

    result = dict(source)
    result["manifest_ref"] = manifest_ref
    result["content_hash"] = computed_content_hash
    result["schema_ref"] = refs["schema_ref"]
    result["calendar_ref"] = refs["calendar_ref"]
    result["session_policy_ref"] = refs["session_policy_ref"]
    result["access_policy_ref"] = refs["access_policy_ref"]
    for optional_ref in ("market_metadata_ref", "gap_manifest_ref", "quality_ref"):
        if optional_ref in refs:
            result[optional_ref] = refs[optional_ref]
    result["created_by"] = created_by
    result["instruments"] = instruments
    result["coverage"] = coverage
    result["time_rules"] = dict(time_rules)
    return result


def source_calendar(source: dict[str, Any]) -> tuple[set[str], str]:
    calendar = _read_json_ref(source["calendar_ref"], "source.calendar_ref")
    return set(calendar["trading_dates"]), calendar["timezone"]


def source_access_policy(source: dict[str, Any]) -> dict[str, Any]:
    return _read_json_ref(source["access_policy_ref"], "source.access_policy_ref")


def _scope(value: Any, path: str) -> dict[str, Any]:
    scope = _object(value, required={"instruments", "start", "end"},
                    optional={"session_filter"}, path=path)
    instruments = _string_list(scope["instruments"], f"{path}.instruments")
    start = _timestamp(scope["start"], f"{path}.start")
    end = _timestamp(scope["end"], f"{path}.end")
    if start >= end:
        raise _invalid(path, "start must be before end")
    result = {"instruments": instruments, "start": start.isoformat(), "end": end.isoformat()}
    if "session_filter" in scope:
        result["session_filter"] = _string_list(scope["session_filter"], f"{path}.session_filter")
    return result


def _limits(value: Any) -> dict[str, Any]:
    limits = _object(value, required={"allowed_steps", "allowed_scope", "budget"},
                     optional=set(), path="study.limits")
    steps = limits["allowed_steps"]
    if not isinstance(steps, list) or not steps or any(type(step) is not int for step in steps):
        raise _invalid("study.limits.allowed_steps", "must be a non-empty array of step integers")
    if len(set(steps)) != len(steps) or any(step not in {3, 4, 5} for step in steps):
        raise _invalid("study.limits.allowed_steps", "values must be unique members of 3, 4, 5")
    if not isinstance(limits["allowed_scope"], list):
        raise _invalid("study.limits.allowed_scope", "must be an array of scopes")
    scopes = [_scope(item, f"study.limits.allowed_scope[{index}]")
              for index, item in enumerate(limits["allowed_scope"])]
    budget = _object(limits["budget"], required=set(),
                     optional={"max_work_units", "max_partitions", "max_cpu_seconds",
                               "max_memory_bytes", "max_concurrent_jobs"},
                     path="study.limits.budget")
    if not budget:
        raise _invalid("study.limits.budget", "must declare at least one positive resource limit")
    for key, budget_value in budget.items():
        if type(budget_value) is not int or budget_value < 1:
            raise _invalid(f"study.limits.budget.{key}", "must be a positive integer")
    return {"allowed_steps": steps, "allowed_scope": scopes, "budget": dict(budget)}


def validate_study_document(document: Any) -> dict[str, Any]:
    required = {"schema_version", "study_id", "name", "objective", "status", "source_refs",
                "feature_set_refs", "backlog", "limits", "created_by", "change_reason"}
    optional = {"default_feature_set_ref", "default_scope", "evaluation_guide", "notes", "tags"}
    study = _object(document, required=required, optional=optional, path="study")
    if study["schema_version"] != "study_v1":
        raise _invalid("study.schema_version", "must be study_v1")
    study_id = _text(study["study_id"], "study.study_id")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", study_id):
        raise _invalid("study.study_id", "must be a stable identifier")
    for field in ("name", "objective", "change_reason"):
        _text(study[field], f"study.{field}")
    if (not isinstance(study["status"], str)
            or study["status"] not in {"active", "paused", "closed"}):
        raise _invalid("study.status", "must be active, paused, or closed")
    source_refs = _string_list(study["source_refs"], "study.source_refs")
    for index, ref in enumerate(source_refs):
        parse_versioned_ref(ref, f"study.source_refs[{index}]")
    set_refs = _string_list(study["feature_set_refs"], "study.feature_set_refs", nonempty=False)
    for index, ref in enumerate(set_refs):
        parse_versioned_ref(ref, f"study.feature_set_refs[{index}]")
    backlog = study["backlog"]
    if not isinstance(backlog, list):
        raise _invalid("study.backlog", "must be an array")
    normalized_backlog = []
    seen_backlog = set()
    for index, item in enumerate(backlog):
        path = f"study.backlog[{index}]"
        item = _object(item, required={"feature_set_ref", "item_id", "priority", "state", "reason"},
                       optional=set(), path=path)
        set_ref = _text(item["feature_set_ref"], f"{path}.feature_set_ref")
        parse_versioned_ref(set_ref, f"{path}.feature_set_ref")
        if set_ref not in set_refs:
            raise _invalid(f"{path}.feature_set_ref", "must appear in study.feature_set_refs")
        item_id = _text(item["item_id"], f"{path}.item_id")
        if type(item["priority"]) is not int or item["priority"] < 1:
            raise _invalid(f"{path}.priority", "must be a positive integer")
        if (not isinstance(item["state"], str)
                or item["state"] not in {"active", "parked", "retired"}):
            raise _invalid(f"{path}.state", "must be active, parked, or retired")
        reason = _text(item["reason"], f"{path}.reason")
        key = (set_ref, item_id)
        if key in seen_backlog:
            raise _invalid(path, "feature_set_ref/item_id must be unique")
        seen_backlog.add(key)
        normalized_backlog.append({"feature_set_ref": set_ref, "item_id": item_id,
                                   "priority": item["priority"], "state": item["state"],
                                   "reason": reason})
    limits = _limits(study["limits"])
    result = {"schema_version": "study_v1", "study_id": study_id,
              "name": study["name"], "objective": study["objective"], "status": study["status"],
              "source_refs": source_refs, "feature_set_refs": set_refs,
              "backlog": normalized_backlog, "limits": limits,
              "created_by": _actor(study["created_by"], "study.created_by"),
              "change_reason": study["change_reason"]}
    if "default_feature_set_ref" in study:
        default_ref = _text(study["default_feature_set_ref"], "study.default_feature_set_ref")
        parse_versioned_ref(default_ref, "study.default_feature_set_ref")
        if default_ref not in set_refs:
            raise _invalid("study.default_feature_set_ref", "must appear in study.feature_set_refs")
        result["default_feature_set_ref"] = default_ref
    if "default_scope" in study:
        result["default_scope"] = _scope(study["default_scope"], "study.default_scope")
    if "evaluation_guide" in study:
        if not isinstance(study["evaluation_guide"], dict):
            raise _invalid("study.evaluation_guide", "must be an object")
        result["evaluation_guide"] = study["evaluation_guide"]
    if "notes" in study:
        result["notes"] = _text(study["notes"], "study.notes")
    if "tags" in study:
        result["tags"] = _string_list(study["tags"], "study.tags", nonempty=False)
    try:
        canonical_json(result)
    except (TypeError, ValueError) as exc:
        raise _invalid("study", f"contains a non-JSON value: {exc}") from exc
    return result


def scope_is_within(requested: dict[str, Any], allowed: dict[str, Any]) -> bool:
    req_start = _timestamp(requested["start"], "scope.start")
    req_end = _timestamp(requested["end"], "scope.end")
    allow_start = _timestamp(allowed["start"], "allowed_scope.start")
    allow_end = _timestamp(allowed["end"], "allowed_scope.end")
    if not set(requested["instruments"]).issubset(allowed["instruments"]):
        return False
    if req_start < allow_start or req_end > allow_end:
        return False
    allowed_sessions = allowed.get("session_filter")
    requested_sessions = requested.get("session_filter")
    return allowed_sessions is None or (
        requested_sessions is not None and set(requested_sessions).issubset(allowed_sessions)
    )


def study_scope_is_covered(scope: dict[str, Any], sources: list[dict[str, Any]]) -> bool:
    start = _timestamp(scope["start"], "scope.start")
    end = _timestamp(scope["end"], "scope.end")
    for instrument in scope["instruments"]:
        candidates = [source for source in sources if instrument in source["instruments"]]
        if not candidates:
            return False
        timezones = {source["time_rules"]["timezone"] for source in candidates}
        if len(timezones) != 1:
            return False
        timezone = ZoneInfo(next(iter(timezones)))
        observed_dates = [date.fromisoformat(part["trade_date"])
                          for source in candidates for part in source["coverage"]["partitions"]
                          if part["instrument_id"] == instrument]
        calendar_dates = [date.fromisoformat(day)
                          for source in candidates for day in source_calendar(source)[0]]
        if not observed_dates or not calendar_dates:
            return False
        # Exact partition inventories retain internal gaps. A Study may include those dates so
        # later diagnostics can represent them; only bounds outside the known source/calendar
        # extent are rejected here.
        first_day, last_day = min(observed_dates), max(observed_dates)
        extent_start = datetime.combine(first_day, datetime.min.time(), tzinfo=timezone)
        extent_end = datetime.combine(last_day + timedelta(days=1), datetime.min.time(),
                                      tzinfo=timezone)
        for source in candidates:
            policy = _read_json_ref(source["session_policy_ref"], "source.session_policy_ref")
            for interval in policy.get("intervals", []):
                if interval["instrument_id"] != instrument:
                    continue
                interval_day = date.fromisoformat(interval["trade_date"])
                if not first_day <= interval_day <= last_day:
                    continue
                extent_start = min(extent_start, _timestamp(interval["start"], "session.start"))
                extent_end = max(extent_end, _timestamp(interval["end"], "session.end"))
        if start < extent_start or end > extent_end:
            return False
        sessions = scope.get("session_filter")
        if sessions is not None:
            known_sessions = {session for source in candidates
                              for part in source["coverage"]["partitions"]
                              if part["instrument_id"] == instrument
                              for session in part["session_ids"]}
            if not set(sessions).issubset(known_sessions):
                return False
    return True
