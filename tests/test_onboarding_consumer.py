"""Consumer-side checks against small, real serialized onboarding artifacts."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from datetime import time as clock_time
from decimal import Decimal
from itertools import pairwise
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import verify_onboarding as consumer

REPO = Path(__file__).resolve().parents[1]
SOURCE_SCHEMA = REPO / "docs/references/gov_bond_l2.schema.json"
RUN_ID = "run_fixture"
STAGE_RUN_ID = "stage_run_fixture"
CONFIG_HASH = "sha256:" + "1" * 64
CODE_REVISION = "sha256:" + "2" * 64
SNAPSHOT_ID = "snapshot_fixture"
SPLIT_ID = None
RAW_FILE_HASH = "a" * 64
TRADE_DATE = date(2026, 1, 5)
TIMES = [
    datetime(2026, 1, 5, 1, 30, 0, tzinfo=UTC),
    datetime(2026, 1, 5, 1, 30, 1, tzinfo=UTC),
]


def _record_id(row_index: int) -> str:
    raw = f"{RAW_FILE_HASH}:{row_index}".encode()
    return hashlib.sha256(raw).hexdigest()


def _default_value(dtype: pa.DataType) -> Any:
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return ""
    if pa.types.is_decimal(dtype):
        return Decimal(0).quantize(Decimal(1).scaleb(-dtype.scale))
    if pa.types.is_integer(dtype):
        return 0
    if pa.types.is_floating(dtype):
        return 0.0
    if pa.types.is_boolean(dtype):
        return False
    if pa.types.is_date(dtype):
        return TRADE_DATE
    if pa.types.is_timestamp(dtype):
        if dtype.tz:
            return TIMES[0]
        return datetime.fromisoformat("2026-01-05T09:30:00")
    if pa.types.is_list(dtype):
        return []
    raise AssertionError(f"fixture needs a default for Arrow type {dtype}")


def _mapping() -> dict[str, str]:
    result = {
        "TRADINGDATE": "trade_date",
        "TRADINGTIME": "event_time",
        "SYMBOL": "instrument_id",
        "MARKET": "exchange",
        "LASTPRICE": "last_price",
        "TOTALPOSITION": "open_interest",
        "TOTALVOLUME": "volume",
        "TOTALAMOUNT": "turnover",
        "TRADEVOLUME": "volume_increment",
        "TRADEAMOUNT": "turnover_increment",
        "UNIX": "source_event_unix_ms",
    }
    for level in range(1, 6):
        suffix = f"{level:02d}"
        result.update({
            f"BUYPRICE{suffix}": f"bid_price_{level}",
            f"SELLPRICE{suffix}": f"ask_price_{level}",
            f"BUYVOLUME{suffix}": f"bid_size_{level}",
            f"SELLVOLUME{suffix}": f"ask_size_{level}",
        })
    return result


def _alternate_depth_invalid_expression(alias: str, *, standardized: bool) -> str:
    del alias, standardized
    return "FALSE"


def _alternate_expiry_date(
    instrument_id: str, trade_date: date, calendar_dates: set[date]
) -> date | None:
    del instrument_id, trade_date, calendar_dates
    return None


def _alternate_sql_ident(value: str) -> str:
    return value


def _quote_schema_and_rows(row_count: int = 2, *, invalid_l1_at: int | None = None,
                          invalid_size_at: int | None = None,
                          invalid_depth_at: int | None = None,
                          wrong_session_at: int | None = None,
                          unsorted_at: int | None = None
                          ) -> tuple[pa.Schema, list[dict[str, Any]]]:
    schema_document = json.loads(SOURCE_SCHEMA.read_text(encoding="utf-8"))
    mapping = _mapping()
    names: list[str] = []
    types: dict[str, pa.DataType] = {}
    for field in schema_document["sample"]["fields"]:
        output_name = mapping.get(field["name"], field["name"])
        output_type = consumer._arrow_type(field["arrow_type"])
        if output_name == "event_time":
            output_type = pa.timestamp("us", tz="UTC")
        elif output_name == "trade_date":
            output_type = pa.date32()
        elif output_name == "source_event_unix_ms":
            output_type = pa.int64()
        names.append(output_name)
        types[output_name] = output_type
    types.update(consumer.QUOTE_DERIVED_COLUMNS)
    fields = [pa.field(name, types[name]) for name in names]
    fields.extend(pa.field(name, dtype) for name, dtype in consumer.QUOTE_DERIVED_COLUMNS.items()
                  if name not in names)
    schema = pa.schema(fields)
    rows: list[dict[str, Any]] = []
    for index in range(row_count):
        row = {name: _default_value(dtype) for name, dtype in types.items()}
        event_time = TIMES[0] + timedelta(seconds=index)
        if index == wrong_session_at:
            event_time = datetime(2026, 1, 5, 8, 0, 0, tzinfo=UTC)
        if index == unsorted_at:
            event_time = TIMES[0] + timedelta(milliseconds=500)
        unix_ms = int(event_time.timestamp() * 1000)
        row.update({
            "trade_date": TRADE_DATE,
            "event_time": event_time,
            "instrument_id": "T2603",
            "exchange": "CFFEX",
            "source_event_unix_ms": unix_ms,
            "bid_price_1": Decimal("100.000"),
            "ask_price_1": Decimal("100.250"),
            "bid_size_1": Decimal("10.00"),
            "ask_size_1": Decimal("5.00"),
            "session_id": "CN_STOCK:2026-01-05:continuous_morning",
            "available_time": None,
            "quality_flags": [],
            "raw_file_sha256": RAW_FILE_HASH,
            "raw_row_index": index,
            "raw_row_group_index": 0,
            "record_id": _record_id(index),
        })
        for level in range(2, 6):
            row[f"bid_price_{level}"] = Decimal(f"{99.750 - (level - 2) * 0.250:.3f}")
            row[f"ask_price_{level}"] = Decimal(f"{100.500 + (level - 2) * 0.250:.3f}")
            row[f"bid_size_{level}"] = Decimal("12.00")
            row[f"ask_size_{level}"] = Decimal("11.00")
        if index == invalid_l1_at:
            row["bid_price_1"] = Decimal("0.000")
        if index == invalid_size_at:
            row["bid_size_1"] = Decimal("-1.00")
        if index == invalid_depth_at:
            row["bid_price_2"] = Decimal("0.000")
            row["quality_flags"] = ["DEPTH_BOOK_INVALID"]
        rows.append(row)
    return schema, rows


def _quarantine_schema() -> pa.Schema:
    schema_document = json.loads(SOURCE_SCHEMA.read_text(encoding="utf-8"))
    fields = [pa.field(field["name"], consumer._arrow_type(field["arrow_type"]))
              for field in schema_document["sample"]["fields"]]
    fields.extend([
        pa.field("record_id", pa.string()),
        pa.field("raw_file_sha256", pa.string()),
        pa.field("raw_row_index", pa.int64()),
        pa.field("raw_row_group_index", pa.int32()),
        pa.field("reason_codes", pa.list_(pa.string())),
    ])
    return pa.schema(fields)


def _fill_valid_book(row: dict[str, Any], *, standardized: bool) -> None:
    for level in range(1, 6):
        suffix = f"_{level}" if standardized else f"{level:02}"
        names = ("bid_price", "ask_price", "bid_size", "ask_size") if standardized else (
            "BUYPRICE", "SELLPRICE", "BUYVOLUME", "SELLVOLUME",
        )
        row[names[0] + suffix] = Decimal("100.000")
        row[names[1] + suffix] = Decimal("100.250")
        row[names[2] + suffix] = Decimal("10.00")
        row[names[3] + suffix] = Decimal("5.00")


def _fixture_session_id(row: dict[str, Any]) -> str | None:
    if any(row.get(key) is None for key in ("MARKET", "SYMBOL", "TRADINGDATE", "TRADINGTIME")):
        return None
    local_time = row["TRADINGTIME"].time()
    if clock_time(9, 29) <= local_time < clock_time(9, 30):
        segment = "opening_auction_matching"
    elif clock_time(9, 30) <= local_time < clock_time(11, 30):
        segment = "continuous_morning"
    elif clock_time(13) <= local_time < clock_time(15, 15):
        segment = "continuous_afternoon"
    else:
        return None
    return f"CN_STOCK:{row['TRADINGDATE']}:{segment}"


def _fixture_source_rows(quote_rows: list[dict[str, Any]],
                         quarantine_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    schema_document = json.loads(SOURCE_SCHEMA.read_text(encoding="utf-8"))
    fields = [field["name"] for field in schema_document["sample"]["fields"]]
    mapping = _mapping()
    source_rows: list[dict[str, Any]] = []
    local_tz = ZoneInfo("Asia/Shanghai")
    for index, row in enumerate(quote_rows):
        values = {}
        for name in fields:
            output_name = mapping.get(name, name)
            value = row.get(output_name)
            if name == "TRADINGTIME" and value is not None:
                value = value.astimezone(local_tz).replace(tzinfo=None)
            values[name] = value
        source_rows.append({
            **values, "raw_file_sha256": RAW_FILE_HASH, "raw_row_index": row["raw_row_index"],
            "row_kind": "quotes", "reason_codes": [], "session_id": row["session_id"],
        })
    for row in quarantine_rows:
        source_rows.append({
            **{name: row.get(name) for name in fields},
            "raw_file_sha256": RAW_FILE_HASH, "raw_row_index": row["raw_row_index"],
            "row_kind": "quarantine", "reason_codes": row["reason_codes"],
            "session_id": _fixture_session_id(row),
        })
    return source_rows


def _fixture_rate(numerator: int, denominator: int, formula: str) -> dict[str, Any]:
    return {
        "numerator": numerator,
        "denominator": denominator,
        "value": numerator / denominator if denominator else None,
        "formula": formula,
    }


def _fixture_quality_scopes(quote_rows: list[dict[str, Any]],
                            quarantine_rows: list[dict[str, Any]],
                            source_format: str) -> tuple[list[dict[str, Any]],
                                                                            list[dict[str, Any]],
                                                                            list[dict[str, Any]]]:
    schema_document = json.loads(SOURCE_SCHEMA.read_text(encoding="utf-8"))
    field_names = [field["name"] for field in schema_document["sample"]["fields"]]
    source_rows = _fixture_source_rows(quote_rows, quarantine_rows)
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in source_rows:
        key = (row["raw_file_sha256"], row.get("MARKET"), row.get("SYMBOL"), row.get("TRADINGDATE"))
        grouped.setdefault(key, []).append(row)
    source_uri = f"file:///fixture/raw.{source_format}"
    scopes: list[dict[str, Any]] = []
    for (raw_hash, exchange, instrument, trade_day), rows in grouped.items():
        row_count = len(rows)
        accepted = sum(row["row_kind"] == "quotes" for row in rows)
        quarantined = row_count - accepted
        duplicate_groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for row in rows:
            key = (row.get("MARKET"), row.get("SYMBOL"), row.get("TRADINGDATE"), row.get("TRADINGTIME"))
            if all(value is not None for value in key):
                duplicate_groups.setdefault(key, []).append(row)
        duplicate_groups = {key: values for key, values in duplicate_groups.items() if len(values) > 1}
        duplicate_extra = sum(len(values) - 1 for values in duplicate_groups.values())
        duplicate_members = sum(len(values) for values in duplicate_groups.values())
        invalid_l1_rows = sum(
            row["BUYPRICE01"] is None or row["SELLPRICE01"] is None
            or row["BUYVOLUME01"] is None or row["SELLVOLUME01"] is None
            or row["BUYPRICE01"] <= 0 or row["SELLPRICE01"] <= 0
            or row["BUYVOLUME01"] < 0 or row["SELLVOLUME01"] < 0
            or row["BUYPRICE01"] > row["SELLPRICE01"]
            for row in rows
        )
        valid_price_pairs = [
            row for row in rows if row["BUYPRICE01"] is not None and row["SELLPRICE01"] is not None
            and row["BUYPRICE01"] > 0 and row["SELLPRICE01"] > 0
        ]
        crossed_rows = sum(row["BUYPRICE01"] > row["SELLPRICE01"] for row in valid_price_pairs)
        null_values = sum(row.get(field) is None for row in rows for field in field_names)
        depths = []
        for row in rows:
            bad = False
            for level in range(2, 6):
                suffix = f"{level:02d}"
                values = [row[f"{prefix}{suffix}"] for prefix in
                          ("BUYPRICE", "SELLPRICE", "BUYVOLUME", "SELLVOLUME")]
                if (any(value is None for value in values) or values[0] <= 0 or values[1] <= 0
                        or values[2] < 0 or values[3] < 0):
                    bad = True
            depths.append(bad)
        depth_source = sum(depths)
        depth_accepted = sum(bad for bad, row in zip(depths, rows, strict=True)
                             if row["row_kind"] == "quotes")
        unclassified = sum(row["session_id"] is None for row in rows)
        by_order: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for row in rows:
            order_key = (row.get("ingest_run_id"), row.get("source_file"), row.get("SYMBOL"),
                         row.get("TRADINGDATE"))
            by_order.setdefault(order_key, []).append(row)
        reorder_count = 0
        for ordered in by_order.values():
            ordered.sort(key=lambda row: row["raw_row_index"])
            reorder_count += sum(
                prior["TRADINGTIME"] is not None and current["TRADINGTIME"] is not None
                and current["TRADINGTIME"] < prior["TRADINGTIME"]
                for prior, current in pairwise(ordered)
            )
        null_denominator = row_count * len(field_names)
        scopes.append({
            "scope": "raw_file/contract/trade_date", "raw_file_sha256": raw_hash,
            "source_uri": source_uri, "exchange": exchange, "instrument_id": instrument,
            "trade_date": trade_day.isoformat() if trade_day else None,
            "row_count": row_count, "accepted_rows": accepted, "quarantined_rows": quarantined,
            "quarantine_rate": _fixture_rate(quarantined, row_count,
                                               "quarantined_rows / input row_count"),
            "duplicate_event_groups": len(duplicate_groups),
            "duplicate_event_extra_rows": duplicate_extra,
            "duplicate_member_rows": duplicate_members,
            "duplicate_rate": _fixture_rate(duplicate_extra, row_count,
                                              "sum(event_key_group_size - 1) / input row_count"),
            "invalid_quote_rows": invalid_l1_rows,
            "invalid_quote_rate": _fixture_rate(invalid_l1_rows, row_count,
                                                 "invalid L1 rows / input row_count"),
            "null_field_values": null_values, "null_field_value_denominator": null_denominator,
            "null_rate": _fixture_rate(
                null_values, null_denominator,
                "null source-field values / (input row_count * physical source field count)",
            ),
            "crossed_book_rows": crossed_rows, "valid_price_pair_rows": len(valid_price_pairs),
            "crossed_book_rate": _fixture_rate(
                crossed_rows, len(valid_price_pairs),
                "crossed positive-price pairs / positive non-null L1 price pairs",
            ),
            "time_reorder_count": reorder_count, "time_reorder_denominator": row_count,
            "session_unclassified_rows": unclassified,
            "depth_invalid_rows": depth_accepted, "depth_invalid_source_rows": depth_source,
            "stale_quote_segments": {
                "value": None, "status": "NOT_EVALUATED", "denominator": None,
                "reason": "Availability and update cadence are unverified.",
            },
        })

    session_groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in source_rows:
        key = (row["raw_file_sha256"], row.get("MARKET"), row.get("SYMBOL"),
               row.get("TRADINGDATE"), row.get("session_id"))
        session_groups.setdefault(key, []).append(row)
    sessions = []
    for key, rows in session_groups.items():
        raw_hash, exchange, instrument, trade_day, session_id = key
        sessions.append({
            "scope": "raw_file/contract/trade_date/session", "raw_file_sha256": raw_hash,
            "exchange": exchange, "instrument_id": instrument,
            "trade_date": trade_day.isoformat() if trade_day else None,
            "session_id": session_id,
            "classification": "unclassified" if session_id is None else "evidenced_session",
            "row_count": len(rows),
            "accepted_rows": sum(row["row_kind"] == "quotes" for row in rows),
            "quarantined_rows": sum(row["row_kind"] == "quarantine" for row in rows),
        })

    gaps: list[dict[str, Any]] = []
    eligible: dict[tuple[Any, ...], set[datetime]] = {}
    for row in source_rows:
        event_time = row.get("TRADINGTIME")
        trade_day = row.get("TRADINGDATE")
        if (row["session_id"] is None or event_time is None or trade_day not in
                {date(2026, 1, 5), date(2026, 1, 6)} or event_time.date() != trade_day
                or row.get("UNIX") is None):
            continue
        expected_unix = int(event_time.replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp() * 1000)
        if int(row["UNIX"]) != expected_unix:
            continue
        key = (row["raw_file_sha256"], row.get("MARKET"), row.get("SYMBOL"),
               trade_day, row["session_id"])
        eligible.setdefault(key, set()).add(event_time)
    for key, event_times in eligible.items():
        raw_hash, exchange, instrument, trade_day, session_id = key
        ordered = sorted(event_times)
        deltas = [(right - left).total_seconds() for left, right in pairwise(ordered)
                  if right > left]
        def quantile(probability: float, values: list[float] = deltas) -> float | None:
            if not values:
                return None
            ordered_deltas = sorted(values)
            position = (len(ordered_deltas) - 1) * probability
            low = int(position)
            high = min(low + 1, len(ordered_deltas) - 1)
            fraction = position - low
            return ordered_deltas[low] + (ordered_deltas[high] - ordered_deltas[low]) * fraction
        gaps.append({
            "scope": "raw_file/contract/trade_date/session", "raw_file_sha256": raw_hash,
            "exchange": exchange, "instrument_id": instrument,
            "trade_date": trade_day.isoformat(), "session_id": session_id,
            "unique_valid_event_time_count": len(ordered),
            "positive_interval_count": len(deltas), "sample_count": len(deltas),
            "p50_seconds": quantile(0.5), "p90_seconds": quantile(0.9), "p99_seconds": quantile(0.99),
            "formula": "positive adjacent deltas after distinct valid event_time values, grouped within one raw file and evidenced session",
            "status": "EVALUATED" if deltas else "NOT_EVALUATED",
            "reason": None if deltas else "Fewer than two unique valid event times in this session scope.",
        })
    return scopes, sessions, gaps


def _fixture_duplicate_classification(quote_rows: list[dict[str, Any]],
                                      quarantine_rows: list[dict[str, Any]]) -> dict[str, Any]:
    schema_document = json.loads(SOURCE_SCHEMA.read_text(encoding="utf-8"))
    fields = [field["name"] for field in schema_document["sample"]["fields"]]
    ignored = {"source_file", "source_row", "ingest_run_id"}
    source_rows = _fixture_source_rows(quote_rows, quarantine_rows)
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in source_rows:
        key = (row.get("MARKET"), row.get("SYMBOL"), row.get("TRADINGDATE"), row.get("TRADINGTIME"))
        if all(value is not None for value in key):
            groups.setdefault(key, []).append(row)
    duplicate_groups = [rows for rows in groups.values() if len(rows) > 1]
    identical = []
    conflicting = []
    for rows in duplicate_groups:
        payloads = {
            json.dumps({field: row.get(field) for field in fields if field not in ignored},
                       sort_keys=True, default=str)
            for row in rows
        }
        (identical if len(payloads) == 1 else conflicting).append(rows)
    return {
        "event_key": ["exchange", "instrument_id", "trade_date", "event_time"],
        "policy": "quarantine_entire_group_without_sequence",
        "identical_payload_groups": len(identical),
        "identical_payload_rows": sum(len(rows) for rows in identical),
        "identical_payload_extra_rows": sum(len(rows) - 1 for rows in identical),
        "conflicting_payload_groups": len(conflicting),
        "conflicting_payload_rows": sum(len(rows) for rows in conflicting),
        "conflicting_payload_extra_rows": sum(len(rows) - 1 for rows in conflicting),
        "source_record_duplicate_rows": sum(
            "DUPLICATE_SOURCE_RECORD" in (row.get("reason_codes") or [])
            for row in source_rows
        ),
    }


def _manifest(ref: dict[str, Any]) -> dict[str, Any]:
    return {
        **{key: ref[key] for key in (
            "artifact_id", "artifact_type", "schema_version", "uri", "content_hash",
            "manifest_uri", "run_id", "stage_run_id", "stage_name", "stage_version",
            "cache_key", "partition_keys", "time_min", "time_max", "row_count",
        )},
        "config_hash": CONFIG_HASH,
        "code_revision": CODE_REVISION,
        "input_artifact_ids": ["art_source_snapshot", "art_split_manifest"],
        "data_snapshot_id": SNAPSHOT_ID,
        "split_id": SPLIT_ID,
        "quality_flags": [],
        "created_at": "2026-11-02T00:00:00+00:00",
    }


def _publish_artifact(
    root: Path,
    name: str,
    payload_path: Path,
    *,
    artifact_type: str,
    row_count: int | None,
    partition_keys: dict[str, str] | None = None,
    time_min: str | None = None,
    time_max: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    payload_dir = root / name
    payload_dir.mkdir(parents=True, exist_ok=True)
    payload = payload_dir / payload_path.name
    payload.write_bytes(payload_path.read_bytes())
    content_hash = consumer.hash_path(payload)
    ref = {
        "artifact_id": "art_" + content_hash.removeprefix("sha256:"),
        "artifact_type": artifact_type,
        "schema_version": "1.0.0",
        "uri": payload.as_uri(),
        "content_hash": content_hash,
        "manifest_uri": (payload_dir / "manifest.json").as_uri(),
        "partition_keys": partition_keys or {},
        "row_count": row_count,
        "time_min": time_min,
        "time_max": time_max,
        "run_id": RUN_ID,
        "stage_run_id": STAGE_RUN_ID,
        "stage_name": "onboard",
        "stage_version": "1.0.0",
        "cache_key": "sha256:" + "3" * 64,
    }
    manifest = _manifest(ref)
    (payload_dir / "manifest.json").write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return ref, manifest


def _refresh_artifact(root: Path, ref: dict[str, Any], entry: dict[str, Any]) -> None:
    payload = Path(ref["uri"].removeprefix("file://"))
    ref["content_hash"] = consumer.hash_path(payload)
    ref["artifact_id"] = "art_" + ref["content_hash"].removeprefix("sha256:")
    manifest = _manifest(ref)
    Path(ref["manifest_uri"].removeprefix("file://")).write_text(
        json.dumps(manifest, sort_keys=True), encoding="utf-8"
    )
    for alias, refs in entry["artifacts"].items():
        if isinstance(refs, list):
            entry["artifacts"][alias] = [ref if old.get("uri") == ref["uri"] else old for old in refs]
        elif refs.get("uri") == ref["uri"]:
            entry["artifacts"][alias] = ref
    for part in entry["partitions"]:
        if part["artifact_ref"].get("uri") == ref["uri"]:
            part["artifact_ref"] = ref
    entry["artifact_manifests"] = {
        key: (manifest if value.get("uri") == ref["uri"] else value)
        for key, value in entry["artifact_manifests"].items()
    }
    entry_path = root / "onboarding.json"
    entry_path.write_text(json.dumps(entry, sort_keys=True), encoding="utf-8")


def _rewrite_quarantine_rows(
    root: Path, ref: dict[str, Any], entry: dict[str, Any], mutate: Any
) -> None:
    payload = consumer.file_uri_path(ref["uri"])
    table = pq.read_table(payload)
    rows = table.to_pylist()
    mutate(rows)
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), payload)
    _refresh_artifact(root, ref, entry)


def _change_quarantine_reason_count(
    entry: dict[str, Any], old_reason: str, new_reason: str
) -> None:
    counts = entry["quality_report"]["quarantine_reason_counts"]
    counts[old_reason] -= 1
    if counts[old_reason] == 0:
        del counts[old_reason]
    counts[new_reason] = counts.get(new_reason, 0) + 1
    for rule in entry["quality_report"]["rules"]:
        if rule["rule_id"] == old_reason:
            rule["hit_count"] -= 1
            rule["outcome"] = "WARN" if rule["hit_count"] else "PASS"
        elif rule["rule_id"] == new_reason:
            rule["hit_count"] += 1
            rule["outcome"] = "WARN"


def make_fixture(root: Path, *, duplicate_in_quarantine: bool = False,
                 product_only_conflict: bool = False,
                 missing_required_quarantine: bool = False,
                 source_format: str = "parquet", row_count: int = 2,
                 invalid_l1_at: int | None = None, invalid_size_at: int | None = None,
                 invalid_depth_at: int | None = None,
                 wrong_session_at: int | None = None, unsorted_at: int | None = None
                 ) -> tuple[Path, dict[str, Any], dict[str, dict[str, Any]]]:
    root.mkdir(parents=True, exist_ok=True)
    schema_hash = consumer.sha256_file(SOURCE_SCHEMA)
    source_timezone = "Asia/Shanghai"
    evidence_path = REPO / "configs/reference/cffex_calendar_evidence.json"
    evidence_hash = consumer.sha256_file(evidence_path)

    quote_schema, quote_rows = _quote_schema_and_rows(
        row_count, invalid_l1_at=invalid_l1_at, invalid_size_at=invalid_size_at,
        invalid_depth_at=invalid_depth_at,
        wrong_session_at=wrong_session_at, unsorted_at=unsorted_at,
    )
    quote_file = root / "quote.parquet"
    quote_table = pa.Table.from_pylist(quote_rows, schema=quote_schema)
    if source_format == "csv":
        quote_table = quote_table.set_column(
            quote_table.schema.get_field_index("raw_row_group_index"), "raw_row_group_index",
            pa.array([None] * len(quote_rows), type=pa.int32()),
        )
    pq.write_table(quote_table, quote_file)
    q_schema = _quarantine_schema()
    q_file = root / "quarantine.parquet"
    quarantine_rows: list[dict[str, Any]] = []
    if duplicate_in_quarantine:
        quarantine_row = {field.name: _default_value(field.type) for field in q_schema}
        _fill_valid_book(quarantine_row, standardized=False)
        source_fields = json.loads(SOURCE_SCHEMA.read_text(encoding="utf-8"))["sample"]["fields"]
        local_tz = ZoneInfo("Asia/Shanghai")
        for field in source_fields:
            source_name = field["name"]
            output_name = _mapping().get(source_name, source_name)
            value = quote_rows[0].get(output_name)
            if source_name == "TRADINGTIME" and value is not None:
                value = value.astimezone(local_tz).replace(tzinfo=None)
            quarantine_row[source_name] = value
        if product_only_conflict:
            quarantine_row["product"] = "TF"
        quarantine_row.update({
            "record_id": _record_id(2),
            "raw_file_sha256": RAW_FILE_HASH,
            "raw_row_index": 2,
            "raw_row_group_index": 0,
            "reason_codes": ["DUPLICATE_EVENT_KEY_CONFLICTING" if product_only_conflict
                              else "DUPLICATE_EVENT_KEY_IDENTICAL"],
        })
        quarantine_rows.append(quarantine_row)
    if missing_required_quarantine:
        quarantine_row = {field.name: _default_value(field.type) for field in q_schema}
        _fill_valid_book(quarantine_row, standardized=False)
        quarantine_row.update({
            "TRADINGDATE": None,
            "TRADINGTIME": None,
            "SYMBOL": None,
            "MARKET": None,
            "record_id": _record_id(len(quote_rows) + len(quarantine_rows)),
            "raw_file_sha256": RAW_FILE_HASH,
            "raw_row_index": len(quote_rows) + len(quarantine_rows),
            "raw_row_group_index": 0,
            "reason_codes": ["MISSING_REQUIRED_FIELD"],
        })
        quarantine_rows.append(quarantine_row)
    pq.write_table(pa.Table.from_pylist(quarantine_rows, schema=q_schema), q_file)
    calendar_file = root / "calendar.parquet"
    pq.write_table(pa.table({
        "calendar_id": pa.array(["CN_STOCK", "CN_STOCK"], type=pa.string()),
        "trade_date": pa.array([date(2026, 1, 5), date(2026, 1, 6)], type=pa.date32()),
    }), calendar_file)
    rules_file = root / "session_rules.json"
    session_rules = {
        "schema_version": "1.0.0",
        "version": "1.0.0",
        "timezone": source_timezone,
        "interval_convention": "[start, end)",
        "last_trading_day_rule": "fixture only",
        "rules": [{
            "rule_id": "fixture_t",
            "products": ["T"],
            "effective_from": "2020-01-01",
            "effective_for_scope_end": "2030-12-31",
            "opening_auction": {
                "start": "09:25:00", "order_submission_end": "09:29:00",
                "matching_start": "09:29:00", "end": "09:30:00",
            },
            "morning_continuous": {"start": "09:30:00", "end": "11:30:00"},
            "afternoon_continuous_regular_day": {"start": "13:00:00", "end": "15:15:00"},
            "last_trading_day_afternoon": "closed",
        }],
        "sample_classifications": [{"local_time": "09:29", "classification": "auction"}],
        "source_refs": ["fixture"],
        "evidence_uri": evidence_path.relative_to(REPO).as_posix(),
        "evidence_sha256": evidence_hash,
    }
    rules_file.write_text(json.dumps(session_rules, sort_keys=True), encoding="utf-8")

    artifact_root = root / "artifacts"
    part_keys = {"exchange": "CFFEX", "instrument_id": "T2603", "trade_date": "2026-01-05"}
    quote_ref, quote_manifest = _publish_artifact(
        artifact_root, "quotes_T2603_2026-01-05", quote_file,
        artifact_type="quotes.parquet", row_count=len(quote_rows), partition_keys=part_keys,
        time_min=min(row["event_time"] for row in quote_rows).isoformat(),
        time_max=max(row["event_time"] for row in quote_rows).isoformat(),
    )
    quarantine_ref, quarantine_manifest = _publish_artifact(
        artifact_root, "quarantine", q_file, artifact_type="quarantine.parquet",
        row_count=len(quarantine_rows),
    )
    calendar_ref, calendar_manifest = _publish_artifact(
        artifact_root, "calendar", calendar_file, artifact_type="calendar.parquet", row_count=2,
    )
    session_ref, session_manifest = _publish_artifact(
        artifact_root, "session_rules", rules_file, artifact_type="session_rules.json", row_count=None,
    )

    reason_counts: dict[str, int] = {}
    for row in quarantine_rows:
        for reason in row["reason_codes"] or []:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
    def depth_invalid(row: dict[str, Any], *, standardized: bool) -> bool:
        prefixes = (
            ("bid_price", "ask_price", "bid_size", "ask_size") if standardized
            else ("BUYPRICE", "SELLPRICE", "BUYVOLUME", "SELLVOLUME")
        )
        for level in range(2, 6):
            suffix = f"_{level}" if standardized else f"{level:02}"
            bid_price, ask_price, bid_size, ask_size = (row[name + suffix] for name in prefixes)
            if (bid_price is None or ask_price is None or bid_size is None or ask_size is None
                    or bid_price <= 0 or ask_price <= 0 or bid_size < 0 or ask_size < 0):
                return True
        return False

    depth_hits = sum(depth_invalid(row, standardized=True) for row in quote_rows)
    rule_counts = {reason: reason_counts.get(reason, 0) for reason in consumer.QUARANTINE_REASON_CODES}
    rule_counts["DEPTH_BOOK_INVALID"] = depth_hits
    quality_rules = [{
        "rule_id": rule_id,
        "scope": "accepted_quote_row" if rule_id == "DEPTH_BOOK_INVALID" else "input_row",
        "outcome": "WARN" if hit_count else "PASS",
        "hit_count": hit_count,
        "denominator": len(quote_rows) if rule_id == "DEPTH_BOOK_INVALID"
        else len(quote_rows) + len(quarantine_rows),
    } for rule_id, hit_count in rule_counts.items()]
    scopes, session_scopes, gap_summaries = _fixture_quality_scopes(
        quote_rows, quarantine_rows, source_format
    )
    duplicate_classification = _fixture_duplicate_classification(quote_rows, quarantine_rows)
    entry: dict[str, Any] = {
        "schema_version": "1.0.0",
        "stage_name": "onboard",
        "status": "READY_WITH_WARNINGS",
        "run_id": RUN_ID,
        "stage_run_id": STAGE_RUN_ID,
        "config_hash": CONFIG_HASH,
        "code_revision": CODE_REVISION,
        "split_id": SPLIT_ID,
        "data_snapshot_id": SNAPSHOT_ID,
        "inputs": {
            "schema_ref": "docs/references/gov_bond_l2.schema.json",
            "schema_sha256": schema_hash,
            "mapping": _mapping(),
            "source_timezone": source_timezone,
            "raw_files": [{
                "uri": f"file:///fixture/raw.{source_format}", "sha256": "sha256:" + RAW_FILE_HASH,
                "source_format": source_format,
                "size_bytes": 123, "row_count": len(quote_rows) + len(quarantine_rows),
                "row_group_count": 1 if source_format == "parquet" else None,
                "row_group_offsets": [0] if source_format == "parquet" else [],
                "row_group_row_counts": [len(quote_rows) + len(quarantine_rows)]
                if source_format == "parquet" else [],
                "trade_date_min": "2026-01-05", "trade_date_max": "2026-01-05",
                "trade_date_count": 1,
            }],
            "calendar": {
                "provider": "verified_frozen_snapshot_fallback",
                "release": "frozen_snapshot:" + consumer.sha256_file(calendar_file).removeprefix("sha256:"),
                "calendar_id": "CN_STOCK",
                "snapshot_uri": calendar_file.as_uri(),
                "snapshot_sha256": consumer.sha256_file(calendar_file),
                "provenance": {
                    "provider": "verified_frozen_snapshot_fallback",
                    "calendar_id": "CN_STOCK",
                    "snapshot_uri": calendar_file.as_uri(),
                    "snapshot_sha256": consumer.sha256_file(calendar_file),
                    "start": "2026-01-05", "end": "2026-01-06", "row_count": 2,
                    "evidence_uri": evidence_path.as_uri(), "evidence_sha256": evidence_hash,
                },
            },
            "session_evidence_ref": {"uri": evidence_path.as_uri(), "sha256": evidence_hash},
        },
        "passport": {
            "exchange": "CFFEX", "calendar_id": "CN_STOCK", "availability_status": "unknown",
            "research_only": True,
            "instrument_metadata": [{
                "instrument_id": "T2603", "tick_size": None,
                "unknown_reason": "not independently verified",
            }],
        },
        "quality_report": {
            "input_rows": len(quote_rows) + len(quarantine_rows), "accepted_rows": len(quote_rows),
            "quarantined_rows": len(quarantine_rows),
            "expected_missing_dates": ["2026-01-06"], "rules": quality_rules,
            "quarantine_reason_counts": reason_counts,
            "quality_flag_counts": {
                flag: sum(flag in (row["quality_flags"] or []) for row in quote_rows)
                for flag in {flag for row in quote_rows for flag in (row["quality_flags"] or [])}
            },
            "scopes": scopes,
            "session_scopes": session_scopes,
            "event_time_gap_quantiles": gap_summaries,
            "duplicate_classification": duplicate_classification,
        },
        "partitions": [{
            "exchange": "CFFEX", "instrument_id": "T2603", "trade_date": "2026-01-05",
            "row_count": len(quote_rows),
            "event_time_min": min(row["event_time"] for row in quote_rows).isoformat(),
            "event_time_max": max(row["event_time"] for row in quote_rows).isoformat(),
            "artifact_ref": quote_ref,
        }],
        "artifacts": {
            "quotes": [quote_ref], "quarantine": [quarantine_ref],
            "calendar": calendar_ref, "session_rules": session_ref,
        },
        "artifact_manifests": {
            "quotes_T2603_2026-01-05": quote_manifest,
            "quarantine": quarantine_manifest,
            "calendar": calendar_manifest,
            "session_rules": session_manifest,
        },
        "warnings": [{"code": "UNKNOWN_AVAILABILITY", "message": "fixture source has no availability time"}],
    }
    entry_file = root / "onboarding.json"
    entry_file.write_text(json.dumps(entry, sort_keys=True), encoding="utf-8")
    return entry_file, entry, {
        "quotes": quote_ref, "quarantine": quarantine_ref,
        "calendar": calendar_ref, "session_rules": session_ref,
    }


def _write_outer_entry_manifest(entry_file: Path, entry: dict[str, Any]) -> Path:
    content_hash = consumer.sha256_file(entry_file)
    manifest_path = entry_file.parent / "onboarding-manifest.json"
    manifest = {
        "artifact_id": "art_" + content_hash.removeprefix("sha256:"),
        "artifact_type": "onboarding.json",
        "schema_version": "1.0.0",
        "uri": entry_file.as_uri(),
        "content_hash": content_hash,
        "manifest_uri": manifest_path.as_uri(),
        "run_id": entry["run_id"],
        "stage_run_id": entry["stage_run_id"],
        "stage_name": "onboard",
        "stage_version": "1.0.0",
        "cache_key": "sha256:" + "4" * 64,
        "config_hash": entry["config_hash"],
        "code_revision": entry["code_revision"],
        "input_artifact_ids": [],
        "data_snapshot_id": entry["data_snapshot_id"],
        "split_id": entry["split_id"],
        "quality_flags": [],
        "created_at": "2026-11-04T00:00:00+00:00",
        "partition_keys": {},
        "time_min": None,
        "time_max": None,
        "row_count": None,
    }
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return manifest_path


def _refresh_outer_entry_manifest(entry_file: Path, manifest_path: Path) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    content_hash = consumer.sha256_file(entry_file)
    manifest["content_hash"] = content_hash
    manifest["artifact_id"] = "art_" + content_hash.removeprefix("sha256:")
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")


def _add_second_date_quote(
    root: Path,
    entry_file: Path,
    entry: dict[str, Any],
    refs: dict[str, dict[str, Any]],
    *,
    duplicate_row_index: bool = False,
) -> None:
    first_ref = refs["quotes"]
    first_path = Path(first_ref["uri"].removeprefix("file://"))
    first_table = pq.read_table(first_path)
    second = first_table.slice(0, 1).to_pylist()[0]
    trade_day = date(2026, 1, 6)
    event_time = TIMES[0] + timedelta(days=1)
    row_index = 0 if duplicate_row_index else 1
    second.update({
        "trade_date": trade_day,
        "event_time": event_time,
        "source_event_unix_ms": int(event_time.timestamp() * 1000),
        "session_id": "CN_STOCK:2026-01-06:continuous_morning",
        "raw_row_index": row_index,
        "record_id": _record_id(row_index),
    })
    second_file = root / "second-date.parquet"
    pq.write_table(pa.Table.from_pylist([second], schema=first_table.schema), second_file)
    part_keys = {"exchange": "CFFEX", "instrument_id": "T2603", "trade_date": "2026-01-06"}
    quote_ref, quote_manifest = _publish_artifact(
        root / "artifacts", "quotes_T2603_2026-01-06", second_file,
        artifact_type="quotes.parquet", row_count=1, partition_keys=part_keys,
        time_min=event_time.isoformat(), time_max=event_time.isoformat(),
    )
    refs["quotes_second_date"] = quote_ref
    entry["artifacts"]["quotes"].append(quote_ref)
    entry["partitions"].append({
        "exchange": "CFFEX", "instrument_id": "T2603", "trade_date": "2026-01-06",
        "row_count": 1, "event_time_min": event_time.isoformat(),
        "event_time_max": event_time.isoformat(), "artifact_ref": quote_ref,
    })
    entry["artifact_manifests"]["quotes_T2603_2026-01-06"] = quote_manifest
    raw_file = entry["inputs"]["raw_files"][0]
    raw_file.update({
        "row_count": 2, "row_group_offsets": [0], "row_group_row_counts": [2],
        "trade_date_min": "2026-01-05", "trade_date_max": "2026-01-06", "trade_date_count": 2,
    })
    entry["quality_report"].update({
        "input_rows": 2, "accepted_rows": 2, "quarantined_rows": 0,
        "expected_missing_dates": [],
    })
    for rule in entry["quality_report"]["rules"]:
        rule["denominator"] = 2
    quote_rows = first_table.to_pylist() + [second]
    scopes, sessions, gaps = _fixture_quality_scopes(quote_rows, [], "parquet")
    entry["quality_report"]["scopes"] = scopes
    entry["quality_report"]["session_scopes"] = sessions
    entry["quality_report"]["event_time_gap_quantiles"] = gaps
    entry["quality_report"]["duplicate_classification"] = _fixture_duplicate_classification(
        quote_rows, []
    )
    entry["quality_report"]["quality_flag_counts"] = {
        flag: sum(flag in (row["quality_flags"] or []) for row in quote_rows)
        for flag in {flag for row in quote_rows for flag in (row["quality_flags"] or [])}
    }
    entry_file.write_text(json.dumps(entry, sort_keys=True), encoding="utf-8")


def _move_quarantine_only_input_to_second_date(
    root: Path, entry_file: Path, entry: dict[str, Any], refs: dict[str, dict[str, Any]]
) -> None:
    quote_ref = refs["quotes"]
    quote_rows = pq.read_table(consumer.file_uri_path(quote_ref["uri"])).to_pylist()
    quarantine_ref = refs["quarantine"]
    quarantine_path = consumer.file_uri_path(quarantine_ref["uri"])
    quarantine_table = pq.read_table(quarantine_path)
    quarantine_rows = quarantine_table.to_pylist()
    quarantine_row = quarantine_rows[0]
    trade_day = date(2026, 1, 6)
    aware_local_time = datetime(2026, 1, 6, 9, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    local_time = aware_local_time.replace(tzinfo=None)
    quarantine_row.update({
        "TRADINGDATE": trade_day,
        "TRADINGTIME": local_time,
        "UNIX": int(aware_local_time.timestamp() * 1000),
        "SYMBOL": "T2603",
        "MARKET": "CFFEX",
        "BUYPRICE01": Decimal("0.000"),
        "reason_codes": ["INVALID_L1_BOOK"],
    })
    pq.write_table(pa.Table.from_pylist(quarantine_rows, schema=quarantine_table.schema), quarantine_path)
    _refresh_artifact(root, quarantine_ref, entry)

    raw_file = entry["inputs"]["raw_files"][0]
    raw_file.update({
        "row_count": 2, "row_group_offsets": [0], "row_group_row_counts": [2],
        "trade_date_min": "2026-01-05", "trade_date_max": "2026-01-06",
        "trade_date_count": 2,
    })
    reason_counts = {"INVALID_L1_BOOK": 1}
    for rule in entry["quality_report"]["rules"]:
        hit_count = reason_counts.get(rule["rule_id"], 0)
        rule.update({
            "hit_count": hit_count,
            "denominator": 1 if rule["rule_id"] == "DEPTH_BOOK_INVALID" else 2,
            "outcome": "WARN" if hit_count else "PASS",
        })
    scopes, sessions, gaps = _fixture_quality_scopes(quote_rows, quarantine_rows, "parquet")
    entry["quality_report"].update({
        "input_rows": 2,
        "accepted_rows": 1,
        "quarantined_rows": 1,
        "expected_missing_dates": [],
        "calendar_dates_without_source_rows": [],
        "source_trade_dates": ["2026-01-05", "2026-01-06"],
        "quarantine_reason_counts": reason_counts,
        "scopes": scopes,
        "session_scopes": sessions,
        "event_time_gap_quantiles": gaps,
        "duplicate_classification": _fixture_duplicate_classification(quote_rows, quarantine_rows),
    })
    entry_file.write_text(json.dumps(entry, sort_keys=True), encoding="utf-8")


def test_published_entry_with_empty_quarantine_is_consumable(tmp_path: Path) -> None:
    entry_file, _, _ = make_fixture(tmp_path)
    temporary_parent = REPO / "artifacts" / "verification"
    existing_temporary_dirs = set(temporary_parent.glob("duckdb-consumer-*"))

    report = consumer.verify_entry(entry_file)

    assert report["status"] == "PASS"
    assert report["quality_summary"]["input_rows"] == 2
    assert report["quality_summary"]["quarantined_rows"] == 0
    assert report["expected_missing_dates"] == ["2026-01-06"]
    assert report["availability"] == {
        "status": "unknown", "research_only": True,
        "non_null_available_time_rows": 0, "verified_point_in_time": False,
    }
    assert report["capabilities"]["spread_ticks_disabled_instruments"] == ["T2603"]
    assert report["quality_summary"]["scope_count"] == 1
    assert report["quality_summary"]["session_scope_count"] == 1
    assert report["quality_summary"]["event_time_gap_scope_count"] == 1
    assert set(temporary_parent.glob("duckdb-consumer-*")) == existing_temporary_dirs
    assert report["consumer_sample"][0]["mid"] == "100.125"
    assert report["consumer_sample"][0]["spread_price_units"] == "0.250"
    assert report["consumer_sample"][0]["l1_obi"] == "0.3333333333333333333333333333"
    assert report["consumer_sample"][0]["spread_ticks"] is None
    assert report["session_local_intervals"][0]["seconds"] == 1.0


def test_scope_session_and_gap_metrics_reconcile_to_serialized_rows(tmp_path: Path) -> None:
    entry_file, entry, _ = make_fixture(tmp_path)

    scope = entry["quality_report"]["scopes"][0]
    session = entry["quality_report"]["session_scopes"][0]
    gap = entry["quality_report"]["event_time_gap_quantiles"][0]
    assert scope["row_count"] == scope["accepted_rows"] == 2
    assert scope["duplicate_rate"] == {
        "numerator": 0, "denominator": 2, "value": 0.0,
        "formula": "sum(event_key_group_size - 1) / input row_count",
    }
    assert session["session_id"] == "CN_STOCK:2026-01-05:continuous_morning"
    assert session["row_count"] == 2
    assert gap["positive_interval_count"] == 1
    assert gap["p50_seconds"] == gap["p90_seconds"] == gap["p99_seconds"] == 1.0

    report = consumer.verify_entry(entry_file)

    assert report["quality_summary"]["scope_count"] == 1


def test_scoped_quality_working_rows_keep_only_projected_columns(tmp_path: Path) -> None:
    _, _, refs = make_fixture(tmp_path)
    connection = consumer.duckdb.connect(database=":memory:")
    connection.register("_session_intervals", pa.table({
        "instrument_id": pa.array(["T2603"], type=pa.string()),
        "trade_date": pa.array([TRADE_DATE], type=pa.date32()),
        "start_time": pa.array([clock_time(9, 30)], type=pa.time64("us")),
        "end_time": pa.array([clock_time(11, 30)], type=pa.time64("us")),
        "session_id": pa.array(["CN_STOCK:2026-01-05:continuous_morning"], type=pa.string()),
    }))
    source_schema = json.loads(SOURCE_SCHEMA.read_text(encoding="utf-8"))
    quote_path = consumer.file_uri_path(refs["quotes"]["uri"])
    quarantine_path = consumer.file_uri_path(refs["quarantine"]["uri"])
    fields = source_schema["sample"]["fields"]
    quote_sql, quote_params = consumer._projected_source_rows_sql(
        fields, _mapping(), "Asia/Shanghai", is_quote=True
    )
    quarantine_sql, quarantine_params = consumer._projected_source_rows_sql(
        fields, _mapping(), "Asia/Shanghai", is_quote=False
    )
    connection.execute(
        "CREATE TEMP TABLE _consumer_batch_rows AS SELECT r.*,s.session_id FROM (" + quote_sql + ") r "
        "LEFT JOIN _session_intervals s ON r.SYMBOL::VARCHAR=s.instrument_id "
        "AND r.TRADINGDATE::DATE=s.trade_date "
        "AND CAST(r.TRADINGTIME AS TIME)>=s.start_time AND CAST(r.TRADINGTIME AS TIME)<s.end_time",
        [*quote_params, [quote_path.as_posix()]],
    )
    connection.execute(
        "CREATE TEMP TABLE _consumer_quarantine_rows AS " + quarantine_sql,
        [*quarantine_params, [quarantine_path.as_posix()]],
    )
    input_columns = {
        row[0] for row in connection.execute("DESCRIBE _consumer_batch_rows").fetchall()
    }
    assert {"null_field_values", "invalid_quote", "has_invalid_l1_reason",
            "valid_price_pair", "crossed_book",
            "depth_invalid", "payload_hash"}.issubset(input_columns)
    assert not {"BUYPRICE05", "SELLPRICE05", "CLOSEPRICE", "TOTALAMOUNT", "product"} & input_columns
    assert connection.execute(
        "SELECT count(*),sum(null_field_values),count(DISTINCT payload_hash) "
        "FROM _consumer_batch_rows"
    ).fetchone() == (2, 0, 2)
    assert connection.execute(
        "SELECT count(*) FROM _consumer_quarantine_rows"
    ).fetchone()[0] == 0
    assert len(input_columns) == 19
    connection.close()


def test_scoped_duplicate_rate_uses_extra_rows_not_duplicate_members(tmp_path: Path) -> None:
    entry_file, entry, _ = make_fixture(tmp_path, duplicate_in_quarantine=True)
    duplicate_rate = entry["quality_report"]["scopes"][0]["duplicate_rate"]
    assert duplicate_rate["numerator"] == 1
    assert duplicate_rate["denominator"] == 3
    duplicate_rate["numerator"] = 2
    duplicate_rate["value"] = 2 / 3
    entry_file.write_text(json.dumps(entry, sort_keys=True), encoding="utf-8")

    with pytest.raises(consumer.VerificationError, match="duplicate_rate numerator/denominator/formula"):
        consumer.verify_entry(entry_file)


def test_scope_count_tampering_is_rejected(tmp_path: Path) -> None:
    entry_file, entry, _ = make_fixture(tmp_path)
    entry["quality_report"]["scopes"][0]["row_count"] = 3
    entry_file.write_text(json.dumps(entry, sort_keys=True), encoding="utf-8")

    with pytest.raises(consumer.VerificationError, match="scope row_count differs"):
        consumer.verify_entry(entry_file)


def test_session_scope_count_tampering_is_rejected(tmp_path: Path) -> None:
    entry_file, entry, _ = make_fixture(tmp_path)
    entry["quality_report"]["session_scopes"][0]["quarantined_rows"] = 1
    entry_file.write_text(json.dumps(entry, sort_keys=True), encoding="utf-8")

    with pytest.raises(consumer.VerificationError, match="session scope quarantined_rows differs"):
        consumer.verify_entry(entry_file)


def test_event_gap_quantile_tampering_is_rejected(tmp_path: Path) -> None:
    entry_file, entry, _ = make_fixture(tmp_path)
    entry["quality_report"]["event_time_gap_quantiles"][0]["p50_seconds"] = 2.0
    entry_file.write_text(json.dumps(entry, sort_keys=True), encoding="utf-8")

    with pytest.raises(consumer.VerificationError, match="quality report gap p50_seconds differs"):
        consumer.verify_entry(entry_file)


def test_fixture_generator_rebuilds_pinned_66_column_parquet(tmp_path: Path) -> None:
    generator = REPO / "tests/fixtures/onboarding_acceptance/build_fixture.py"
    output = tmp_path / "part-0.parquet"
    completed = subprocess.run(
        [sys.executable, str(generator), "--output", str(output)],
        check=True, capture_output=True, text=True,
    )
    result = json.loads(completed.stdout)
    parquet = pq.ParquetFile(output)
    source_schema = json.loads(SOURCE_SCHEMA.read_text(encoding="utf-8"))

    assert result["schema_matches"] is True
    assert result["column_count"] == len(source_schema["sample"]["fields"]) == 66
    assert result["input_rows"] == parquet.metadata.num_rows == 14
    assert result["accepted_rows"] == 9
    assert result["quarantined_rows"] == 5
    assert result["row_group_rows"] == [4, 4, 4, 2]


def test_missing_required_source_keys_are_allowed_only_with_quarantine_reason(tmp_path: Path) -> None:
    entry_file, _, _ = make_fixture(tmp_path, missing_required_quarantine=True)

    report = consumer.verify_entry(entry_file)

    assert report["status"] == "PASS"
    assert report["quality_summary"]["accepted_rows"] == 2
    assert report["quality_summary"]["quarantined_rows"] == 1
    assert report["quality_summary"]["quarantine_reason_counts"] == {
        "MISSING_REQUIRED_FIELD": 1,
    }


def test_quality_report_quarantine_reasons_must_match_serialized_rows(tmp_path: Path) -> None:
    entry_file, entry, _ = make_fixture(tmp_path, missing_required_quarantine=True)
    entry["quality_report"]["quarantine_reason_counts"]["MISSING_REQUIRED_FIELD"] = 0
    entry_file.write_text(json.dumps(entry, sort_keys=True), encoding="utf-8")

    with pytest.raises(consumer.VerificationError, match="quarantine_reason_counts differs"):
        consumer.verify_entry(entry_file)


def test_quality_report_rule_hits_must_match_quarantined_rows(tmp_path: Path) -> None:
    entry_file, entry, _ = make_fixture(tmp_path, missing_required_quarantine=True)
    rule = next(rule for rule in entry["quality_report"]["rules"]
                if rule["rule_id"] == "MISSING_REQUIRED_FIELD")
    rule["hit_count"] = 0
    entry_file.write_text(json.dumps(entry, sort_keys=True), encoding="utf-8")

    with pytest.raises(consumer.VerificationError, match="rule MISSING_REQUIRED_FIELD counts differ"):
        consumer.verify_entry(entry_file)


def test_depth_invalid_flag_is_reconciled_with_levels_two_through_five(tmp_path: Path) -> None:
    entry_file, _, _ = make_fixture(tmp_path, row_count=3, invalid_depth_at=2)

    report = consumer.verify_entry(entry_file)

    assert report["quality_summary"]["accepted_rows"] == 3
    assert report["quality_summary"]["quality_flag_counts"]["DEPTH_BOOK_INVALID"] == 1
    assert "DEPTH_BOOK_INVALID" not in report["quality_summary"]["quarantine_reason_counts"]


def test_reported_quality_flag_counts_must_match_accepted_quotes(tmp_path: Path) -> None:
    entry_file, entry, _ = make_fixture(tmp_path, row_count=3, invalid_depth_at=2)
    entry["quality_report"]["quality_flag_counts"]["DEPTH_BOOK_INVALID"] = 0
    entry_file.write_text(json.dumps(entry, sort_keys=True), encoding="utf-8")

    with pytest.raises(consumer.VerificationError, match="quality_flag_counts differs"):
        consumer.verify_entry(entry_file)


def test_depth_invalid_without_quality_flag_is_rejected(tmp_path: Path) -> None:
    entry_file, entry, refs = make_fixture(tmp_path, row_count=3, invalid_depth_at=2)
    ref = refs["quotes"]
    payload = Path(ref["uri"].removeprefix("file://"))
    table = pq.read_table(payload)
    flags = table.column("quality_flags").to_pylist()
    flags[2] = []
    table = table.set_column(
        table.schema.get_field_index("quality_flags"), "quality_flags",
        pa.array(flags, type=pa.list_(pa.string())),
    )
    pq.write_table(table, payload)
    _refresh_artifact(tmp_path, ref, entry)

    with pytest.raises(consumer.VerificationError, match="DEPTH_BOOK_INVALID flags disagree"):
        consumer.verify_entry(entry_file)


def test_null_quality_flags_are_rejected(tmp_path: Path) -> None:
    entry_file, entry, refs = make_fixture(tmp_path)
    ref = refs["quotes"]
    payload = Path(ref["uri"].removeprefix("file://"))
    table = pq.read_table(payload)
    table = table.set_column(
        table.schema.get_field_index("quality_flags"), "quality_flags",
        pa.array([None, []], type=pa.list_(pa.string())),
    )
    pq.write_table(table, payload)
    _refresh_artifact(tmp_path, ref, entry)

    with pytest.raises(consumer.VerificationError, match="contains null quality_flags"):
        consumer.verify_entry(entry_file)


def test_csv_source_allows_null_row_group_provenance(tmp_path: Path) -> None:
    entry_file, _, _ = make_fixture(tmp_path, source_format="csv")

    report = consumer.verify_entry(entry_file)

    assert report["status"] == "PASS"
    assert report["quality_summary"]["accepted_rows"] == 2


def test_file_uris_decode_encoded_equals_and_spaces(tmp_path: Path) -> None:
    entry_file, _, _ = make_fixture(tmp_path / "year=2026" / "month=01 data")

    assert consumer.verify_entry(entry_file)["status"] == "PASS"


def test_parquet_row_group_index_must_match_frozen_footer_boundaries(tmp_path: Path) -> None:
    entry_file, entry, refs = make_fixture(tmp_path)
    ref = refs["quotes"]
    payload = Path(ref["uri"].removeprefix("file://"))
    table = pq.read_table(payload)
    table = table.set_column(
        table.schema.get_field_index("raw_row_group_index"), "raw_row_group_index",
        pa.array([0, 1], type=pa.int32()),
    )
    pq.write_table(table, payload)
    _refresh_artifact(tmp_path, ref, entry)

    with pytest.raises(consumer.VerificationError, match="record_id/source provenance mismatch"):
        consumer.verify_entry(entry_file)


def test_bad_ref_hash_fails_before_parquet_scan(tmp_path: Path) -> None:
    entry_file, _, refs = make_fixture(tmp_path)
    payload = Path(refs["quotes"]["uri"].removeprefix("file://"))
    payload.write_bytes(payload.read_bytes() + b"tamper")

    with pytest.raises(consumer.VerificationError, match="content hash mismatch"):
        consumer.verify_entry(entry_file)


def test_modified_manifest_row_count_fails_footer_reconciliation(tmp_path: Path) -> None:
    entry_file, entry, refs = make_fixture(tmp_path)
    ref = refs["quotes"]
    ref["row_count"] = 3
    manifest_path = Path(ref["manifest_uri"].removeprefix("file://"))
    updated_manifest = _manifest(ref)
    manifest_path.write_text(json.dumps(updated_manifest, sort_keys=True), encoding="utf-8")
    entry["artifacts"]["quotes"][0] = ref
    entry["partitions"][0]["artifact_ref"] = ref
    entry["partitions"][0]["row_count"] = 3
    entry["artifact_manifests"]["quotes_T2603_2026-01-05"] = updated_manifest
    entry_file.write_text(json.dumps(entry, sort_keys=True), encoding="utf-8")

    with pytest.raises(consumer.VerificationError, match="row_count mismatch"):
        consumer.verify_entry(entry_file)


def test_modified_quote_schema_fails_exact_schema_validation(tmp_path: Path) -> None:
    entry_file, entry, refs = make_fixture(tmp_path)
    ref = refs["quotes"]
    payload = Path(ref["uri"].removeprefix("file://"))
    table = pq.read_table(payload).append_column("unexpected", pa.array([1, 2], type=pa.int64()))
    pq.write_table(table, payload)
    _refresh_artifact(tmp_path, ref, entry)

    with pytest.raises(consumer.VerificationError, match="unexpected fields"):
        consumer.verify_entry(entry_file)


def test_broken_entry_schema_fails_explicitly(tmp_path: Path) -> None:
    entry_file, entry, _ = make_fixture(tmp_path)
    del entry["partitions"]
    entry_file.write_text(json.dumps(entry), encoding="utf-8")

    with pytest.raises(consumer.VerificationError, match="missing required fields"):
        consumer.verify_entry(entry_file)


def test_non_null_availability_is_rejected_even_if_manifests_are_updated(tmp_path: Path) -> None:
    entry_file, entry, refs = make_fixture(tmp_path)
    ref = refs["quotes"]
    payload = Path(ref["uri"].removeprefix("file://"))
    table = pq.read_table(payload)
    table = table.set_column(
        table.schema.get_field_index("available_time"), "available_time",
        pa.array([TIMES[0], None], type=pa.timestamp("us", tz="UTC")),
    )
    pq.write_table(table, payload)
    _refresh_artifact(tmp_path, ref, entry)

    with pytest.raises(consumer.VerificationError, match="available_time is non-null"):
        consumer.verify_entry(entry_file)


def test_session_rules_stale_for_partition_are_rejected(tmp_path: Path) -> None:
    entry_file, entry, refs = make_fixture(tmp_path)
    ref = refs["session_rules"]
    payload = Path(ref["uri"].removeprefix("file://"))
    rules = json.loads(payload.read_text(encoding="utf-8"))
    rules["rules"][0]["effective_from"] = "2027-01-01"
    payload.write_text(json.dumps(rules, sort_keys=True), encoding="utf-8")
    _refresh_artifact(tmp_path, ref, entry)

    with pytest.raises(consumer.VerificationError, match="cover T2603 2026-01-05 0 times"):
        consumer.verify_entry(entry_file)


def test_invalid_l1_after_bounded_sample_is_still_rejected(tmp_path: Path) -> None:
    entry_file, _, _ = make_fixture(tmp_path, row_count=10, invalid_l1_at=9)

    with pytest.raises(consumer.VerificationError, match="invalid one-level books"):
        consumer.verify_entry(entry_file)


def test_negative_l1_size_after_bounded_sample_is_rejected(tmp_path: Path) -> None:
    entry_file, _, _ = make_fixture(tmp_path, row_count=10, invalid_size_at=9)

    with pytest.raises(consumer.VerificationError, match="invalid one-level books"):
        consumer.verify_entry(entry_file)


def test_quote_time_must_match_the_frozen_session_segment(tmp_path: Path) -> None:
    entry_file, _, _ = make_fixture(tmp_path, row_count=3, wrong_session_at=2)

    with pytest.raises(consumer.VerificationError, match="does not match session_rules/session_id"):
        consumer.verify_entry(entry_file)


def test_quote_file_must_be_sorted_by_event_and_source_identity(tmp_path: Path) -> None:
    entry_file, _, _ = make_fixture(tmp_path, row_count=3, unsorted_at=2)

    with pytest.raises(consumer.VerificationError, match="not sorted by event_time"):
        consumer.verify_entry(entry_file)


def test_availability_status_cannot_be_promoted_without_evidence(tmp_path: Path) -> None:
    entry_file, entry, _ = make_fixture(tmp_path)
    entry["passport"]["research_only"] = False
    entry_file.write_text(json.dumps(entry), encoding="utf-8")

    with pytest.raises(consumer.VerificationError, match="must remain research_only"):
        consumer.verify_entry(entry_file)


def test_duplicate_event_key_cannot_survive_in_quotes_and_quarantine(tmp_path: Path) -> None:
    entry_file, _, _ = make_fixture(tmp_path, duplicate_in_quarantine=True)

    with pytest.raises(consumer.VerificationError, match="repeated event-key groups"):
        consumer.verify_entry(entry_file)


def test_duplicate_payload_hash_includes_product(tmp_path: Path) -> None:
    entry_file, entry, _ = make_fixture(
        tmp_path, duplicate_in_quarantine=True, product_only_conflict=True
    )
    classification = entry["quality_report"]["duplicate_classification"]
    assert classification["identical_payload_groups"] == 0
    assert classification["conflicting_payload_groups"] == 1

    with pytest.raises(consumer.VerificationError, match="repeated event-key groups"):
        consumer.verify_entry(entry_file)


def test_duplicate_reason_must_match_payload_variant_category(tmp_path: Path) -> None:
    entry_file, entry, refs = make_fixture(tmp_path, duplicate_in_quarantine=True)
    _change_quarantine_reason_count(
        entry, "DUPLICATE_EVENT_KEY_IDENTICAL", "DUPLICATE_EVENT_KEY_CONFLICTING"
    )
    _rewrite_quarantine_rows(
        tmp_path, refs["quarantine"], entry,
        lambda rows: rows[0].update({"reason_codes": ["DUPLICATE_EVENT_KEY_CONFLICTING"]}),
    )

    with pytest.raises(consumer.VerificationError,
                       match="duplicate-event reason category differs from source payload variants"):
        consumer.verify_entry(entry_file)


def test_invalid_l1_reason_cannot_be_forged_for_valid_quarantine_row(tmp_path: Path) -> None:
    entry_file, entry, refs = make_fixture(tmp_path, missing_required_quarantine=True)
    entry["quality_report"]["quarantine_reason_counts"]["INVALID_L1_BOOK"] = 1
    invalid_rule = next(rule for rule in entry["quality_report"]["rules"]
                        if rule["rule_id"] == "INVALID_L1_BOOK")
    invalid_rule.update({"hit_count": 1, "outcome": "WARN"})

    def add_forged_reason(rows: list[dict[str, Any]]) -> None:
        rows[0]["reason_codes"].append("INVALID_L1_BOOK")

    _rewrite_quarantine_rows(tmp_path, refs["quarantine"], entry, add_forged_reason)

    with pytest.raises(consumer.VerificationError,
                       match="source L1 validity differs from quarantine INVALID_L1_BOOK reason"):
        consumer.verify_entry(entry_file)


def test_invalid_l1_quarantine_row_must_have_invalid_l1_reason(tmp_path: Path) -> None:
    entry_file, entry, refs = make_fixture(tmp_path, missing_required_quarantine=True)

    def remove_l1_validity(rows: list[dict[str, Any]]) -> None:
        rows[0]["BUYPRICE01"] = Decimal("0.000")

    _rewrite_quarantine_rows(tmp_path, refs["quarantine"], entry, remove_l1_validity)

    with pytest.raises(consumer.VerificationError,
                       match="source L1 validity differs from quarantine INVALID_L1_BOOK reason"):
        consumer.verify_entry(entry_file)


def test_session_intervals_and_horizons_do_not_cross_boundaries_or_gaps() -> None:
    rows = [
        {"instrument_id": "T2603", "session_id": "morning", "event_time": "2026-01-05T01:00:00+00:00"},
        {"instrument_id": "T2603", "session_id": "morning", "event_time": "2026-01-05T01:00:01+00:00"},
        {"instrument_id": "T2603", "session_id": "morning", "event_time": "2026-01-05T01:00:10+00:00"},
        {"instrument_id": "T2603", "session_id": "afternoon", "event_time": "2026-01-05T05:00:00+00:00"},
        {"instrument_id": "TF2603", "session_id": "morning", "event_time": "2026-01-05T01:00:02+00:00"},
        {"instrument_id": "T2603", "session_id": "afternoon", "event_time": "2026-01-05T05:00:02+00:00"},
    ]

    intervals = consumer.session_local_intervals(rows)
    pairs = consumer.horizon_pairs(rows, horizon_seconds=2)

    assert sorted(interval["seconds"] for interval in intervals) == [1.0, 2.0, 9.0]
    assert len(pairs) == 2
    assert all(left["instrument_id"] == right["instrument_id"] for left, right in pairs)
    assert all(left["session_id"] == right["session_id"] for left, right in pairs)


def test_l1_hand_calculation_and_invalid_tick_size() -> None:
    values = consumer.calculate_l1("100.000", "100.250", "10", "5")
    assert values == {
        "mid": "100.125", "spread_price_units": "0.250",
        "l1_obi": "0.3333333333333333333333333333", "spread_ticks": None,
    }
    with pytest.raises(consumer.VerificationError, match="tick_size must be positive"):
        consumer.calculate_l1("100", "101", "1", "1", "0")
    zero_size = consumer.calculate_l1("100", "101", "0", "0")
    assert zero_size["spread_price_units"] == "1"
    assert zero_size["l1_obi"] is None


def test_cli_writes_machine_readable_report(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    entry_file, _, _ = make_fixture(tmp_path)
    report_path = tmp_path / "verification.json"
    checkpoint_path = tmp_path / "verification-checkpoint.jsonl"

    exit_code = consumer.main([
        "--entry", str(entry_file), "--report", str(report_path),
        "--checkpoint", str(checkpoint_path),
    ])

    assert exit_code == 0
    printed = json.loads(capsys.readouterr().out)
    saved = json.loads(report_path.read_text(encoding="utf-8"))
    assert printed["status"] == saved["status"] == "PASS"
    assert saved["quality_summary"]["accepted_rows"] == 2
    assert saved["verification_limits"]["checkpoint_status"] == "CREATED"
    assert checkpoint_path.is_file()


def test_verified_checkpoint_reuses_only_completed_artifact_scans(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry_file, entry, _ = make_fixture(tmp_path)
    outer_manifest = _write_outer_entry_manifest(entry_file, entry)
    checkpoint = tmp_path / "consumer-checkpoint.jsonl"

    first = consumer.verify_entry(
        entry_file, entry_manifest_path=outer_manifest, checkpoint_path=checkpoint
    )

    assert first["status"] == "PASS"
    assert first["verified_outer_entry_manifest"] is True
    assert first["verification_limits"]["checkpoint_status"] == "CREATED"
    assert first["verification_limits"]["checkpoint_scan_count"] == 2

    def no_rescan(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        pytest.fail("unchanged, hash-bound checkpoint unexpectedly rescanned an artifact")

    monkeypatch.setattr(consumer, "_scan_quote_file", no_rescan)
    monkeypatch.setattr(consumer, "_scan_quarantine_file", no_rescan)
    monkeypatch.setattr(consumer, "_assert_sorted_quote_file", no_rescan)
    resumed = consumer.verify_entry(
        entry_file, entry_manifest_path=outer_manifest, checkpoint_path=checkpoint
    )

    assert resumed["status"] == "PASS"
    assert resumed["verification_limits"]["checkpoint_status"] == "RESUMED"
    assert resumed["verification_limits"]["checkpoint_scan_count"] == 2


def _rewrite_checkpoint_as_legacy_full_verifier_revision(checkpoint: Path) -> None:
    lines = checkpoint.read_text(encoding="utf-8").splitlines()
    header = json.loads(lines[0])
    binding = dict(header["binding"])
    binding.pop("scanner_revision")
    binding["verifier_revision"] = (
        "sha256:426b5eb219f9bc4e3c548ee19ba7cdd9bae8b7b1877c968226736645c8de550f"
    )
    header["binding"] = binding
    header["binding_hash"] = consumer._canonical_hash(binding)
    lines[0] = json.dumps(header, sort_keys=True, separators=(",", ":"))
    checkpoint.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_legacy_checkpoint_rebinds_only_for_exact_scanner_fingerprint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry_file, entry, _ = make_fixture(tmp_path)
    outer_manifest = _write_outer_entry_manifest(entry_file, entry)
    checkpoint = tmp_path / "legacy-checkpoint.jsonl"
    first = consumer.verify_entry(
        entry_file, entry_manifest_path=outer_manifest, checkpoint_path=checkpoint
    )
    assert first["status"] == "PASS"
    _rewrite_checkpoint_as_legacy_full_verifier_revision(checkpoint)

    def no_rescan(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        pytest.fail("compatible, hash-bound legacy scan record unexpectedly rescanned")

    monkeypatch.setattr(consumer, "_scan_quote_file", no_rescan)
    monkeypatch.setattr(consumer, "_scan_quarantine_file", no_rescan)
    monkeypatch.setattr(consumer, "_assert_sorted_quote_file", no_rescan)
    resumed = consumer.verify_entry(
        entry_file, entry_manifest_path=outer_manifest, checkpoint_path=checkpoint
    )

    assert resumed["status"] == "PASS"
    assert resumed["verification_limits"]["checkpoint_status"] == "RESUMED_COMPATIBLE_REVISION"
    assert resumed["verification_limits"]["checkpoint_scan_count"] == 2


def test_legacy_checkpoint_scanner_revision_mismatch_rescans(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry_file, entry, _ = make_fixture(tmp_path)
    outer_manifest = _write_outer_entry_manifest(entry_file, entry)
    checkpoint = tmp_path / "legacy-checkpoint-mismatch.jsonl"
    consumer.verify_entry(
        entry_file, entry_manifest_path=outer_manifest, checkpoint_path=checkpoint
    )
    _rewrite_checkpoint_as_legacy_full_verifier_revision(checkpoint)

    original_scan = consumer._scan_quote_file
    scan_calls = 0

    def count_scans(*args: Any, **kwargs: Any) -> dict[str, Any]:
        nonlocal scan_calls
        scan_calls += 1
        return original_scan(*args, **kwargs)

    monkeypatch.setattr(consumer, "_checkpoint_scanner_revision", lambda: "sha256:" + "0" * 64)
    monkeypatch.setattr(consumer, "_scan_quote_file", count_scans)
    resumed = consumer.verify_entry(
        entry_file, entry_manifest_path=outer_manifest, checkpoint_path=checkpoint
    )

    assert resumed["status"] == "PASS"
    assert resumed["verification_limits"]["checkpoint_status"] == "INVALIDATED_BINDING"
    assert scan_calls == 1


def test_scanner_fingerprint_covers_schema_contracts_and_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_revision = consumer._checkpoint_scanner_revision()
    original_schema = consumer.QUOTE_CORE_COLUMNS
    try:
        for helper_name, replacement in (
            ("_depth_invalid_expression", _alternate_depth_invalid_expression),
            ("_expiry_date", _alternate_expiry_date),
            ("_sql_ident", _alternate_sql_ident),
        ):
            with monkeypatch.context() as patcher:
                patcher.setattr(consumer, helper_name, replacement)
                consumer._checkpoint_scanner_revision.cache_clear()
                assert consumer._checkpoint_scanner_revision() != original_revision

        with monkeypatch.context() as patcher:
            patcher.setattr(consumer.duckdb, "__version__", "0.0.0-test")
            consumer._checkpoint_scanner_revision.cache_clear()
            assert consumer._checkpoint_scanner_revision() != original_revision

        with monkeypatch.context() as patcher:
            patcher.setattr(consumer, "QUOTE_CORE_COLUMNS", {
                **original_schema, "exchange": pa.large_string(),
            })
            consumer._checkpoint_scanner_revision.cache_clear()
            assert consumer._checkpoint_scanner_revision() != original_revision
    finally:
        consumer._checkpoint_scanner_revision.cache_clear()


def test_checkpoint_outer_manifest_change_forces_artifact_rescan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry_file, entry, _ = make_fixture(tmp_path)
    outer_manifest = _write_outer_entry_manifest(entry_file, entry)
    checkpoint = tmp_path / "consumer-checkpoint.jsonl"
    consumer.verify_entry(entry_file, entry_manifest_path=outer_manifest, checkpoint_path=checkpoint)

    manifest = json.loads(outer_manifest.read_text(encoding="utf-8"))
    manifest["created_at"] = "2026-11-04T00:00:01+00:00"
    outer_manifest.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    scan_calls = 0
    original_scan = consumer._scan_quote_file

    def count_scans(*args: Any, **kwargs: Any) -> dict[str, Any]:
        nonlocal scan_calls
        scan_calls += 1
        return original_scan(*args, **kwargs)

    monkeypatch.setattr(consumer, "_scan_quote_file", count_scans)
    report = consumer.verify_entry(
        entry_file, entry_manifest_path=outer_manifest, checkpoint_path=checkpoint
    )

    assert report["status"] == "PASS"
    assert report["verification_limits"]["checkpoint_status"] == "INVALIDATED_BINDING"
    assert scan_calls == 1


def test_tampered_checkpoint_record_is_ignored_and_rescanned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry_file, entry, _ = make_fixture(tmp_path)
    outer_manifest = _write_outer_entry_manifest(entry_file, entry)
    checkpoint = tmp_path / "consumer-checkpoint.jsonl"
    consumer.verify_entry(entry_file, entry_manifest_path=outer_manifest, checkpoint_path=checkpoint)

    lines = checkpoint.read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(lines[1:], start=1):
        record = json.loads(line)
        if record.get("kind") == "quote":
            record["result"]["scan"]["bad_record_ids"] = 1
            lines[index] = json.dumps(record, sort_keys=True, separators=(",", ":"))
            break
    else:
        raise AssertionError("checkpoint did not contain a quote scan record")
    checkpoint.write_text("\n".join(lines) + "\n", encoding="utf-8")
    scan_calls = 0
    original_scan = consumer._scan_quote_file

    def count_scans(*args: Any, **kwargs: Any) -> dict[str, Any]:
        nonlocal scan_calls
        scan_calls += 1
        return original_scan(*args, **kwargs)

    monkeypatch.setattr(consumer, "_scan_quote_file", count_scans)
    report = consumer.verify_entry(
        entry_file, entry_manifest_path=outer_manifest, checkpoint_path=checkpoint
    )

    assert report["status"] == "PASS"
    assert scan_calls == 1


def test_checkpoint_payload_change_forces_artifact_rescan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry_file, entry, refs = make_fixture(tmp_path)
    outer_manifest = _write_outer_entry_manifest(entry_file, entry)
    checkpoint = tmp_path / "consumer-checkpoint.jsonl"
    consumer.verify_entry(entry_file, entry_manifest_path=outer_manifest, checkpoint_path=checkpoint)

    quote_ref = refs["quotes"]
    quote_path = Path(quote_ref["uri"].removeprefix("file://"))
    table = pq.read_table(quote_path)
    column_index = table.schema.get_field_index("last_price")
    values = table.column(column_index).to_pylist()
    values[0] = Decimal("100.010")
    table = table.set_column(
        column_index, "last_price", pa.array(values, type=table.schema.field(column_index).type)
    )
    pq.write_table(table, quote_path)
    _refresh_artifact(tmp_path, quote_ref, entry)
    _refresh_outer_entry_manifest(entry_file, outer_manifest)

    scan_calls = 0
    original_scan = consumer._scan_quote_file

    def count_scans(*args: Any, **kwargs: Any) -> dict[str, Any]:
        nonlocal scan_calls
        scan_calls += 1
        return original_scan(*args, **kwargs)

    monkeypatch.setattr(consumer, "_scan_quote_file", count_scans)
    report = consumer.verify_entry(
        entry_file, entry_manifest_path=outer_manifest, checkpoint_path=checkpoint
    )

    assert report["status"] == "PASS"
    assert report["verification_limits"]["checkpoint_status"] == "INVALIDATED_BINDING"
    assert scan_calls == 1


def test_source_row_identity_bitset_detects_duplicates_across_date_batches() -> None:
    connection = consumer.duckdb.connect(database=":memory:")
    raw_hash = "a" * 64
    bitmaps, expected = consumer._new_identity_bitmaps([{
        "sha256": "sha256:" + raw_hash, "row_count": 2,
    }])
    rows_seen: Counter[str] = Counter()
    try:
        connection.execute(
            "CREATE TEMP TABLE _consumer_batch_rows AS "
            "SELECT ?::VARCHAR AS raw_file_sha256,0::BIGINT AS raw_row_index", [raw_hash]
        )
        assert consumer._consume_batch_identity_rows(connection, bitmaps, expected, rows_seen) is False
        connection.execute(
            "CREATE OR REPLACE TEMP TABLE _consumer_batch_rows AS "
            "SELECT ?::VARCHAR AS raw_file_sha256,0::BIGINT AS raw_row_index", [raw_hash]
        )
        assert consumer._consume_batch_identity_rows(connection, bitmaps, expected, rows_seen) is True
    finally:
        connection.close()


def test_serialized_trade_date_batches_keep_identity_and_scope_counts(tmp_path: Path) -> None:
    entry_file, entry, refs = make_fixture(tmp_path, row_count=1)
    _add_second_date_quote(tmp_path, entry_file, entry, refs)

    report = consumer.verify_entry(entry_file)

    assert report["status"] == "PASS"
    assert report["quality_summary"]["input_rows"] == 2
    assert report["quality_summary"]["identity_audit_rows"] == 2
    assert report["quality_summary"]["scope_count"] == 2
    assert report["quality_summary"]["duplicate_event_key_groups"] == 0


def test_expected_missing_dates_use_source_rows_not_accepted_quote_partitions(
    tmp_path: Path,
) -> None:
    entry_file, entry, refs = make_fixture(
        tmp_path, row_count=1, missing_required_quarantine=True
    )
    _move_quarantine_only_input_to_second_date(tmp_path, entry_file, entry, refs)

    report = consumer.verify_entry(entry_file)

    assert report["status"] == "PASS"
    assert report["quality_summary"]["quote_dates"] == 1
    assert report["source"]["observed_source_trade_dates"] == ["2026-01-05", "2026-01-06"]
    assert report["expected_missing_dates"] == []


def test_serialized_duplicate_source_row_across_dates_is_rejected(tmp_path: Path) -> None:
    entry_file, entry, refs = make_fixture(tmp_path, row_count=1)
    _add_second_date_quote(tmp_path, entry_file, entry, refs, duplicate_row_index=True)

    with pytest.raises(consumer.VerificationError, match="duplicated across outputs"):
        consumer.verify_entry(entry_file)


def test_cli_accepts_and_reports_duckdb_resource_overrides(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    entry_file, _, _ = make_fixture(tmp_path)
    report_path = tmp_path / "verification-resources.json"

    exit_code = consumer.main([
        "--entry", str(entry_file), "--report", str(report_path),
        "--duckdb-memory-limit", "2GB", "--duckdb-threads", "2",
    ])

    assert exit_code == 0
    capsys.readouterr()
    saved = json.loads(report_path.read_text(encoding="utf-8"))
    assert saved["verification_limits"]["duckdb_memory_limit"] == "2GB"
    assert saved["verification_limits"]["duckdb_threads"] == 2


def test_cli_rejects_invalid_duckdb_resource_overrides(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    entry_file, _, _ = make_fixture(tmp_path)

    exit_code = consumer.main([
        "--entry", str(entry_file), "--duckdb-memory-limit", "8GB;DROP",
        "--duckdb-threads", "0",
    ])

    assert exit_code == 1
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "FAIL"
    assert "memory limit" in report["errors"][0]["message"]
