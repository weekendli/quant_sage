"""Step 02 L2 snapshot onboarding, normalization, and row-level quality gates."""

from __future__ import annotations

import json
import re
import tempfile
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv

from quantsage_discovery.core.artifacts import ArtifactStore
from quantsage_discovery.core.errors import ArtifactValidationError
from quantsage_discovery.core.types import (
    ArtifactManifest,
    ArtifactRef,
    RunContext,
    StageResult,
)
from quantsage_discovery.core.utils import canonical_json
from quantsage_discovery.ingest.calendar import (
    CalendarSnapshot,
    bare_sha256,
    fetch_api_calendar,
    read_frozen_calendar,
    read_local_calendar,
    sha256_file,
)
from quantsage_discovery.ingest.readers import verify_source_inventory
from quantsage_discovery.ingest.schema import (
    L2_FIELD_MAPPING,
    SourceSchema,
    load_source_schema,
    validate_physical_schema,
)
from quantsage_discovery.ingest.sessions import freeze_session_rules, load_session_rules

ENTRY_VERSION = "1.0.0"
QUARANTINE_SUFFIXES = (
    ("record_id", pa.string()),
    ("raw_file_sha256", pa.string()),
    ("raw_row_index", pa.int64()),
    ("raw_row_group_index", pa.int32()),
    ("reason_codes", pa.list_(pa.string())),
)
QUOTE_SUFFIXES = (
    ("record_id", pa.string()),
    ("raw_file_sha256", pa.string()),
    ("raw_row_index", pa.int64()),
    ("raw_row_group_index", pa.int32()),
    ("session_id", pa.string()),
    ("available_time", pa.timestamp("us", tz="UTC")),
    ("quality_flags", pa.list_(pa.string())),
)
REQUIRED_QUOTE_COLUMNS = {
    "exchange", "instrument_id", "trade_date", "event_time", "source_event_unix_ms",
    "bid_price_1", "ask_price_1", "bid_size_1", "ask_size_1", "record_id",
    "raw_file_sha256", "raw_row_index", "raw_row_group_index", "session_id",
    "available_time", "quality_flags",
}


def _path_from_uri(value: str) -> Path:
    parsed = urlparse(value)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path)).resolve()
    return Path(value).expanduser().resolve()


def _sql_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _schema_types(source: SourceSchema) -> tuple[pa.Schema, pa.Schema]:
    quote_fields: list[pa.Field] = []
    for field in source.fields:
        name = source.standardized_name(field.name)
        dtype = pa.timestamp("us", tz="UTC") if field.name == "TRADINGTIME" else field.arrow_type
        quote_fields.append(pa.field(name, dtype, nullable=True))
    quote_fields.extend(pa.field(name, dtype, nullable=name in {"raw_row_group_index", "available_time"})
                        for name, dtype in QUOTE_SUFFIXES)
    quarantine_fields = [pa.field(field.name, field.arrow_type, nullable=True) for field in source.fields]
    quarantine_fields.extend(pa.field(name, dtype, nullable=name == "raw_row_group_index")
                             for name, dtype in QUARANTINE_SUFFIXES)
    quote_schema = pa.schema(quote_fields)
    quarantine_schema = pa.schema(quarantine_fields)
    if len(quote_schema.names) != len(set(quote_schema.names)):
        raise ValueError("normalized L2 schema contains duplicate output column names")
    return quote_schema, quarantine_schema


def _validate_parquet_schema(path: Path, expected: pa.Schema, what: str) -> None:
    try:
        actual = pq.ParquetFile(path).schema_arrow
    except Exception as exc:  # pragma: no cover - error wording is Arrow-version-specific
        raise ArtifactValidationError(f"{what} output is not readable Parquet: {exc}") from exc
    if actual.names != expected.names or actual.types != expected.types:
        raise ArtifactValidationError(
            f"{what} output schema differs from its declared contract: "
            f"expected={expected}, actual={actual}"
        )


def _merge_partition_files(
    connection: duckdb.DuckDBPyConnection,
    source_paths: list[Path],
    output_path: Path,
    *,
    expected_rows: int,
    expected_schema: pa.Schema,
) -> int:
    """Merge every DuckDB per-thread partition file into one ordered artifact file."""
    ordered_sources = sorted(source_paths, key=lambda path: path.as_posix())
    if not ordered_sources:
        raise ValueError("partition merge received no source Parquet files")
    physical_rows = sum(pq.ParquetFile(path).metadata.num_rows for path in ordered_sources)
    if physical_rows != expected_rows:
        raise ValueError(
            f"partition footer reconciliation failed: expected={expected_rows}, "
            f"physical_parts={physical_rows}, files={len(ordered_sources)}"
        )
    source_files_sql = "[" + ",".join(_sql_string(path.as_posix()) for path in ordered_sources) + "]"
    connection.execute(
        f"COPY (SELECT * FROM read_parquet({source_files_sql}, hive_partitioning=false) "
        "ORDER BY event_time, raw_file_sha256, raw_row_index) "
        f"TO {_sql_string(output_path.as_posix())} "
        "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
    )
    _validate_parquet_schema(output_path, expected_schema, "quotes")
    merged_rows = pq.ParquetFile(output_path).metadata.num_rows
    if merged_rows != expected_rows:
        raise ValueError(
            f"merged quote footer reconciliation failed: expected={expected_rows}, actual={merged_rows}"
        )
    return merged_rows


def validate_quotes_artifact(path: Path) -> None:
    try:
        parquet = pq.ParquetFile(path)
    except Exception as exc:
        raise ArtifactValidationError(f"quotes output is not readable Parquet: {exc}") from exc
    names = set(parquet.schema_arrow.names)
    missing = sorted(REQUIRED_QUOTE_COLUMNS - names)
    if missing:
        raise ArtifactValidationError(f"quotes artifact is missing required fields: {missing}")
    if parquet.schema_arrow.field("event_time").type != pa.timestamp("us", tz="UTC"):
        raise ArtifactValidationError("quotes.event_time must be timestamp[us, UTC]")
    if parquet.metadata.num_rows <= 0:
        raise ArtifactValidationError("empty quote partitions cannot be published")


def validate_quarantine_artifact(path: Path) -> None:
    try:
        parquet = pq.ParquetFile(path)
    except Exception as exc:
        raise ArtifactValidationError(f"quarantine output is not readable Parquet: {exc}") from exc
    required = {name for name, _ in QUARANTINE_SUFFIXES}
    missing = sorted(required - set(parquet.schema_arrow.names))
    if missing:
        raise ArtifactValidationError(f"quarantine artifact is missing fields: {missing}")
    if parquet.schema_arrow.field("reason_codes").type != pa.list_(pa.string()):
        raise ArtifactValidationError("quarantine.reason_codes must be list<string>")


def validate_calendar_artifact(path: Path) -> None:
    expected = pa.schema([pa.field("calendar_id", pa.string()), pa.field("trade_date", pa.date32())])
    _validate_parquet_schema(path, expected, "calendar")
    parquet = pq.ParquetFile(path)
    if parquet.metadata.num_rows <= 0:
        raise ArtifactValidationError("calendar snapshot cannot be empty")


def validate_session_rules_artifact(path: Path) -> None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ArtifactValidationError(f"invalid frozen session rules JSON: {exc}") from exc
    if not isinstance(value, dict) or not value.get("rules") or not value.get("source_refs"):
        raise ArtifactValidationError("session rules require rules and evidence source references")
    if not value.get("evidence_sha256"):
        raise ArtifactValidationError("session rules require the evidence content hash")


def validate_onboarding_entry_artifact(path: Path) -> None:
    try:
        entry = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ArtifactValidationError(f"invalid onboarding entry JSON: {exc}") from exc
    if not isinstance(entry, dict) or entry.get("stage_name") != "onboard":
        raise ArtifactValidationError("onboarding entry must be a Step 02 object")
    inputs = entry.get("inputs", {})
    for item in inputs.get("raw_files", []):
        try:
            source = _path_from_uri(item["uri"])
            if not source.is_file() or sha256_file(source) != item["sha256"]:
                raise ValueError(f"frozen source changed or disappeared: {source}")
        except (KeyError, OSError, ValueError) as exc:
            raise ArtifactValidationError(f"onboarding source snapshot validation failed: {exc}") from exc
    for ref_key in ("session_evidence_ref",):
        ref = inputs.get(ref_key)
        if isinstance(ref, dict):
            evidence = _path_from_uri(str(ref.get("uri", "")))
            if not evidence.is_file() or sha256_file(evidence) != ref.get("sha256"):
                raise ArtifactValidationError(f"onboarding {ref_key} hash changed")


def _source_inventory(ctx: RunContext, inputs: dict[str, ArtifactRef]) -> tuple[dict[str, Any], dict[str, Any]]:
    by_type = {ref.artifact_type: ref for ref in inputs.values()}
    source_ref = by_type.get("source_snapshot")
    split_ref = by_type.get("split_manifest")
    if source_ref is None or split_ref is None:
        raise ValueError("onboard requires bootstrap source_snapshot and split_manifest artifacts")
    source_path = _path_from_uri(source_ref.uri)
    split_path = _path_from_uri(split_ref.uri)
    inventory = json.loads(source_path.read_text(encoding="utf-8"))
    split = json.loads(split_path.read_text(encoding="utf-8"))
    verify_source_inventory(inventory, verify_hashes=True)
    if inventory.get("data_snapshot_id") != ctx.data_snapshot_id:
        raise ValueError("onboard source inventory does not match context data_snapshot_id")
    if split.get("split_id") != ctx.split_id or split.get("source_data_snapshot_id") != ctx.data_snapshot_id:
        raise ValueError("onboard SplitManifest does not match context snapshot/split IDs")
    if split.get("method") != "unassigned_research_only":
        raise ValueError("Step 02 accepts only the frozen unassigned research-only SplitManifest")
    return inventory, split


def _source_schema(config: dict[str, Any]) -> SourceSchema:
    input_config = config.get("input", {})
    path = input_config.get("source_schema_path")
    if not path:
        raise ValueError("input.source_schema_path is required")
    schema = load_source_schema(path)
    required = {
        "TRADINGDATE", "TRADINGTIME", "MARKET", "SYMBOL", "UNIX", "BUYPRICE01", "SELLPRICE01",
        "BUYVOLUME01", "SELLVOLUME01", "source_file", "source_row", "ingest_run_id", "product",
    }
    missing = sorted(required - set(schema.names))
    if missing:
        raise ValueError(f"source schema is missing onboarding-required fields: {missing}")
    schema.standardized_names()
    return schema


def _calendar_snapshot(config: dict[str, Any], inventory: dict[str, Any]) -> CalendarSnapshot:
    calendar_config = config.get("calendar", {})
    evidence_path = Path(calendar_config.get("evidence_path", "")).expanduser().resolve()
    if not evidence_path.is_file():
        raise ValueError("calendar.evidence_path must point to the pinned CFFEX evidence JSON")
    if calendar_config.get("evidence_sha256") != sha256_file(evidence_path):
        raise ValueError("calendar evidence hash differs from preflight configuration")
    date_range = inventory.get("source_date_range", {})
    if not date_range.get("start") or not date_range.get("end"):
        raise ValueError("source inventory has no non-null trade-date range")
    start = date.fromisoformat(date_range["start"])
    end = date.fromisoformat(date_range["end"])
    calendar_id = str(calendar_config.get("calendar_id", "CN_STOCK"))
    provider = calendar_config.get("provider", "local_duckdb")
    if provider == "local_duckdb":
        snapshot = read_local_calendar(
            data_root=calendar_config.get("data_root", "/data/marketdata"),
            evidence_path=evidence_path,
            calendar_id=calendar_id,
            start=start,
            end=end,
        )
    elif provider == "api":
        snapshot = fetch_api_calendar(
            base_url=str(calendar_config["base_url"]),
            ca_bundle=str(calendar_config["ca_bundle"]),
            evidence_path=evidence_path,
            calendar_id=calendar_id,
            start=start,
            end=end,
        )
    elif provider == "frozen_snapshot":
        snapshot_path = Path(calendar_config["frozen_snapshot_path"]).expanduser().resolve()
        snapshot = read_frozen_calendar(
            snapshot_path=snapshot_path,
            expected_sha256=str(calendar_config["frozen_snapshot_sha256"]),
            evidence_path=evidence_path,
            calendar_id=calendar_id,
            start=start,
            end=end,
        )
    else:
        raise ValueError(f"unsupported calendar.provider {provider!r}")
    return snapshot


def _build_session_intervals(evidence_path: Path) -> tuple[dict[str, Any], pa.Table]:
    frozen = freeze_session_rules(evidence_path)
    rules = load_session_rules(evidence_path)
    from datetime import time as clock_time

    rows = []
    for rule in rules.rules:
        first = date.fromisoformat(rule.get("effective_from", rule.get("effective_for_scope_start", "0001-01-01")))
        last = date.fromisoformat(rule.get("effective_for_scope_end", "9999-12-31"))
        auction = rule.get("opening_auction")
        segments = []
        if auction:
            segments.extend([
                ("opening_auction_submission", auction["start"], auction["order_submission_end"], False, False),
                ("opening_auction_matching", auction["matching_start"], auction["end"], False, False),
            ])
        morning = rule.get("morning_continuous")
        if morning:
            segments.append(("continuous_morning", morning["start"], morning["end"], True, False))
        afternoon = rule.get("afternoon_continuous_regular_day")
        if afternoon:
            segments.append(("continuous_afternoon", afternoon["start"], afternoon["end"], True,
                             rule.get("last_trading_day_afternoon") == "closed"))
        for product in rule["products"]:
            for segment, start_raw, end_raw, continuous, closed_expiry in segments:
                rows.append((product, first, last, segment,
                             clock_time.fromisoformat(start_raw), clock_time.fromisoformat(end_raw),
                             continuous, closed_expiry, str(rule["rule_id"])))
    table = pa.table({
        "product": pa.array([row[0] for row in rows], type=pa.string()),
        "first_date": pa.array([row[1] for row in rows], type=pa.date32()),
        "last_date": pa.array([row[2] for row in rows], type=pa.date32()),
        "segment_id": pa.array([row[3] for row in rows], type=pa.string()),
        "start_time": pa.array([row[4] for row in rows], type=pa.time64("us")),
        "end_time": pa.array([row[5] for row in rows], type=pa.time64("us")),
        "continuous_trading": pa.array([row[6] for row in rows], type=pa.bool_()),
        "closed_expiry": pa.array([row[7] for row in rows], type=pa.bool_()),
        "rule_id": pa.array([row[8] for row in rows], type=pa.string()),
    })
    return frozen, table


def _expiry_rows(instrument_ids: list[str], calendar_dates: set[date]) -> pa.Table:
    from quantsage_discovery.ingest.sessions import contract_last_trading_date

    instruments: list[str] = []
    expiry: list[date | None] = []
    for instrument in instrument_ids:
        if not re.fullmatch(r"(?:T|TF|TL|TS)\d{4}", instrument):
            continue
        instruments.append(instrument)
        expiry.append(contract_last_trading_date(instrument, calendar_dates))
    return pa.table({"instrument_id": pa.array(instruments, pa.string()),
                     "expiry_date": pa.array(expiry, pa.date32())})


def _prepare_csv_parquet(path: Path, schema: SourceSchema, output: Path) -> None:
    reader = csv.open_csv(
        path,
        read_options=csv.ReadOptions(autogenerate_column_names=False, block_size=1 << 20),
        convert_options=csv.ConvertOptions(
            column_types={field.name: field.arrow_type for field in schema.fields},
            strings_can_be_null=True,
        ),
    )
    validate_physical_schema(reader.schema, schema)
    writer: pq.ParquetWriter | None = None
    try:
        for batch in reader:
            if writer is None:
                writer = pq.ParquetWriter(output, batch.schema, compression="zstd")
            writer.write_batch(batch, row_group_size=250_000)
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        raise ValueError(f"CSV source file is empty: {path}")


def _row_group_case(raw_file: dict[str, Any]) -> str:
    if raw_file["source_format"] == "csv":
        return "CAST(NULL AS INTEGER)"
    clauses = []
    for index, (start, count) in enumerate(zip(
        raw_file["row_group_offsets"], raw_file["row_group_row_counts"], strict=True
    )):
        clauses.append(f"WHEN file_row_number >= {int(start)} AND file_row_number < {int(start + count)} THEN {index}")
    return "CASE " + " ".join(clauses) + " ELSE NULL END"


def _payload_hash_expr(source: SourceSchema) -> str:
    ignored = {"source_file", "source_row", "ingest_run_id"}
    entries = [
        f"{_sql_ident(field.name)} := {_sql_ident(field.name)}"
        for field in source.fields if field.name not in ignored
    ]
    return "sha256(to_json(struct_pack(" + ", ".join(entries) + ")))"


def _register_global_duplicate_indexes(
    connection: duckdb.DuckDBPyConnection,
    raw_files: list[dict[str, Any]],
    source: SourceSchema,
    *,
    event_keys_overlap: bool,
    provenance_pairs_overlap: bool,
) -> None:
    parquet_paths = [_path_from_uri(item["uri"]).as_posix() for item in raw_files]
    if any(item["source_format"] != "parquet" for item in raw_files):
        if event_keys_overlap or provenance_pairs_overlap:
            raise ValueError("multi-file CSV duplicate prepass is not supported; freeze one CSV per run")
        return
    paths_sql = "[" + ",".join(_sql_string(path) for path in parquet_paths) + "]"
    if event_keys_overlap:
        connection.execute(
            f"CREATE TEMP TABLE _global_event_candidates AS SELECT MARKET, SYMBOL, TRADINGDATE, "
            f"TRADINGTIME, count(*) AS event_count FROM read_parquet({paths_sql}) "
            "WHERE MARKET IS NOT NULL AND SYMBOL IS NOT NULL AND TRADINGDATE IS NOT NULL "
            "AND TRADINGTIME IS NOT NULL GROUP BY ALL HAVING count(*) > 1"
        )
        payload = _payload_hash_expr(source)
        connection.execute(
            f"CREATE TEMP TABLE _global_event_stats AS SELECT r.MARKET, r.SYMBOL, r.TRADINGDATE, "
            f"r.TRADINGTIME, c.event_count, count(DISTINCT {payload}) AS payload_variants "
            f"FROM read_parquet({paths_sql}) r JOIN _global_event_candidates c USING "
            "(MARKET, SYMBOL, TRADINGDATE, TRADINGTIME) GROUP BY ALL"
        )
        union_parts = []
        for item in raw_files:
            source_path = _path_from_uri(item["uri"])
            union_parts.append(
                "SELECT MARKET, SYMBOL, TRADINGDATE, TRADINGTIME, "
                f"file_row_number::BIGINT AS raw_row_index, "
                f"{_sql_string(bare_sha256(str(item['sha256'])))}::VARCHAR AS raw_file_sha256 "
                f"FROM read_parquet({_sql_string(source_path.as_posix())}, file_row_number=true)"
            )
        union_sql = " UNION ALL ".join(union_parts)
        connection.execute(
            "CREATE TEMP TABLE _global_event_extra AS WITH all_rows AS (" + union_sql + "), "
            "duplicate_members AS (SELECT r.* FROM all_rows r JOIN _global_event_candidates c "
            "USING(MARKET,SYMBOL,TRADINGDATE,TRADINGTIME)), ranked AS ("
            "SELECT *, row_number() OVER (PARTITION BY MARKET,SYMBOL,TRADINGDATE,TRADINGTIME "
            "ORDER BY raw_file_sha256,raw_row_index) AS event_rank FROM duplicate_members) "
            "SELECT MARKET,SYMBOL,TRADINGDATE,TRADINGTIME,raw_file_sha256,raw_row_index "
            "FROM ranked WHERE event_rank>1"
        )
    if provenance_pairs_overlap:
        connection.execute(
            f"CREATE TEMP TABLE _global_source_stats AS "
            f"SELECT ingest_run_id, source_file, source_row, count(*) AS source_count "
            f"FROM read_parquet({paths_sql}) "
            f"WHERE ingest_run_id IS NOT NULL AND source_file IS NOT NULL AND source_row IS NOT NULL "
            f"GROUP BY ALL"
        )


def _create_raw_table(
    connection: duckdb.DuckDBPyConnection,
    source_path: Path,
    raw_file: dict[str, Any],
) -> None:
    group_case = _row_group_case(raw_file)
    if raw_file["source_format"] == "parquet":
        relation = "read_parquet(?, file_row_number=true, hive_partitioning=false)"
    else:
        relation = "read_parquet(?, file_row_number=true, hive_partitioning=false)"
    connection.execute("DROP TABLE IF EXISTS _raw")
    connection.execute(
        f"CREATE TEMP TABLE _raw AS SELECT *, file_row_number::BIGINT AS raw_row_index, "
        f"({group_case})::INTEGER AS raw_row_group_index FROM {relation}",
        [source_path.as_posix()],
    )
    actual = connection.execute("SELECT count(*) FROM _raw").fetchone()[0]
    if actual != raw_file["row_count"]:
        raise ValueError(f"source row count changed during processing: {source_path}")


def _create_classified_table(
    connection: duckdb.DuckDBPyConnection,
    source: SourceSchema,
    raw_file: dict[str, Any],
    raw_sha: str,
    source_timezone: str,
    calendar_id: str,
    *,
    global_event_stats: bool,
    global_source_stats: bool,
) -> None:
    payload_hash = _payload_hash_expr(source)
    connection.execute("DROP TABLE IF EXISTS _local_event_stats")
    connection.execute("DROP TABLE IF EXISTS _local_event_candidates")
    connection.execute("DROP TABLE IF EXISTS _local_source_stats")
    connection.execute("DROP TABLE IF EXISTS _classified")
    connection.execute(
        "CREATE TEMP TABLE _local_event_candidates AS SELECT MARKET, SYMBOL, TRADINGDATE, TRADINGTIME, "
        "count(*) AS event_count FROM _raw WHERE MARKET IS NOT NULL AND SYMBOL IS NOT NULL "
        "AND TRADINGDATE IS NOT NULL AND TRADINGTIME IS NOT NULL GROUP BY ALL HAVING count(*) > 1"
    )
    connection.execute(
        f"CREATE TEMP TABLE _local_event_stats AS SELECT r.MARKET, r.SYMBOL, r.TRADINGDATE, "
        f"r.TRADINGTIME, c.event_count, count(DISTINCT {payload_hash}) AS payload_variants "
        "FROM _raw r JOIN _local_event_candidates c USING (MARKET, SYMBOL, TRADINGDATE, TRADINGTIME) "
        "GROUP BY ALL"
    )
    connection.execute(
        "CREATE TEMP TABLE _local_source_stats AS SELECT ingest_run_id, source_file, source_row, "
        "count(*) AS source_count FROM _raw WHERE ingest_run_id IS NOT NULL AND source_file IS NOT NULL "
        "AND source_row IS NOT NULL GROUP BY ALL"
    )
    event_table = "_global_event_stats" if global_event_stats else "_local_event_stats"
    source_table = "_global_source_stats" if global_source_stats else "_local_source_stats"
    event_key = " AND ".join(
        f"e.{_sql_ident(name)} IS NOT DISTINCT FROM r.{_sql_ident(name)}"
        for name in ("MARKET", "SYMBOL", "TRADINGDATE", "TRADINGTIME")
    )
    source_key = " AND ".join(
        f"s.{_sql_ident(name)} IS NOT DISTINCT FROM r.{_sql_ident(name)}"
        for name in ("ingest_run_id", "source_file", "source_row")
    )
    product_expr = (
        "CASE WHEN starts_with(r.SYMBOL, 'TF') THEN 'TF' "
        "WHEN starts_with(r.SYMBOL, 'TS') THEN 'TS' "
        "WHEN starts_with(r.SYMBOL, 'TL') THEN 'TL' "
        "WHEN starts_with(r.SYMBOL, 'T') THEN 'T' ELSE '' END"
    )
    # The interval table has already passed effective-date and overlap validation.
    connection.execute(
        f"CREATE TEMP TABLE _classified AS WITH joined AS ("
        f"SELECT r.*, '{raw_sha}'::VARCHAR AS raw_file_sha256, "
        f"sha256('{raw_sha}:' || CAST(r.raw_row_index AS VARCHAR)) AS record_id, "
        f"timezone('{source_timezone}', r.TRADINGTIME)::TIMESTAMPTZ AS event_time_utc, "
        f"COALESCE(e.event_count, 1)::BIGINT AS event_count, "
        f"COALESCE(e.payload_variants, 0)::BIGINT AS payload_variants, "
        f"COALESCE(s.source_count, 1)::BIGINT AS source_count, "
        f"si.segment_id, si.rule_id, si.continuous_trading, "
        f"CASE WHEN si.segment_id IS NULL THEN NULL ELSE '{calendar_id}:' || "
        f"CAST(r.TRADINGDATE AS VARCHAR) || ':' || si.segment_id END AS session_id, "
        f"(r.TRADINGDATE IS NOT NULL AND cd.trade_date IS NULL) AS non_trading_day, "
        f"(r.TRADINGTIME IS NOT NULL AND r.TRADINGDATE IS NOT NULL "
        f"AND CAST(r.TRADINGTIME AS DATE) <> r.TRADINGDATE) AS event_date_mismatch, "
        f"(r.TRADINGTIME IS NOT NULL AND r.UNIX IS NOT NULL AND "
        f"CAST(epoch_ms(timezone('{source_timezone}', r.TRADINGTIME)) AS BIGINT) <> r.UNIX) "
        f"AS unix_time_mismatch, "
        f"(r.BUYPRICE01 IS NULL OR r.SELLPRICE01 IS NULL OR r.BUYVOLUME01 IS NULL OR "
        f"r.SELLVOLUME01 IS NULL OR r.BUYPRICE01 <= 0 OR r.SELLPRICE01 <= 0 OR "
        f"r.BUYVOLUME01 < 0 OR r.SELLVOLUME01 < 0 OR r.BUYPRICE01 > r.SELLPRICE01) AS invalid_l1, "
        f"({product_expr}) AS product_code "
        f"FROM _raw r "
        f"LEFT JOIN {event_table} e ON {event_key} "
        f"LEFT JOIN {source_table} s ON {source_key} "
        f"LEFT JOIN _calendar_dates cd ON cd.trade_date = r.TRADINGDATE "
        f"LEFT JOIN _expiry_map em ON em.instrument_id = r.SYMBOL "
        f"LEFT JOIN _session_intervals si ON si.product = ({product_expr}) "
        f"AND r.TRADINGDATE BETWEEN si.first_date AND si.last_date "
        f"AND CAST(r.TRADINGTIME AS TIME) >= si.start_time "
        f"AND CAST(r.TRADINGTIME AS TIME) < si.end_time "
        f"AND (NOT si.closed_expiry OR em.expiry_date IS NULL OR r.TRADINGDATE <> em.expiry_date)"
        f"), decisions AS (SELECT *, list_filter(["
        "CASE WHEN MARKET IS NULL OR SYMBOL IS NULL OR TRADINGDATE IS NULL OR TRADINGTIME IS NULL "
        "THEN 'MISSING_REQUIRED_FIELD' END, "
        "CASE WHEN non_trading_day THEN 'NON_TRADING_DATE' END, "
        "CASE WHEN event_date_mismatch THEN 'EVENT_DATE_MISMATCH' END, "
        "CASE WHEN unix_time_mismatch OR UNIX IS NULL THEN 'UNIX_TIME_MISMATCH' END, "
        "CASE WHEN segment_id IS NULL THEN 'SESSION_UNCLASSIFIED' END, "
        "CASE WHEN invalid_l1 THEN 'INVALID_L1_BOOK' END, "
        "CASE WHEN event_count > 1 AND payload_variants = 1 THEN 'DUPLICATE_EVENT_KEY_IDENTICAL' END, "
        "CASE WHEN event_count > 1 AND payload_variants > 1 THEN 'DUPLICATE_EVENT_KEY_CONFLICTING' END, "
        "CASE WHEN source_count > 1 THEN 'DUPLICATE_SOURCE_RECORD' END "
        "], x -> x IS NOT NULL) AS reason_codes, "
        "list_filter(["
        "CASE WHEN BUYPRICE02 IS NULL OR SELLPRICE02 IS NULL OR BUYVOLUME02 IS NULL OR SELLVOLUME02 IS NULL "
        "OR BUYPRICE03 IS NULL OR SELLPRICE03 IS NULL OR BUYVOLUME03 IS NULL OR SELLVOLUME03 IS NULL "
        "OR BUYPRICE04 IS NULL OR SELLPRICE04 IS NULL OR BUYVOLUME04 IS NULL OR SELLVOLUME04 IS NULL "
        "OR BUYPRICE05 IS NULL OR SELLPRICE05 IS NULL OR BUYVOLUME05 IS NULL OR SELLVOLUME05 IS NULL "
        "OR BUYPRICE02 <= 0 OR SELLPRICE02 <= 0 OR BUYPRICE03 <= 0 OR SELLPRICE03 <= 0 "
        "OR BUYPRICE04 <= 0 OR SELLPRICE04 <= 0 OR BUYPRICE05 <= 0 OR SELLPRICE05 <= 0 "
        "OR BUYVOLUME02 < 0 OR SELLVOLUME02 < 0 OR BUYVOLUME03 < 0 OR SELLVOLUME03 < 0 "
        "OR BUYVOLUME04 < 0 OR SELLVOLUME04 < 0 OR BUYVOLUME05 < 0 OR SELLVOLUME05 < 0 "
        "THEN 'DEPTH_BOOK_INVALID' END, "
        "'END_OF_DAY_FIELDS_UNVERIFIED', 'VOLUME_INCREMENT_UNVERIFIED' "
        "], x -> x IS NOT NULL) AS quality_flags "
        "FROM joined) SELECT * FROM decisions"
    )


def _output_sql(source: SourceSchema, *, quote: bool) -> str:
    if quote:
        columns: list[str] = []
        for field in source.fields:
            if field.name == "TRADINGTIME":
                columns.append("event_time_utc AS event_time")
            else:
                columns.append(f"{_sql_ident(field.name)} AS {_sql_ident(source.standardized_name(field.name))}")
        columns.extend([
            "record_id", "raw_file_sha256", "raw_row_index", "raw_row_group_index", "session_id",
            "CAST(NULL AS TIMESTAMPTZ) AS available_time", "quality_flags",
        ])
        return ", ".join(columns)
    columns = [f"{_sql_ident(field.name)}" for field in source.fields]
    columns.extend(["record_id", "raw_file_sha256", "raw_row_index", "raw_row_group_index", "reason_codes"])
    return ", ".join(columns)


def _rate_metric(numerator: int, denominator: int, formula: str) -> dict[str, Any]:
    return {
        "numerator": numerator,
        "denominator": denominator,
        "value": numerator / denominator if denominator else None,
        "formula": formula,
    }


def _scoped_quality_summaries(
    connection: duckdb.DuckDBPyConnection,
    source: SourceSchema,
    raw_file: dict[str, Any],
    raw_sha: str,
    *,
    global_event_stats: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Build bounded per-file quality, session reconciliation, and event-gap summaries."""
    null_values = " + ".join(
        f"CASE WHEN {_sql_ident(field.name)} IS NULL THEN 1 ELSE 0 END" for field in source.fields
    )
    base_rows = connection.execute(
        "SELECT MARKET, SYMBOL, TRADINGDATE, count(*) AS row_count, "
        "count(*) FILTER (WHERE len(reason_codes)=0) AS accepted_rows, "
        "count(*) FILTER (WHERE len(reason_codes)>0) AS quarantined_rows, "
        "count(*) FILTER (WHERE event_count>1) AS duplicate_member_rows, "
        "count(*) FILTER (WHERE invalid_l1) AS invalid_quote_rows, "
        "count(*) FILTER (WHERE BUYPRICE01 IS NOT NULL AND SELLPRICE01 IS NOT NULL "
        "AND BUYPRICE01>0 AND SELLPRICE01>0) AS valid_price_pair_rows, "
        "count(*) FILTER (WHERE BUYPRICE01 IS NOT NULL AND SELLPRICE01 IS NOT NULL "
        "AND BUYPRICE01>0 AND SELLPRICE01>0 AND BUYPRICE01>SELLPRICE01) AS crossed_book_rows, "
        f"sum({null_values}) AS null_field_values, "
        "count(*) FILTER (WHERE session_id IS NULL) AS unclassified_session_rows, "
        "count(*) FILTER (WHERE list_contains(quality_flags,'DEPTH_BOOK_INVALID')) AS depth_invalid_source_rows, "
        "count(*) FILTER (WHERE len(reason_codes)=0 AND "
        "list_contains(quality_flags,'DEPTH_BOOK_INVALID')) AS depth_invalid_rows "
        "FROM _classified GROUP BY MARKET, SYMBOL, TRADINGDATE "
        "ORDER BY MARKET NULLS LAST, SYMBOL NULLS LAST, TRADINGDATE NULLS LAST"
    ).fetchall()

    duplicate_rows = connection.execute(
        "WITH duplicate_keys AS (SELECT MARKET, SYMBOL, TRADINGDATE, TRADINGTIME, "
        "max(event_count) AS event_count FROM _classified WHERE event_count>1 GROUP BY ALL), "
        "duplicate_extras AS (SELECT c.MARKET, c.SYMBOL, c.TRADINGDATE, "
        "count(*) AS duplicate_extra_rows FROM _classified c "
        "JOIN _global_event_extra e ON e.MARKET IS NOT DISTINCT FROM c.MARKET "
        "AND e.SYMBOL IS NOT DISTINCT FROM c.SYMBOL AND e.TRADINGDATE IS NOT DISTINCT FROM c.TRADINGDATE "
        "AND e.TRADINGTIME IS NOT DISTINCT FROM c.TRADINGTIME "
        "AND e.raw_file_sha256=c.raw_file_sha256 AND e.raw_row_index=c.raw_row_index "
        "GROUP BY c.MARKET,c.SYMBOL,c.TRADINGDATE) "
        "SELECT d.MARKET,d.SYMBOL,d.TRADINGDATE,count(*) AS duplicate_event_groups, "
        "coalesce(max(x.duplicate_extra_rows),0) AS duplicate_extra_rows FROM duplicate_keys d "
        "LEFT JOIN duplicate_extras x ON x.MARKET IS NOT DISTINCT FROM d.MARKET "
        "AND x.SYMBOL IS NOT DISTINCT FROM d.SYMBOL AND x.TRADINGDATE IS NOT DISTINCT FROM d.TRADINGDATE "
        "GROUP BY d.MARKET,d.SYMBOL,d.TRADINGDATE"
        if global_event_stats else
        "WITH duplicate_keys AS (SELECT MARKET, SYMBOL, TRADINGDATE, TRADINGTIME, "
        "count(*) AS event_count FROM _raw WHERE MARKET IS NOT NULL AND SYMBOL IS NOT NULL "
        "AND TRADINGDATE IS NOT NULL AND TRADINGTIME IS NOT NULL GROUP BY ALL HAVING count(*)>1) "
        "SELECT MARKET, SYMBOL, TRADINGDATE, count(*) AS duplicate_event_groups, "
        "sum(event_count-1) AS duplicate_extra_rows FROM duplicate_keys "
        "GROUP BY MARKET, SYMBOL, TRADINGDATE"
    ).fetchall()
    duplicate_by_scope = {
        (exchange, instrument, trade_day): (int(groups), int(extra_rows))
        for exchange, instrument, trade_day, groups, extra_rows in duplicate_rows
    }

    reorder_rows = connection.execute(
        "WITH ordered AS (SELECT MARKET, SYMBOL, TRADINGDATE, TRADINGTIME, "
        "lag(TRADINGTIME) OVER (PARTITION BY ingest_run_id, source_file, SYMBOL, TRADINGDATE "
        "ORDER BY raw_row_index) AS prior_time FROM _raw) "
        "SELECT MARKET, SYMBOL, TRADINGDATE, count(*) FILTER (WHERE prior_time IS NOT NULL "
        "AND TRADINGTIME < prior_time) AS time_reorder_count FROM ordered "
        "GROUP BY MARKET, SYMBOL, TRADINGDATE"
    ).fetchall()
    reorder_by_scope = {
        (exchange, instrument, trade_day): int(count)
        for exchange, instrument, trade_day, count in reorder_rows
    }

    scopes: list[dict[str, Any]] = []
    for (exchange, instrument, trade_day, row_count, accepted, quarantined, duplicate_members,
         invalid_quotes, valid_price_pairs, crossed, null_count, unclassified,
         depth_invalid_source, depth_invalid) in base_rows:
        key = (exchange, instrument, trade_day)
        rows = int(row_count)
        null_denominator = rows * len(source.fields)
        dup_groups, duplicate_extra = duplicate_by_scope.get(key, (0, 0))
        scope = {
            "scope": "raw_file/contract/trade_date",
            "raw_file_sha256": raw_sha,
            "source_uri": raw_file["uri"],
            "exchange": exchange,
            "instrument_id": instrument,
            "trade_date": trade_day.isoformat() if trade_day is not None else None,
            "row_count": rows,
            "accepted_rows": int(accepted),
            "quarantined_rows": int(quarantined),
            "quarantine_rate": _rate_metric(
                int(quarantined), rows, "quarantined_rows / input row_count"
            ),
            "duplicate_event_groups": dup_groups,
            "duplicate_event_extra_rows": duplicate_extra,
            "duplicate_member_rows": int(duplicate_members),
            "duplicate_rate": _rate_metric(
                duplicate_extra, rows, "sum(event_key_group_size - 1) / input row_count"
            ),
            "invalid_quote_rows": int(invalid_quotes),
            "invalid_quote_rate": _rate_metric(
                int(invalid_quotes), rows, "invalid L1 rows / input row_count"
            ),
            "null_field_values": int(null_count or 0),
            "null_field_value_denominator": null_denominator,
            "null_rate": _rate_metric(
                int(null_count or 0), null_denominator,
                "null source-field values / (input row_count * physical source field count)",
            ),
            "crossed_book_rows": int(crossed),
            "valid_price_pair_rows": int(valid_price_pairs),
            "crossed_book_rate": _rate_metric(
                int(crossed), int(valid_price_pairs),
                "crossed positive-price pairs / positive non-null L1 price pairs",
            ),
            "time_reorder_count": reorder_by_scope.get(key, 0),
            "time_reorder_denominator": rows,
            "session_unclassified_rows": int(unclassified),
            "depth_invalid_rows": int(depth_invalid),
            "depth_invalid_source_rows": int(depth_invalid_source),
            "stale_quote_segments": {
                "value": None,
                "status": "NOT_EVALUATED",
                "denominator": None,
                "reason": (
                    "True quote age and stale duration cannot be established: source available_time is absent, "
                    "and feed cadence/change-driven update semantics and a stale threshold are unverified. "
                    "Event-time observation gaps are reported separately as a proxy."
                ),
            },
        }
        scopes.append(scope)

    session_rows = connection.execute(
        "SELECT MARKET, SYMBOL, TRADINGDATE, session_id, count(*) AS row_count, "
        "count(*) FILTER (WHERE len(reason_codes)=0) AS accepted_rows, "
        "count(*) FILTER (WHERE len(reason_codes)>0) AS quarantined_rows "
        "FROM _classified GROUP BY MARKET, SYMBOL, TRADINGDATE, session_id "
        "ORDER BY MARKET NULLS LAST, SYMBOL NULLS LAST, TRADINGDATE NULLS LAST, session_id NULLS LAST"
    ).fetchall()
    session_scopes = [{
        "scope": "raw_file/contract/trade_date/session",
        "raw_file_sha256": raw_sha,
        "exchange": exchange,
        "instrument_id": instrument,
        "trade_date": trade_day.isoformat() if trade_day is not None else None,
        "session_id": session_id,
        "classification": "unclassified" if session_id is None else "evidenced_session",
        "row_count": int(rows),
        "accepted_rows": int(accepted),
        "quarantined_rows": int(quarantined),
    } for exchange, instrument, trade_day, session_id, rows, accepted, quarantined in session_rows]

    gap_rows = connection.execute(
        "WITH valid_times AS (SELECT DISTINCT MARKET, SYMBOL, TRADINGDATE, session_id, event_time_utc "
        "FROM _classified WHERE session_id IS NOT NULL AND non_trading_day=false "
        "AND event_date_mismatch=false AND unix_time_mismatch=false), "
        "group_counts AS (SELECT MARKET, SYMBOL, TRADINGDATE, session_id, count(*) AS unique_event_times "
        "FROM valid_times GROUP BY ALL), "
        "ordered AS (SELECT MARKET, SYMBOL, TRADINGDATE, session_id, event_time_utc, "
        "epoch_us(event_time_utc)-epoch_us(lag(event_time_utc) OVER (PARTITION BY MARKET, SYMBOL, "
        "TRADINGDATE, session_id ORDER BY event_time_utc)) AS gap_us FROM valid_times), "
        "gap_stats AS (SELECT MARKET, SYMBOL, TRADINGDATE, session_id, count(*) AS positive_interval_count, "
        "quantile_cont(gap_us/1000000.0, 0.50) AS p50_seconds, "
        "quantile_cont(gap_us/1000000.0, 0.90) AS p90_seconds, "
        "quantile_cont(gap_us/1000000.0, 0.99) AS p99_seconds FROM ordered "
        "WHERE gap_us>0 GROUP BY ALL) "
        "SELECT c.MARKET, c.SYMBOL, c.TRADINGDATE, c.session_id, c.unique_event_times, "
        "coalesce(g.positive_interval_count,0), g.p50_seconds, g.p90_seconds, g.p99_seconds "
        "FROM group_counts c LEFT JOIN gap_stats g USING(MARKET,SYMBOL,TRADINGDATE,session_id) "
        "ORDER BY c.MARKET, c.SYMBOL, c.TRADINGDATE, c.session_id"
    ).fetchall()
    gap_summaries = [{
        "scope": "raw_file/contract/trade_date/session",
        "raw_file_sha256": raw_sha,
        "exchange": exchange,
        "instrument_id": instrument,
        "trade_date": trade_day.isoformat(),
        "session_id": session_id,
        "unique_valid_event_time_count": int(unique_times),
        "positive_interval_count": int(intervals),
        "sample_count": int(intervals),
        "p50_seconds": float(p50) if p50 is not None else None,
        "p90_seconds": float(p90) if p90 is not None else None,
        "p99_seconds": float(p99) if p99 is not None else None,
        "formula": "positive adjacent deltas after distinct valid event_time values, grouped within one raw file and evidenced session",
        "status": "EVALUATED" if int(intervals) else "NOT_EVALUATED",
        "reason": None if int(intervals) else "Fewer than two unique valid event times in this session scope.",
    } for exchange, instrument, trade_day, session_id, unique_times, intervals, p50, p90, p99 in gap_rows]
    return scopes, session_scopes, gap_summaries


def _arrow_path_from_ref(ref: ArtifactRef) -> Path:
    return _path_from_uri(ref.uri)


class OnboardStage:
    name = "onboard"
    version = ENTRY_VERSION
    dependencies = ("bootstrap",)
    partitioned = False
    config_keys = (
        "input.source_schema_path", "input.source_timezone", "input.adapter",
        "calendar.provider", "calendar.data_root", "calendar.evidence_path", "calendar.calendar_id",
        "calendar.base_url", "calendar.ca_bundle", "calendar.frozen_snapshot_path",
        "calendar.frozen_snapshot_sha256", "sessions.evidence_path", "resources.memory_limit",
        "resources.threads", "resources.temporary_uri", "onboarding.max_quarantine_rate",
    )

    def register_validators(self, store: ArtifactStore) -> None:
        store.register_validator("quotes", self.version, validate_quotes_artifact)
        store.register_validator("quarantine", self.version, validate_quarantine_artifact)
        store.register_validator("calendar", self.version, validate_calendar_artifact)
        store.register_validator("session_rules", self.version, validate_session_rules_artifact)
        store.register_validator("onboarding_entry", self.version, validate_onboarding_entry_artifact)

    def validate(self, ctx: RunContext, inputs: dict[str, ArtifactRef]) -> None:
        del inputs
        if not ctx.data_snapshot_id or not ctx.split_id:
            raise ValueError("onboard requires frozen data_snapshot_id and split_id context values")
        if ctx.config.get("input", {}).get("adapter") != "l2_snapshots":
            raise ValueError("onboard currently supports the l2_snapshots adapter")
        schema = _source_schema(dict(ctx.config))
        input_config = ctx.config["input"]
        if not input_config.get("source_timezone"):
            raise ValueError("input.source_timezone must be explicit")
        if not Path(ctx.config.get("sessions", {}).get("evidence_path", "")).is_file():
            raise ValueError("sessions.evidence_path must reference frozen session-rule evidence")
        _schema_types(schema)
        if ctx.config.get("onboarding", {}).get("max_quarantine_rate") is not None:
            threshold = float(ctx.config["onboarding"]["max_quarantine_rate"])
            if not 0 <= threshold <= 1:
                raise ValueError("onboarding.max_quarantine_rate must be between zero and one")

    def execute(self, ctx: RunContext, inputs: dict[str, ArtifactRef]) -> StageResult:
        inventory, _ = _source_inventory(ctx, inputs)
        source = _source_schema(dict(ctx.config))
        quote_schema, quarantine_schema = _schema_types(source)
        raw_files: list[dict[str, Any]] = inventory["raw_files"]
        raw_rows = int(inventory["input_row_count"])
        source_dates = [date.fromisoformat(item) for item in inventory.get("source_dates", [])]
        if not source_dates:
            raise ValueError("source inventory has no usable TRADINGDATE values")
        calendar_snapshot = _calendar_snapshot(dict(ctx.config), inventory)
        calendar_table = calendar_snapshot.table
        calendar_ids = set(calendar_table.column("calendar_id").to_pylist())
        if len(calendar_ids) != 1:
            raise ValueError(f"calendar snapshot has multiple calendar IDs: {sorted(calendar_ids)}")
        calendar_id = next(iter(calendar_ids))
        calendar_dates = set(calendar_table.column("trade_date").to_pylist())
        if None in calendar_dates:
            raise ValueError("calendar snapshot contains null trade dates")
        session_evidence_path = Path(ctx.config["sessions"]["evidence_path"]).expanduser().resolve()
        if ctx.config["sessions"].get("evidence_sha256") != sha256_file(session_evidence_path):
            raise ValueError("session evidence hash differs from preflight configuration")
        frozen_sessions, session_intervals = _build_session_intervals(session_evidence_path)
        timezone_name = str(ctx.config["input"]["source_timezone"])
        if timezone_name != frozen_sessions["timezone"]:
            raise ValueError("input.source_timezone differs from frozen session evidence timezone")
        input_refs = {ref.artifact_type: ref for ref in inputs.values()}
        if set(input_refs).isdisjoint({"source_snapshot", "split_manifest"}):
            raise ValueError("onboard did not receive the bootstrap snapshot and split refs")

        transaction = ctx.artifact_transaction
        output_refs: dict[str, ArtifactRef] = {}
        output_quality_flags: dict[str, tuple[str, ...]] = {}
        quote_refs: list[ArtifactRef] = []
        quarantine_refs: list[ArtifactRef] = []
        partitions: list[dict[str, Any]] = []
        quality_scopes: list[dict[str, Any]] = []
        session_scopes: list[dict[str, Any]] = []
        event_time_gap_summaries: list[dict[str, Any]] = []
        gap_interval_count = 0
        quality_hits: Counter[str] = Counter()
        quality_flag_hits: Counter[str] = Counter()
        rule_examples: dict[str, list[dict[str, Any]]] = {}
        source_order_reorders = 0
        increment_mismatches = 0
        volume_increment_mismatches = 0
        turnover_increment_mismatches = 0
        cumulative_resets = 0
        volume_cumulative_resets = 0
        turnover_cumulative_resets = 0
        identical_event_groups = 0
        identical_event_rows = 0
        identical_event_extra_rows = 0
        conflicting_event_groups = 0
        conflicting_event_rows = 0
        conflicting_event_extra_rows = 0
        quote_rows = 0
        quarantine_rows = 0
        temporary_base = ctx.resource_budget.temporary_uri or ctx.config.get("storage", {}).get("temporary_uri")
        scratch_parent = Path(temporary_base).expanduser().resolve() if temporary_base else Path(tempfile.gettempdir())
        scratch_parent.mkdir(parents=True, exist_ok=True)

        calendar_alias = "calendar"
        calendar_path = transaction.reserve_output(
            calendar_alias,
            artifact_type="calendar",
            schema_version=self.version,
            relative_path="calendar.parquet",
            row_count=calendar_table.num_rows,
            quality_flags=["CFFEX_APPLICABILITY_VERIFIED"],
        )
        pq.write_table(calendar_table, calendar_path, compression="zstd")
        output_refs[calendar_alias] = transaction.register_output(calendar_alias)
        output_quality_flags[calendar_alias] = ("CFFEX_APPLICABILITY_VERIFIED",)

        session_alias = "session_rules"
        session_path = transaction.reserve_output(
            session_alias,
            artifact_type="session_rules",
            schema_version=self.version,
            relative_path="session_rules.json",
            quality_flags=["EVIDENCE_HASH_PINNED"],
        )
        session_path.write_text(canonical_json(frozen_sessions) + "\n", encoding="utf-8")
        output_refs[session_alias] = transaction.register_output(session_alias)
        output_quality_flags[session_alias] = ("EVIDENCE_HASH_PINNED",)

        with tempfile.TemporaryDirectory(prefix="onboard-", dir=scratch_parent) as scratch:
            connection = duckdb.connect(
                database=":memory:",
                config={
                    "memory_limit": ctx.resource_budget.memory_limit,
                    "threads": str(min(ctx.resource_budget.threads, 4)),
                    "temp_directory": scratch,
                    "preserve_insertion_order": "true",
                },
            )
            try:
                connection.register("_calendar_arrow", calendar_table)
                connection.execute("CREATE TEMP TABLE _calendar_dates AS SELECT trade_date FROM _calendar_arrow")
                connection.register("_session_intervals_arrow", session_intervals)
                connection.execute("CREATE TEMP TABLE _session_intervals AS SELECT * FROM _session_intervals_arrow")
                expiry_table = _expiry_rows(list(inventory["instrument_ids"]), calendar_dates)
                connection.register("_expiry_arrow", expiry_table)
                connection.execute("CREATE TEMP TABLE _expiry_map AS SELECT * FROM _expiry_arrow")
                event_overlaps = not bool(inventory.get("source_file_intervals_disjoint", False))
                source_overlaps = not bool(inventory.get("source_provenance_pairs_disjoint", False))
                _register_global_duplicate_indexes(
                    connection, raw_files, source, event_keys_overlap=event_overlaps,
                    provenance_pairs_overlap=source_overlaps,
                )
                global_event = event_overlaps and all(item["source_format"] == "parquet" for item in raw_files)
                global_source = source_overlaps and all(item["source_format"] == "parquet" for item in raw_files)
                if global_event:
                    duplicate_metrics = connection.execute(
                        "SELECT count(*) FILTER (WHERE payload_variants=1), "
                        "sum(event_count) FILTER (WHERE payload_variants=1), "
                        "sum(event_count-1) FILTER (WHERE payload_variants=1), "
                        "count(*) FILTER (WHERE payload_variants>1), "
                        "sum(event_count) FILTER (WHERE payload_variants>1), "
                        "sum(event_count-1) FILTER (WHERE payload_variants>1) "
                        "FROM _global_event_stats"
                    ).fetchone()
                    (identical_event_groups, identical_event_rows, identical_event_extra_rows,
                     conflicting_event_groups, conflicting_event_rows, conflicting_event_extra_rows) = (
                        int(value or 0) for value in duplicate_metrics
                    )
                for file_index, raw_file in enumerate(raw_files):
                    source_path = _path_from_uri(raw_file["uri"])
                    raw_sha = bare_sha256(str(raw_file["sha256"]))
                    if ctx.logger is not None:
                        ctx.logger.emit(
                            "onboarding_source_file_start",
                            stage=self.name,
                            stage_run_id=transaction.stage_run_id,
                            source_index=file_index + 1,
                            source_count=len(raw_files),
                            source_uri=raw_file["uri"],
                            source_rows=raw_file["row_count"],
                        )
                    if raw_file["source_format"] == "csv":
                        converted = Path(scratch) / f"source-{file_index:04d}.parquet"
                        _prepare_csv_parquet(source_path, source, converted)
                        processing_path = converted
                    else:
                        processing_path = source_path
                    parquet_source = pq.ParquetFile(processing_path)
                    validate_physical_schema(parquet_source.schema_arrow, source)
                    _create_raw_table(connection, processing_path, raw_file)
                    _create_classified_table(
                        connection, source, raw_file, raw_sha, timezone_name, calendar_id,
                        global_event_stats=global_event,
                        global_source_stats=global_source,
                    )
                    if not global_event:
                        duplicate_metrics = connection.execute(
                            "SELECT count(*) FILTER (WHERE payload_variants=1), "
                            "sum(event_count) FILTER (WHERE payload_variants=1), "
                            "sum(event_count-1) FILTER (WHERE payload_variants=1), "
                            "count(*) FILTER (WHERE payload_variants>1), "
                            "sum(event_count) FILTER (WHERE payload_variants>1), "
                            "sum(event_count-1) FILTER (WHERE payload_variants>1) "
                            "FROM _local_event_stats"
                        ).fetchone()
                        identical_event_groups += int(duplicate_metrics[0] or 0)
                        identical_event_rows += int(duplicate_metrics[1] or 0)
                        identical_event_extra_rows += int(duplicate_metrics[2] or 0)
                        conflicting_event_groups += int(duplicate_metrics[3] or 0)
                        conflicting_event_rows += int(duplicate_metrics[4] or 0)
                        conflicting_event_extra_rows += int(duplicate_metrics[5] or 0)
                    rows = connection.execute(
                        "SELECT count(*) AS total, count(*) FILTER (WHERE len(reason_codes)=0) AS accepted, "
                        "count(*) FILTER (WHERE len(reason_codes)>0) AS quarantined, "
                        "count(*) FILTER (WHERE list_contains(reason_codes,'MISSING_REQUIRED_FIELD')) AS missing, "
                        "count(*) FILTER (WHERE list_contains(reason_codes,'NON_TRADING_DATE')) AS nontrading, "
                        "count(*) FILTER (WHERE list_contains(reason_codes,'SESSION_UNCLASSIFIED')) AS session_bad, "
                        "count(*) FILTER (WHERE list_contains(reason_codes,'UNIX_TIME_MISMATCH')) AS unix_bad, "
                        "count(*) FILTER (WHERE list_contains(reason_codes,'EVENT_DATE_MISMATCH')) AS date_bad, "
                        "count(*) FILTER (WHERE list_contains(reason_codes,'INVALID_L1_BOOK')) AS l1_bad, "
                        "count(*) FILTER (WHERE list_contains(reason_codes,'DUPLICATE_EVENT_KEY_IDENTICAL')) AS dup_identical, "
                        "count(*) FILTER (WHERE list_contains(reason_codes,'DUPLICATE_EVENT_KEY_CONFLICTING')) AS dup_conflicting, "
                        "count(*) FILTER (WHERE list_contains(reason_codes,'DUPLICATE_SOURCE_RECORD')) AS dup_source, "
                        "count(*) FILTER (WHERE len(reason_codes)=0 AND "
                        "list_contains(quality_flags,'DEPTH_BOOK_INVALID')) AS depth_bad_accepted "
                        "FROM _classified"
                    ).fetchone()
                    if int(rows[0]) != int(raw_file["row_count"]):
                        raise ValueError(f"classified row count mismatch for {source_path}")
                    (_, accepted, quarantined, missing, nontrading, session_bad, unix_bad, date_bad,
                     l1_bad, dup_i, dup_c, dup_s, depth_bad_accepted) = map(int, rows)
                    quote_rows += accepted
                    quarantine_rows += quarantined
                    quality_hits.update({
                        "MISSING_REQUIRED_FIELD": missing,
                        "NON_TRADING_DATE": nontrading,
                        "SESSION_UNCLASSIFIED": session_bad,
                        "UNIX_TIME_MISMATCH": unix_bad,
                        "EVENT_DATE_MISMATCH": date_bad,
                        "INVALID_L1_BOOK": l1_bad,
                        "DUPLICATE_EVENT_KEY_IDENTICAL": dup_i,
                        "DUPLICATE_EVENT_KEY_CONFLICTING": dup_c,
                        "DUPLICATE_SOURCE_RECORD": dup_s,
                    })
                    quality_flag_hits.update({
                        "DEPTH_BOOK_INVALID": depth_bad_accepted,
                        "END_OF_DAY_FIELDS_UNVERIFIED": accepted,
                        "VOLUME_INCREMENT_UNVERIFIED": accepted,
                    })
                    source_example = {
                        "uri": raw_file["uri"],
                        "sha256": raw_file["sha256"],
                        "row_count": int(raw_file["row_count"]),
                    }
                    file_reason_hits = {
                        "MISSING_REQUIRED_FIELD": missing,
                        "NON_TRADING_DATE": nontrading,
                        "SESSION_UNCLASSIFIED": session_bad,
                        "UNIX_TIME_MISMATCH": unix_bad,
                        "EVENT_DATE_MISMATCH": date_bad,
                        "INVALID_L1_BOOK": l1_bad,
                        "DUPLICATE_EVENT_KEY_IDENTICAL": dup_i,
                        "DUPLICATE_EVENT_KEY_CONFLICTING": dup_c,
                        "DUPLICATE_SOURCE_RECORD": dup_s,
                        "DEPTH_BOOK_INVALID": depth_bad_accepted,
                    }
                    for rule_id, hit_count in file_reason_hits.items():
                        if hit_count and len(rule_examples.setdefault(rule_id, [])) < 3:
                            rule_examples[rule_id].append(source_example)
                    file_scopes, file_session_scopes, file_gap_summaries = _scoped_quality_summaries(
                        connection, source, raw_file, raw_sha, global_event_stats=global_event
                    )
                    quality_scopes.extend(file_scopes)
                    session_scopes.extend(file_session_scopes)
                    event_time_gap_summaries.extend(file_gap_summaries)
                    gap_interval_count += sum(
                        int(item["positive_interval_count"]) for item in file_gap_summaries
                    )
                    if any(item["positive_interval_count"] for item in file_gap_summaries) and len(
                        rule_examples.setdefault("TIME_GAP_QUANTILES", [])
                    ) < 3:
                        rule_examples["TIME_GAP_QUANTILES"].append(source_example)
                    # Source order is checked against physical row position before output sorting.
                    file_reorders = int(connection.execute(
                        "WITH ordered AS (SELECT TRADINGTIME, lag(TRADINGTIME) OVER ("
                        "PARTITION BY ingest_run_id, source_file, SYMBOL, TRADINGDATE "
                        "ORDER BY raw_row_index) AS prior_time FROM _raw) "
                        "SELECT count(*) FROM ordered WHERE prior_time IS NOT NULL AND TRADINGTIME < prior_time"
                    ).fetchone()[0])
                    source_order_reorders += file_reorders
                    if file_reorders and len(rule_examples.setdefault("PHYSICAL_ORDER_AUDIT", [])) < 3:
                        rule_examples["PHYSICAL_ORDER_AUDIT"].append(source_example)
                    increment_stats = connection.execute(
                        "WITH ordered AS (SELECT ingest_run_id, source_file, SYMBOL, TRADINGDATE, raw_row_index, "
                        "TOTALVOLUME, TOTALAMOUNT, TRADEVOLUME, TRADEAMOUNT, "
                        "lag(TOTALVOLUME) OVER (PARTITION BY ingest_run_id, source_file, SYMBOL, TRADINGDATE "
                        "ORDER BY raw_row_index) AS prior_volume, "
                        "lag(TOTALAMOUNT) OVER (PARTITION BY ingest_run_id, source_file, SYMBOL, TRADINGDATE "
                        "ORDER BY raw_row_index) AS prior_amount FROM _raw) "
                        "SELECT count(*) FILTER (WHERE prior_volume IS NOT NULL AND TOTALVOLUME >= prior_volume "
                        "AND TRADEVOLUME IS NOT NULL AND TOTALVOLUME - prior_volume <> TRADEVOLUME), "
                        "count(*) FILTER (WHERE prior_volume IS NOT NULL AND TOTALVOLUME < prior_volume), "
                        "count(*) FILTER (WHERE prior_amount IS NOT NULL AND TOTALAMOUNT >= prior_amount "
                        "AND TRADEAMOUNT IS NOT NULL AND TOTALAMOUNT - prior_amount <> TRADEAMOUNT), "
                        "count(*) FILTER (WHERE prior_amount IS NOT NULL AND TOTALAMOUNT < prior_amount), "
                        "count(*) FILTER (WHERE (prior_volume IS NOT NULL AND TOTALVOLUME < prior_volume) "
                        "OR (prior_amount IS NOT NULL AND TOTALAMOUNT < prior_amount)) "
                        "FROM ordered"
                    ).fetchone()
                    file_volume_mismatches = int(increment_stats[0] or 0)
                    file_volume_resets = int(increment_stats[1] or 0)
                    file_turnover_mismatches = int(increment_stats[2] or 0)
                    file_turnover_resets = int(increment_stats[3] or 0)
                    file_cumulative_resets = int(increment_stats[4] or 0)
                    file_increment_mismatches = (
                        file_volume_mismatches + file_turnover_mismatches
                    )
                    increment_mismatches += file_increment_mismatches
                    volume_increment_mismatches += file_volume_mismatches
                    turnover_increment_mismatches += file_turnover_mismatches
                    cumulative_resets += file_cumulative_resets
                    volume_cumulative_resets += file_volume_resets
                    turnover_cumulative_resets += file_turnover_resets
                    if (file_increment_mismatches or file_cumulative_resets) and len(
                        rule_examples.setdefault("CUMULATIVE_INCREMENT_RECONCILIATION", [])
                    ) < 3:
                        rule_examples["CUMULATIVE_INCREMENT_RECONCILIATION"].append(source_example)

                    pairs = connection.execute(
                        "SELECT MARKET AS exchange, SYMBOL AS instrument_id, TRADINGDATE AS trade_date, "
                        "count(*) AS rows, min(event_time_utc), max(event_time_utc) FROM _classified "
                        "WHERE len(reason_codes)=0 GROUP BY ALL ORDER BY exchange, instrument_id, trade_date"
                    ).fetchall()
                    partition_root = Path(scratch) / f"quotes-{file_index:04d}"
                    if pairs:
                        connection.execute(
                            f"COPY (SELECT {_output_sql(source, quote=True)} FROM _classified "
                            "WHERE len(reason_codes)=0 "
                            "ORDER BY MARKET, SYMBOL, TRADINGDATE, event_time_utc, raw_file_sha256, raw_row_index) "
                            f"TO {_sql_string(partition_root.as_posix())} "
                            "(FORMAT PARQUET, PARTITION_BY (instrument_id, trade_date), "
                            "WRITE_PARTITION_COLUMNS true, PER_THREAD_OUTPUT false, "
                            "COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
                        )
                    for part_index, (exchange, instrument_id, trade_date, part_rows, time_min, time_max) in enumerate(pairs):
                        if exchange != "CFFEX":
                            raise ValueError(f"source contains unsupported exchange {exchange!r}")
                        if not re.fullmatch(r"(?:TF|TS|TL|T)\d{4}", str(instrument_id)):
                            raise ValueError(f"source contains unsupported instrument_id {instrument_id!r}")
                        trade_date_text = trade_date.isoformat()
                        alias = f"quote_{file_index:04d}_{part_index:04d}"
                        relative = (
                            f"quotes/instrument_id={instrument_id}/trade_date={trade_date_text}/part-0.parquet"
                        )
                        part_path = transaction.reserve_output(
                            alias,
                            artifact_type="quotes",
                            schema_version=self.version,
                            relative_path=relative,
                            partition_keys={"exchange": exchange, "instrument_id": instrument_id,
                                            "trade_date": trade_date_text},
                            row_count=int(part_rows),
                            time_min=time_min.isoformat(),
                            time_max=time_max.isoformat(),
                            quality_flags=["RESEARCH_ONLY", "AVAILABILITY_UNVERIFIED"],
                        )
                        partition_dir = (
                            partition_root
                            / f"instrument_id={instrument_id}"
                            / f"trade_date={trade_date.isoformat()}"
                        )
                        partition_sources = sorted(partition_dir.glob("*.parquet"))
                        if not partition_sources:
                            raise ValueError(f"DuckDB partition output is missing: {partition_dir}")
                        try:
                            _merge_partition_files(
                                connection,
                                partition_sources,
                                part_path,
                                expected_rows=int(part_rows),
                                expected_schema=quote_schema,
                            )
                        except ValueError as exc:
                            raise ValueError(
                                f"quote partition footer reconciliation failed for "
                                f"{instrument_id} {trade_date}: {exc}"
                            ) from exc
                        ref = transaction.register_output(alias)
                        quote_refs.append(ref)
                        output_refs[alias] = ref
                        output_quality_flags[alias] = ("RESEARCH_ONLY", "AVAILABILITY_UNVERIFIED")
                        partitions.append({
                            "exchange": exchange,
                            "instrument_id": instrument_id,
                            "trade_date": trade_date_text,
                            "row_count": int(part_rows),
                            "event_time_min": time_min.isoformat(),
                            "event_time_max": time_max.isoformat(),
                            "artifact_ref": transaction.final_ref(ref).to_dict(),
                        })

                    quarantine_alias = f"quarantine_{file_index:04d}"
                    quarantine_path = transaction.reserve_output(
                        quarantine_alias,
                        artifact_type="quarantine",
                        schema_version=self.version,
                        relative_path=f"quarantine/part-{file_index:04d}.parquet",
                        row_count=quarantined,
                        quality_flags=["ROW_LEVEL_REASONS"],
                    )
                    connection.execute(
                        f"COPY (SELECT {_output_sql(source, quote=False)} FROM _classified "
                        "WHERE len(reason_codes)>0 ORDER BY raw_row_index) "
                        f"TO {_sql_string(quarantine_path.as_posix())} "
                        "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)",
                    )
                    _validate_parquet_schema(quarantine_path, quarantine_schema, "quarantine")
                    q_ref = transaction.register_output(quarantine_alias)
                    quarantine_refs.append(q_ref)
                    output_refs[quarantine_alias] = q_ref
                    output_quality_flags[quarantine_alias] = ("ROW_LEVEL_REASONS",)
                    if ctx.logger is not None:
                        ctx.logger.emit(
                            "onboarding_source_file_complete",
                            stage=self.name,
                            stage_run_id=transaction.stage_run_id,
                            source_index=file_index + 1,
                            source_count=len(raw_files),
                            input_rows=int(rows[0]),
                            accepted_rows=accepted,
                            quarantined_rows=quarantined,
                            quote_partitions=len(pairs),
                        )
                    connection.execute("DROP TABLE _classified")
                    connection.execute("DROP TABLE _raw")
            finally:
                connection.close()

        if quote_rows <= 0:
            raise ValueError("onboarding produced zero accepted rows; no READY entry can be published")
        if quote_rows + quarantine_rows != raw_rows:
            raise ValueError(
                f"onboarding reconciliation failed: input={raw_rows}, accepted={quote_rows}, "
                f"quarantined={quarantine_rows}"
            )
        max_quarantine_rate = ctx.config.get("onboarding", {}).get("max_quarantine_rate")
        quarantine_rate = quarantine_rows / raw_rows if raw_rows else 0.0
        if max_quarantine_rate is not None and quarantine_rate > float(max_quarantine_rate):
            raise ValueError(
                f"quarantine rate {quarantine_rate:.6%} exceeds configured maximum {max_quarantine_rate:.6%}"
            )

        # Predict complete data artifact manifests before writing the only consumer entry.
        manifest_input_ids = sorted({ref.artifact_id for ref in inputs.values()})
        embedded_manifests: dict[str, dict[str, Any]] = {}
        final_refs: dict[str, ArtifactRef] = {}
        for alias, ref in output_refs.items():
            final_ref = transaction.final_ref(ref)
            final_refs[alias] = final_ref
            embedded_manifests[alias] = ArtifactManifest(
                artifact_id=final_ref.artifact_id,
                artifact_type=final_ref.artifact_type,
                schema_version=final_ref.schema_version,
                uri=final_ref.uri,
                content_hash=final_ref.content_hash,
                manifest_uri=final_ref.manifest_uri or "",
                run_id=ctx.run_id,
                stage_run_id=transaction.stage_run_id,
                stage_name=self.name,
                stage_version=self.version,
                cache_key=str(final_ref.cache_key),
                config_hash=ctx.config_hash,
                code_revision=ctx.code_revision,
                input_artifact_ids=tuple(manifest_input_ids),
                data_snapshot_id=ctx.data_snapshot_id,
                split_id=ctx.split_id,
                partition_keys=final_ref.partition_keys,
                time_min=final_ref.time_min,
                time_max=final_ref.time_max,
                row_count=final_ref.row_count,
                quality_flags=output_quality_flags[alias],
                created_at="pending_commit_timestamp",
            ).to_dict()

        expected_missing_dates = sorted(
            day.isoformat() for day in calendar_dates
            if day not in {date.fromisoformat(value) for value in inventory.get("source_dates", [])}
        )
        rules = []
        rule_specs = [
            ("MISSING_REQUIRED_FIELD", "Rows with null required identity/time fields"),
            ("NON_TRADING_DATE", "Rows whose trade_date is absent from the frozen calendar"),
            ("EVENT_DATE_MISMATCH", "Local event date differs from trade_date"),
            ("UNIX_TIME_MISMATCH", "UNIX milliseconds disagree with localized event_time"),
            ("SESSION_UNCLASSIFIED", "Rows outside evidenced intraday segments"),
            ("INVALID_L1_BOOK", "Nonpositive/crossed L1 prices or negative/null L1 sizes"),
            ("DUPLICATE_EVENT_KEY_IDENTICAL", "Identical business payloads at a repeated event key"),
            ("DUPLICATE_EVENT_KEY_CONFLICTING", "Conflicting business payloads at a repeated event key"),
            ("DUPLICATE_SOURCE_RECORD", "Repeated upstream provenance key"),
            ("DEPTH_BOOK_INVALID", "At least one deeper book level is missing or invalid"),
        ]
        for rule_id, description in rule_specs:
            hits = int(
                quality_flag_hits[rule_id]
                if rule_id == "DEPTH_BOOK_INVALID"
                else quality_hits[rule_id]
            )
            rules.append({
                "rule_id": rule_id,
                "scope": "accepted_quote_row" if rule_id == "DEPTH_BOOK_INVALID" else "input_row",
                "outcome": "WARN" if hits else "PASS",
                "hit_count": hits,
                "denominator": quote_rows if rule_id == "DEPTH_BOOK_INVALID" else raw_rows,
                "threshold": None,
                "example_source_refs": rule_examples.get(rule_id, []),
                "description": (
                    "Retained accepted quotes with invalid deeper levels; row remains available with a depth-disabled flag."
                    if rule_id == "DEPTH_BOOK_INVALID" else description
                ),
            })
        rules.extend([
            {"rule_id": "PHYSICAL_ORDER_AUDIT", "scope": "source/instrument/trade_date",
             "outcome": "WARN" if source_order_reorders else "PASS", "hit_count": source_order_reorders,
             "denominator": raw_rows, "threshold": None,
             "example_source_refs": rule_examples.get("PHYSICAL_ORDER_AUDIT", []),
             "description": "Physical input order checked before normalized event-time sorting."},
            {"rule_id": "CUMULATIVE_INCREMENT_RECONCILIATION", "scope": "source/instrument/trade_date",
             "outcome": "WARN" if increment_mismatches or cumulative_resets else "PASS",
             "hit_count": increment_mismatches + cumulative_resets, "denominator": raw_rows,
             "threshold": None, "increment_mismatch_rows": increment_mismatches,
             "volume_increment_mismatch_rows": volume_increment_mismatches,
             "turnover_increment_mismatch_rows": turnover_increment_mismatches,
             "cumulative_reset_rows": cumulative_resets,
             "volume_cumulative_reset_rows": volume_cumulative_resets,
             "turnover_cumulative_reset_rows": turnover_cumulative_resets,
             "example_source_refs": rule_examples.get("CUMULATIVE_INCREMENT_RECONCILIATION", []),
             "description": "Native increments compared with cumulative changes without lunch resets; values are never corrected."},
            {"rule_id": "TIME_GAP_QUANTILES", "scope": "raw_file/instrument/trade_date/session",
             "outcome": "PASS" if gap_interval_count else "NOT_EVALUATED",
             "hit_count": gap_interval_count if gap_interval_count else None,
             "denominator": gap_interval_count if gap_interval_count else None,
             "threshold": None,
             "formula": "positive adjacent deltas between distinct valid event_time values within a single evidenced session",
             "summary_count": len(event_time_gap_summaries),
             "example_source_refs": rule_examples.get("TIME_GAP_QUANTILES", []),
             "reason": None if gap_interval_count else "No session scope contained two unique valid event times."},
            {"rule_id": "STALE_QUOTE_AGE", "scope": "raw_file/instrument/trade_date/session",
             "outcome": "NOT_EVALUATED", "hit_count": None, "denominator": None,
             "threshold": None,
             "example_source_refs": [],
             "reason": "Source available_time is absent; update cadence/change-driven semantics and a stale threshold are unverified. Event-time observation gaps are only a proxy."},
            {"rule_id": "AVAILABLE_TIME", "scope": "dataset", "outcome": "NOT_APPLICABLE",
             "hit_count": 0, "denominator": raw_rows, "threshold": None,
             "example_source_refs": [],
             "reason": "No per-record source availability timestamp exists; available_time is null."},
        ])

        instruments = sorted(set(inventory.get("instrument_ids", [])))
        products = sorted(set(inventory.get("products", [])))
        instrument_metadata = [
            {"exchange": "CFFEX", "instrument_id": instrument, "product": next(
                (name for name in ("TF", "TS", "TL", "T") if instrument.startswith(name)), None
            ), "tick_size": None, "contract_multiplier": None, "price_unit": None,
             "turnover_unit": None, "source": None,
             "unknown_reason": "No verified instrument metadata provider is configured."}
            for instrument in instruments
        ]
        warnings = [
            {"code": "UNKNOWN_AVAILABILITY", "message": "Per-record availability time is not present in the source."},
            {"code": "UNKNOWN_CONTRACT_METADATA", "message": "Tick size, multiplier, and units remain unverified."},
        ]
        if quarantine_rows:
            warnings.append({"code": "ROWS_QUARANTINED", "message": f"{quarantine_rows} input rows were quarantined with reason codes."})
        if inventory.get("source_date_range", {}).get("start"):
            start_date = date.fromisoformat(inventory["source_date_range"]["start"])
            end_date = date.fromisoformat(inventory["source_date_range"]["end"])
        else:  # pragma: no cover - preflight rejects empty date ranges
            start_date, end_date = min(source_dates), max(source_dates)
        input_config = ctx.config.get("input", {})
        schema_ref = "docs/references/gov_bond_l2.schema.json"
        schema_path = Path(input_config["source_schema_path"]).resolve()
        try:
            schema_ref = schema_path.relative_to(Path.cwd().resolve()).as_posix()
        except ValueError:
            schema_ref = schema_path.as_uri()
        calendar_provenance = dict(calendar_snapshot.provenance)
        calendar_input = {
            "provider": calendar_provenance.get("provider"),
            "calendar_id": calendar_id,
            "release": calendar_provenance.get("db_release_id", calendar_provenance.get("release")),
            "query": calendar_provenance.get("query"),
            "params": calendar_provenance.get("params"),
            "provenance": calendar_provenance,
        }
        for source_key, target_key in (
            ("catalog_pointer_uri", "catalog_pointer_uri"),
            ("database_uri", "database_uri"),
            ("database_manifest_uri", "db_manifest_uri"),
            ("selected_file_uri", "selected_file_uri"),
            ("selected_file_sha256", "selected_file_sha256"),
        ):
            if calendar_provenance.get(source_key):
                calendar_input[target_key] = calendar_provenance[source_key]
        if calendar_provenance.get("snapshot_uri"):
            calendar_input["snapshot_uri"] = calendar_provenance["snapshot_uri"]
            calendar_input["snapshot_sha256"] = calendar_provenance["snapshot_sha256"]
            calendar_input["release"] = "frozen_snapshot:" + bare_sha256(calendar_provenance["snapshot_sha256"])
        if calendar_provenance.get("base_url"):
            calendar_input["base_url"] = calendar_provenance["base_url"]
        session_evidence_uri = session_evidence_path.as_uri()
        input_raw_files = []
        for item in raw_files:
            input_raw_files.append({key: value for key, value in item.items() if key in {
                "uri", "sha256", "size_bytes", "row_count", "source_format", "row_group_count",
                "row_group_offsets", "row_group_row_counts", "trade_date_min", "trade_date_max",
                "trade_date_count",
            }})
        entry = {
            "schema_version": ENTRY_VERSION,
            "stage_name": self.name,
            "status": "READY_WITH_WARNINGS" if warnings or quarantine_rows else "READY",
            "run_id": ctx.run_id,
            "stage_run_id": transaction.stage_run_id,
            "config_hash": ctx.config_hash,
            "code_revision": ctx.code_revision,
            "split_id": ctx.split_id,
            "data_snapshot_id": ctx.data_snapshot_id,
            "inputs": {
                "source_snapshot": transaction.final_ref(input_refs["source_snapshot"]).to_dict(),
                "split_manifest": transaction.final_ref(input_refs["split_manifest"]).to_dict(),
                "raw_files": input_raw_files,
                "schema_ref": schema_ref,
                "schema_sha256": source.sha256,
                "mapping": dict(L2_FIELD_MAPPING),
                "source_timezone": timezone_name,
                "calendar": calendar_input,
                "session_evidence_ref": {"uri": session_evidence_uri,
                                         "sha256": sha256_file(session_evidence_path)},
            },
            "passport": {
                "asset_class": "government_bond_futures",
                "exchange": "CFFEX",
                "calendar_id": calendar_id,
                "products": products,
                "instruments": instruments,
                "date_range": {"start": start_date.isoformat(), "end": end_date.isoformat()},
                "partition_count": len(partitions),
                "row_count": quote_rows,
                "availability_status": "unverified",
                "research_only": True,
                "instrument_metadata": instrument_metadata,
                "field_semantics_ref": schema_ref,
                "unconfirmed_items": ["available_time", "tick_size", "contract_multiplier", "price_unit", "turnover_unit"],
                "input_constraints": {
                    "available_time": "unavailable; all values null; research_only",
                    "volume_increment": "native TRADEVOLUME preserved; consistency checked; do not difference again",
                    "turnover_increment": "native TRADEAMOUNT preserved; consistency checked; do not difference again",
                    "volume_bars": "disabled if cumulative/increment reconciliation reports mismatches",
                    "depth_features": "disabled per row when DEPTH_BOOK_INVALID is present",
                    "spread_ticks": "disabled until tick_size is independently verified",
                    "availability_sensitive_execution": "disabled while availability_status is unverified",
                },
            },
            "quality_report": {
                "input_rows": raw_rows,
                "accepted_rows": quote_rows,
                "quarantined_rows": quarantine_rows,
                "quarantine_rate": quarantine_rate,
                "duplicate_rate": _rate_metric(
                    sum(int(item["duplicate_event_extra_rows"]) for item in quality_scopes),
                    raw_rows,
                    "sum(event_key_group_size - 1) / input row_count",
                ),
                "source_date_range": {"start": start_date.isoformat(), "end": end_date.isoformat()},
                "source_trade_dates": inventory.get("source_dates", []),
                "expected_missing_dates": expected_missing_dates,
                "calendar_dates_without_source_rows": expected_missing_dates,
                "calendar_row_count": calendar_table.num_rows,
                "source_order_reorder_rows": source_order_reorders,
                "depth_invalid_source_rows": sum(int(item["depth_invalid_source_rows"]) for item in quality_scopes),
                "depth_invalid_accepted_rows": quality_flag_hits["DEPTH_BOOK_INVALID"],
                "scopes": quality_scopes,
                "session_scopes": session_scopes,
                "event_time_gap_quantiles": event_time_gap_summaries,
                "time_quality_semantics": {
                    "event_time_gap": "Observation-time interval proxy computed between distinct valid event times inside one evidenced session.",
                    "quote_age_or_staleness": "Not established; source available_time and feed update semantics are unavailable or unverified.",
                },
                "volume_turnover_increment_mismatch_rows": increment_mismatches,
                "volume_increment_mismatch_rows": volume_increment_mismatches,
                "turnover_increment_mismatch_rows": turnover_increment_mismatches,
                "cumulative_reset_rows": cumulative_resets,
                "volume_cumulative_reset_rows": volume_cumulative_resets,
                "turnover_cumulative_reset_rows": turnover_cumulative_resets,
                "rules": rules,
                "quarantine_reason_counts": dict(sorted(quality_hits.items())),
                "quality_flag_counts": dict(sorted(quality_flag_hits.items())),
                "duplicate_classification": {
                    "event_key": ["exchange", "instrument_id", "trade_date", "event_time"],
                    "policy": "quarantine_entire_group_without_sequence",
                    "identical_payload_groups": identical_event_groups,
                    "identical_payload_rows": identical_event_rows,
                    "identical_payload_extra_rows": identical_event_extra_rows,
                    "conflicting_payload_groups": conflicting_event_groups,
                    "conflicting_payload_rows": conflicting_event_rows,
                    "conflicting_payload_extra_rows": conflicting_event_extra_rows,
                    "source_record_duplicate_rows": int(quality_hits["DUPLICATE_SOURCE_RECORD"]),
                },
                "daily_zero_policy": "A calendar date without source rows is reported as a source gap; no zero rows are synthesized.",
            },
            "partitions": partitions,
            "artifacts": {
                "quotes": [final_refs[alias].to_dict() for alias in output_refs
                           if alias.startswith("quote_")],
                "quarantine": [final_refs[alias].to_dict() for alias in output_refs
                               if alias.startswith("quarantine_")],
                "calendar": final_refs["calendar"].to_dict(),
                "session_rules": final_refs["session_rules"].to_dict(),
            },
            "artifact_manifests": embedded_manifests,
            "warnings": warnings,
        }
        onboarding_alias = "onboarding"
        entry_path = transaction.reserve_output(
            onboarding_alias,
            artifact_type="onboarding_entry",
            schema_version=ENTRY_VERSION,
            relative_path=f"onboard/{ctx.data_snapshot_id}/{ctx.config_hash.removeprefix('sha256:')}/onboarding.json",
            row_count=quote_rows,
            quality_flags=["RESEARCH_ONLY", "AVAILABILITY_UNVERIFIED"],
        )
        entry_path.write_text(canonical_json(entry) + "\n", encoding="utf-8")
        output_refs[onboarding_alias] = transaction.register_output(onboarding_alias)
        return StageResult(
            artifacts=output_refs,
            metrics={
                "input_rows": raw_rows,
                "accepted_rows": quote_rows,
                "quarantined_rows": quarantine_rows,
                "quote_partitions": len(partitions),
                "calendar_rows": calendar_table.num_rows,
            },
            warnings=tuple(item["code"] for item in warnings),
        )
