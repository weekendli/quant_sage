"""CFFEX trading-date evidence, Marketdata catalog access, and API parsing."""

from __future__ import annotations

import hashlib
import json
import re
import ssl
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def bare_sha256(value: str) -> str:
    return value.removeprefix("sha256:")


def _as_date(value: Any) -> date:
    if isinstance(value, date) and not hasattr(value, "hour"):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise ValueError(f"calendar date must be an ISO date or date object, got {value!r}")


def _closures(evidence: dict[str, Any]) -> list[tuple[date, date]]:
    ranges: list[tuple[date, date]] = []
    for notice in evidence.get("annual_notices", []):
        for span in notice.get("closure_ranges_inclusive", []):
            ranges.append((_as_date(span["start"]), _as_date(span["end"])))
    return ranges


def expected_cffex_dates(evidence_path: str | Path, start: date, end: date) -> list[date]:
    """Compute weekday membership from the checked holiday closure evidence."""
    evidence = json.loads(Path(evidence_path).read_text(encoding="utf-8"))
    scope = evidence.get("scope", {})
    scope_start = _as_date(scope.get("trade_date_start"))
    scope_end = _as_date(scope.get("trade_date_end"))
    if start < scope_start or end > scope_end or start > end:
        raise ValueError(
            f"requested calendar range {start}..{end} falls outside verified evidence "
            f"{scope_start}..{scope_end}"
        )
    closed_ranges = _closures(evidence)
    dates: list[date] = []
    cursor = start
    from datetime import timedelta

    while cursor <= end:
        is_weekday = cursor.weekday() < 5
        holiday_closed = any(first <= cursor <= last for first, last in closed_ranges)
        if is_weekday and not holiday_closed:
            dates.append(cursor)
        cursor += timedelta(days=1)
    return dates


def validate_calendar_dates(
    actual_calendar_id: str,
    actual_dates: list[date] | tuple[date, ...],
    approved_calendar_id: str,
    expected_dates: list[date] | tuple[date, ...],
) -> bool:
    if actual_calendar_id != approved_calendar_id:
        raise ValueError(
            f"unapproved calendar_id {actual_calendar_id!r}; expected {approved_calendar_id!r}"
        )
    normalized = [_as_date(value) for value in actual_dates]
    if len(normalized) != len(set(normalized)):
        raise ValueError("calendar date keys contain duplicates")
    expected = set(expected_dates)
    actual = set(normalized)
    if actual != expected:
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        raise ValueError(
            f"calendar coverage mismatch; missing={missing[:5]}, unexpected={unexpected[:5]}"
        )
    return True


def parse_calendar_json_response(
    response: dict[str, Any], *, expected_release: str
) -> list[tuple[str, date]]:
    """Validate and parse positional API rows without relying on row-object order."""
    if response.get("error"):
        raise ValueError(f"calendar API error: {response['error']}")
    if response.get("truncated") is not False:
        raise ValueError("calendar API response is truncated or has no truncation status")
    if response.get("release") != expected_release:
        raise ValueError(
            f"calendar API release mismatch: expected {expected_release!r}, "
            f"got {response.get('release')!r}"
        )
    columns = response.get("columns")
    if not isinstance(columns, list):
        raise TypeError("calendar API response columns must be a list")
    names = [column.get("name") for column in columns]
    types = [column.get("type") for column in columns]
    if names != ["calendar_id", "trade_date"] or types != ["string", "date32[day]"]:
        raise ValueError(
            "calendar API response columns must be calendar_id:string, trade_date:date32[day]"
        )
    rows = response.get("rows")
    if not isinstance(rows, list):
        raise TypeError("calendar API response rows must be a list of positional arrays")
    row_count = response.get("row_count")
    if row_count is not None and row_count != len(rows):
        raise ValueError("calendar API row_count does not match returned rows")
    parsed: list[tuple[str, date]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, list) or len(row) != 2:
            raise ValueError(f"calendar API row {index} does not match its column count")
        if not isinstance(row[0], str) or not row[0]:
            raise ValueError(f"calendar API row {index} has an invalid calendar_id")
        parsed.append((row[0], _as_date(row[1])))
    keys = [(calendar_id, trade_date) for calendar_id, trade_date in parsed]
    if len(keys) != len(set(keys)):
        raise ValueError("calendar API response contains duplicate calendar keys")
    return parsed


@dataclass(frozen=True)
class CalendarSnapshot:
    table: pa.Table
    provenance: dict[str, Any]


def _resolve_catalog_file_path(view_sql: str) -> str:
    match = re.search(r"read_parquet\([^']*'([^']+\.parquet)'", view_sql, flags=re.IGNORECASE)
    if match is None:
        raise ValueError("could not resolve the catalog-selected reference.calendars Parquet file")
    return match.group(1)


def read_local_calendar(
    *,
    data_root: str | Path,
    evidence_path: str | Path,
    calendar_id: str,
    start: date,
    end: date,
) -> CalendarSnapshot:
    root = Path(data_root).expanduser().resolve()
    pointer_path = root / "catalog/duckdb/current.json"
    pointer_bytes = pointer_path.read_bytes()
    pointer = json.loads(pointer_bytes)
    if not isinstance(pointer.get("database"), str) or not isinstance(
        pointer.get("database_manifest"), str
    ):
        raise TypeError("active DuckDB pointer database and database_manifest must be strings")
    database_path = (root / pointer["database"]).resolve()
    manifest_path = (root / pointer["database_manifest"]).resolve()
    for path in (database_path, manifest_path):
        if not path.is_file() or root not in path.parents:
            raise ValueError(f"catalog pointer resolves outside data root or to a missing file: {path}")
    database_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if database_manifest.get("db_release_id") != pointer.get("db_release_id"):
        raise ValueError("catalog pointer and database manifest release IDs differ")
    if bare_sha256(str(database_manifest.get("database_sha256", ""))) != bare_sha256(
        sha256_file(database_path)
    ):
        raise ValueError("active DuckDB file hash differs from its database manifest")

    with duckdb.connect(str(database_path), read_only=True) as connection:
        view = connection.execute(
            "SELECT sql FROM duckdb_views() WHERE schema_name = 'reference' AND view_name = 'calendars'"
        ).fetchone()
        if view is None:
            raise ValueError("active catalog has no reference.calendars view")
        selected_path = Path(_resolve_catalog_file_path(view[0])).resolve()
        if not selected_path.is_file():
            raise ValueError(f"catalog-selected calendar Parquet file is missing: {selected_path}")
        external_files = database_manifest.get("external_files", [])
        relative = selected_path.relative_to(root).as_posix()
        selected_meta = next(
            (item for item in external_files if item.get("path") == relative), None
        )
        if selected_meta is None or selected_meta.get("dataset_id") != "reference.calendars":
            raise ValueError("selected calendar file is not registered by database_manifest")
        selected_hash = sha256_file(selected_path)
        if bare_sha256(str(selected_meta.get("sha256", ""))) != bare_sha256(selected_hash):
            raise ValueError("catalog-selected calendar file hash differs from database_manifest")
        if calendar_id not in {
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT calendar_id FROM reference.calendars ORDER BY calendar_id"
            ).fetchall()
        }:
            raise ValueError(f"calendar_id {calendar_id!r} is absent from active catalog")
        rows = connection.execute(
            "SELECT calendar_id, trade_date FROM reference.calendars "
            "WHERE calendar_id = ? AND trade_date >= ? AND trade_date <= ? "
            "ORDER BY calendar_id, trade_date",
            [calendar_id, start, end],
        ).fetchall()
        source = connection.execute(
            "SELECT DISTINCT calendar_id FROM reference.calendars ORDER BY calendar_id"
        ).fetchall()

    evidence = json.loads(Path(evidence_path).read_text(encoding="utf-8"))
    approved = evidence["calendar_comparison"]["candidate_calendar_id"]
    expected_catalog = evidence["calendar_comparison"]["catalog"]
    if pointer.get("db_release_id") != expected_catalog.get("db_release_id"):
        raise ValueError("active database release differs from the calendar evidence release")
    if pointer.get("data_release_id") != expected_catalog.get("data_release_id"):
        raise ValueError("active data release differs from the calendar evidence release")
    if database_path.relative_to(root).as_posix() != expected_catalog.get("database"):
        raise ValueError("active database path differs from the pinned calendar evidence")
    if relative != expected_catalog.get("catalog_selected_file"):
        raise ValueError("catalog-selected calendar file differs from the pinned evidence file")
    if bare_sha256(sha256_file(manifest_path)) != bare_sha256(
        str(expected_catalog.get("database_manifest_sha256", ""))
    ):
        raise ValueError("active database manifest hash differs from the calendar evidence")
    if bare_sha256(selected_hash) != bare_sha256(
        str(expected_catalog.get("calendar_file_sha256", ""))
    ):
        raise ValueError("catalog-selected calendar hash differs from the calendar evidence")
    expected_dates = expected_cffex_dates(evidence_path, start, end)
    actual_ids = {row[0] for row in rows}
    if actual_ids != {approved} or calendar_id != approved:
        raise ValueError(f"unapproved calendar_id {calendar_id!r}; evidence approves {approved!r}")
    actual_dates = [row[1] for row in rows]
    validate_calendar_dates(calendar_id, actual_dates, approved, expected_dates)
    current_pointer_after = pointer_path.read_bytes()
    if current_pointer_after != pointer_bytes:
        raise ValueError("active catalog pointer changed while freezing the calendar")
    if sha256_file(selected_path) != selected_hash:
        raise ValueError("catalog-selected calendar file changed while it was being frozen")
    table = pa.table(
        {
            "calendar_id": pa.array([row[0] for row in rows], type=pa.string()),
            "trade_date": pa.array([row[1] for row in rows], type=pa.date32()),
        }
    )
    provenance = {
        "provider": "local_duckdb",
        "catalog_pointer_uri": pointer_path.as_uri(),
        "catalog_pointer_sha256": "sha256:" + hashlib.sha256(pointer_bytes).hexdigest(),
        "db_release_id": pointer["db_release_id"],
        "data_release_id": pointer["data_release_id"],
        "database_uri": database_path.as_uri(),
        "database_sha256": database_manifest["database_sha256"],
        "database_manifest_uri": manifest_path.as_uri(),
        "database_manifest_sha256": sha256_file(manifest_path),
        "selected_file_uri": selected_path.as_uri(),
        "selected_file_sha256": selected_hash,
        "calendar_id": calendar_id,
        "query": (
            "SELECT calendar_id, trade_date FROM reference.calendars "
            "WHERE calendar_id = ? AND trade_date >= ? AND trade_date <= ? "
            "ORDER BY calendar_id, trade_date"
        ),
        "params": [calendar_id, start.isoformat(), end.isoformat()],
        "start": start.isoformat(),
        "end": end.isoformat(),
        "row_count": len(rows),
        "discovered_calendar_ids": [row[0] for row in source],
        "evidence_uri": Path(evidence_path).resolve().as_uri(),
        "evidence_sha256": "sha256:" + hashlib.sha256(Path(evidence_path).read_bytes()).hexdigest(),
        "calendar_evidence_status": evidence["calendar_comparison"]["status"],
    }
    return CalendarSnapshot(table=table, provenance=provenance)


def read_frozen_calendar(
    *,
    snapshot_path: str | Path,
    expected_sha256: str,
    evidence_path: str | Path,
    calendar_id: str,
    start: date,
    end: date,
) -> CalendarSnapshot:
    """Use a previously frozen, hash-pinned calendar after its provider is unavailable."""
    path = Path(snapshot_path).expanduser().resolve()
    if not path.is_file():
        raise ValueError(f"frozen calendar snapshot is missing: {path}")
    actual_hash = sha256_file(path)
    if bare_sha256(actual_hash) != bare_sha256(expected_sha256):
        raise ValueError("frozen calendar snapshot hash mismatch")
    table = pq.read_table(path)
    expected_schema = pa.schema(
        [pa.field("calendar_id", pa.string()), pa.field("trade_date", pa.date32())]
    )
    if table.schema.names != expected_schema.names or table.schema.types != expected_schema.types:
        raise ValueError("frozen calendar snapshot must contain calendar_id:string and trade_date:date32")
    if not set(table.column("calendar_id").to_pylist()) <= {calendar_id}:
        raise ValueError("frozen calendar snapshot contains an unexpected calendar_id")
    filter_mask = pc.and_kleene(
        pc.greater_equal(table.column("trade_date"), pa.scalar(start, pa.date32())),
        pc.less_equal(table.column("trade_date"), pa.scalar(end, pa.date32())),
    )
    subset = table.filter(filter_mask)
    rows = list(
        zip(
            subset.column("calendar_id").to_pylist(),
            subset.column("trade_date").to_pylist(),
            strict=True,
        )
    )
    evidence = json.loads(Path(evidence_path).read_text(encoding="utf-8"))
    approved = evidence["calendar_comparison"]["candidate_calendar_id"]
    if calendar_id != approved or {row[0] for row in rows} != {approved}:
        raise ValueError(f"unapproved calendar_id {calendar_id!r}; evidence approves {approved!r}")
    expected_dates = expected_cffex_dates(evidence_path, start, end)
    validate_calendar_dates(calendar_id, [row[1] for row in rows], approved, expected_dates)
    return CalendarSnapshot(
        table=subset,
        provenance={
            "provider": "verified_frozen_snapshot_fallback",
            "snapshot_uri": path.as_uri(),
            "snapshot_sha256": actual_hash,
            "calendar_id": calendar_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "row_count": subset.num_rows,
            "evidence_uri": Path(evidence_path).resolve().as_uri(),
            "evidence_sha256": sha256_file(evidence_path),
        },
    )


def fetch_api_calendar(
    *,
    base_url: str,
    ca_bundle: str | Path,
    evidence_path: str | Path,
    calendar_id: str,
    start: date,
    end: date,
) -> CalendarSnapshot:
    """Fetch a release-consistent JSON calendar through the read-only Marketdata API."""
    ssl_context = ssl.create_default_context(cafile=str(ca_bundle))
    base = base_url.rstrip("/")
    with urlopen(base + "/health", context=ssl_context, timeout=30) as response:
        health = json.loads(response.read())
    release = health.get("db_release_id", health.get("release"))
    if not isinstance(release, str) or not release:
        raise ValueError("calendar API health response is missing a release ID")
    sql = (
        "SELECT calendar_id, trade_date FROM marketdata.reference.calendars "
        "WHERE calendar_id = ? AND trade_date >= ? AND trade_date <= ? "
        "ORDER BY calendar_id, trade_date"
    )
    body = json.dumps(
        {
            "sql": sql,
            "params": [calendar_id, start.isoformat(), end.isoformat()],
            "max_rows": 100000,
            "format": "json",
        }
    ).encode("utf-8")
    request = Request(
        base + "/query", data=body, headers={"Content-Type": "application/json"}
    )
    with urlopen(request, context=ssl_context, timeout=45) as response:
        result = json.loads(response.read())
    rows = parse_calendar_json_response(result, expected_release=release)
    evidence = json.loads(Path(evidence_path).read_text(encoding="utf-8"))
    approved = evidence["calendar_comparison"]["candidate_calendar_id"]
    actual_ids = {row[0] for row in rows}
    if actual_ids != {approved} or calendar_id != approved:
        raise ValueError(f"unapproved calendar_id {calendar_id!r}; evidence approves {approved!r}")
    expected = expected_cffex_dates(evidence_path, start, end)
    validate_calendar_dates(calendar_id, [row[1] for row in rows], approved, expected)
    table = pa.table(
        {
            "calendar_id": pa.array([row[0] for row in rows], type=pa.string()),
            "trade_date": pa.array([row[1] for row in rows], type=pa.date32()),
        }
    )
    return CalendarSnapshot(
        table=table,
        provenance={
            "provider": "marketdata_api",
            "base_url": base,
            "release": release,
            "query": sql,
            "params": [calendar_id, start.isoformat(), end.isoformat()],
            "row_count": len(rows),
            "evidence_uri": Path(evidence_path).resolve().as_uri(),
            "evidence_sha256": sha256_file(evidence_path),
        },
    )
