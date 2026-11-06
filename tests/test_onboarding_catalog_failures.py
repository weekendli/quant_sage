"""Serialized local catalog and session-evidence failure fixtures for Step 02."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quantsage_discovery.ingest.calendar import read_local_calendar, sha256_file
from quantsage_discovery.stages.onboard import _build_session_intervals

START = date(2026, 1, 5)
END = date(2026, 1, 6)
RELEASE = "fixture-db-release"
DATA_RELEASE = "fixture-data-release"


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _make_catalog(
    root: Path,
    *,
    calendar_id: str = "CN_STOCK",
    dates: list[date] | None = None,
    pointer_release: str = RELEASE,
    write_pointer: bool = True,
) -> tuple[Path, Path, Path]:
    """Create an actual DuckDB database, view, Parquet dependency and manifests."""
    root.mkdir(parents=True, exist_ok=True)
    selected_relative = Path("canonical/reference/calendars/selected.parquet")
    selected_path = root / selected_relative
    selected_path.parent.mkdir(parents=True, exist_ok=True)
    selected_dates = dates if dates is not None else [START, END]
    pq.write_table(pa.table({
        "calendar_id": pa.array([calendar_id] * len(selected_dates), type=pa.string()),
        "trade_date": pa.array(selected_dates, type=pa.date32()),
    }), selected_path)

    database_relative = Path("catalog/duckdb/releases/fixture.duckdb")
    manifest_relative = Path("catalog/duckdb/releases/fixture-manifest.json")
    database_path = root / database_relative
    database_path.parent.mkdir(parents=True, exist_ok=True)
    selected_sql_path = selected_path.as_posix().replace("'", "''")
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("CREATE SCHEMA reference")
        connection.execute(
            "CREATE VIEW reference.calendars AS "
            f"SELECT * FROM read_parquet('{selected_sql_path}')"
        )

    manifest = {
        "db_release_id": RELEASE,
        "database_sha256": sha256_file(database_path),
        "external_files": [{
            "path": selected_relative.as_posix(),
            "dataset_id": "reference.calendars",
            "sha256": sha256_file(selected_path),
        }],
    }
    manifest_path = root / manifest_relative
    _write_json(manifest_path, manifest)
    pointer_path = root / "catalog/duckdb/current.json"
    if write_pointer:
        _write_json(pointer_path, {
            "database": database_relative.as_posix(),
            "database_manifest": manifest_relative.as_posix(),
            "db_release_id": pointer_release,
            "data_release_id": DATA_RELEASE,
        })

    evidence_path = root / "cffex_evidence.json"
    _write_json(evidence_path, {
        "scope": {"trade_date_start": START.isoformat(), "trade_date_end": END.isoformat()},
        "annual_notices": [],
        "calendar_comparison": {
            "candidate_calendar_id": "CN_STOCK",
            "status": "fixture_verified",
            "catalog": {
                "db_release_id": RELEASE,
                "data_release_id": DATA_RELEASE,
                "database": database_relative.as_posix(),
                "catalog_selected_file": selected_relative.as_posix(),
                "database_manifest_sha256": sha256_file(manifest_path),
                "calendar_file_sha256": sha256_file(selected_path),
            },
        },
    })
    return root, evidence_path, selected_path


def _read_fixture(root: Path, evidence_path: Path, *, calendar_id: str = "CN_STOCK"):
    return read_local_calendar(
        data_root=root,
        evidence_path=evidence_path,
        calendar_id=calendar_id,
        start=START,
        end=END,
    )


def test_serialized_catalog_fixture_reads_selected_file_and_pins_manifests(tmp_path: Path) -> None:
    root, evidence, selected_path = _make_catalog(tmp_path / "catalog")

    result = _read_fixture(root, evidence)

    assert result.table.column("trade_date").to_pylist() == [START, END]
    assert result.provenance["selected_file_sha256"] == sha256_file(selected_path)
    assert result.provenance["db_release_id"] == RELEASE


def test_missing_local_catalog_pointer_is_a_hard_failure(tmp_path: Path) -> None:
    root, evidence, _ = _make_catalog(tmp_path / "catalog", write_pointer=False)

    with pytest.raises(FileNotFoundError):
        _read_fixture(root, evidence)


def test_stale_local_catalog_release_pointer_is_rejected(tmp_path: Path) -> None:
    root, evidence, _ = _make_catalog(tmp_path / "catalog", pointer_release="stale-release")

    with pytest.raises(ValueError, match="release IDs differ"):
        _read_fixture(root, evidence)


def test_broken_selected_parquet_dependency_is_a_hard_failure(tmp_path: Path) -> None:
    root, evidence, selected_path = _make_catalog(tmp_path / "catalog")
    selected_path.unlink()

    with pytest.raises(ValueError, match="calendar Parquet file is missing"):
        _read_fixture(root, evidence)


def test_duplicate_selected_calendar_keys_are_rejected(tmp_path: Path) -> None:
    root, evidence, _ = _make_catalog(tmp_path / "catalog", dates=[START, START, END])

    with pytest.raises(ValueError, match="date keys contain duplicates"):
        _read_fixture(root, evidence)


def test_unapproved_calendar_id_is_rejected_even_when_present(tmp_path: Path) -> None:
    root, evidence, _ = _make_catalog(tmp_path / "catalog", calendar_id="OTHER")

    with pytest.raises(ValueError, match="unapproved calendar_id"):
        _read_fixture(root, evidence, calendar_id="OTHER")


def test_selected_calendar_must_cover_the_complete_source_date_range(tmp_path: Path) -> None:
    root, evidence, _ = _make_catalog(tmp_path / "catalog", dates=[START])

    with pytest.raises(ValueError, match="calendar coverage mismatch"):
        _read_fixture(root, evidence)


def test_missing_serialized_session_evidence_blocks_session_interval_build(tmp_path: Path) -> None:
    evidence = tmp_path / "session-evidence.json"
    _write_json(evidence, {"evidence_version": "fixture-without-session-rules"})

    with pytest.raises(KeyError, match="session_rules"):
        _build_session_intervals(evidence)
