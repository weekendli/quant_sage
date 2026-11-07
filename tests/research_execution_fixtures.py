"""Serialized numeric source fixtures for 01-E execution and recovery tests."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from quantsage_discovery.core import Registry
from quantsage_discovery.research.service import ResearchService
from tests.research_fixtures import refresh_source_fixture_refs
from tests.research_planning_fixtures import (
    FEATURE_SET_REF,
    INSTRUMENT,
    make_plan_request,
    make_planning_feature_set_document,
    make_planning_source_fixture,
    make_planning_study_document,
    register_planning_catalog,
    split_ref_from_fixture,
)

EXCHANGE = "SYNTH"
TRADE_DATE = "2024-06-04"
SESSION_ID = "night"
RUN_START = "2024-06-03T13:10:00+00:00"
RUN_END = "2024-06-03T13:11:00+00:00"


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _file_ref(path: Path) -> dict[str, str]:
    payload = path.read_bytes()
    return {
        "uri": path.resolve().as_uri(),
        "content_hash": "sha256:" + hashlib.sha256(payload).hexdigest(),
    }


def make_numeric_source_fixture(directory: Path, rows: list[dict[str, Any]]):
    """Build a validated DataSource with actual, hashed Parquet values.

    The file includes rows outside the requested interval so executor tests can
    prove that the frozen plan filters authorized output scope.
    """
    fixture = make_planning_source_fixture(directory)
    table = pa.table({
        "event_time": pa.array(
            [datetime.fromisoformat(row["event_time"]) for row in rows],
            type=pa.timestamp("us", tz="UTC"),
        ),
        "instrument_id": pa.array([row.get("instrument_id", INSTRUMENT) for row in rows],
                                   type=pa.string()),
        "trade_date": pa.array([row.get("trade_date", TRADE_DATE) for row in rows],
                                type=pa.string()),
        "session_id": pa.array([row.get("session_id", SESSION_ID) for row in rows],
                                type=pa.string()),
        "value": pa.array([row.get("value") for row in rows], type=pa.float64()),
    })
    pq.write_table(table, fixture.data_path)

    schema_document = {
        "schema_version": "source_schema_v1",
        "fields": {
            "event_time": "timestamp[us, tz=UTC]",
            "instrument_id": "string",
            "trade_date": "string",
            "session_id": "string",
            "value": "float64",
        },
    }
    fixture.paths["schema"].write_bytes(_canonical_json(schema_document) + b"\n")
    refresh_source_fixture_refs(fixture, refresh_split=True)

    # Bind the source manifest to the newly serialized Parquet file, not the
    # small placeholder file that the general planning fixture starts with.
    data_ref = _file_ref(fixture.data_path)
    manifest_path = fixture.paths["manifest"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["data_artifacts"] = [data_ref]
    content_hash = "sha256:" + hashlib.sha256(_canonical_json([data_ref])).hexdigest()
    manifest["content_hash"] = content_hash
    manifest_path.write_bytes(_canonical_json(manifest) + b"\n")
    fixture.document["content_hash"] = content_hash
    fixture.document["manifest_ref"] = _file_ref(manifest_path)
    fixture.document["format"] = "parquet"
    return fixture


def make_execution_environment(
    tmp_path: Path,
    *,
    rows: list[dict[str, Any]] | None = None,
    feature_set_document: dict[str, Any] | None = None,
):
    tmp_path.mkdir(parents=True, exist_ok=True)
    registry = Registry(tmp_path / "research.duckdb")
    service = ResearchService(registry)
    source = make_numeric_source_fixture(
        tmp_path / "source",
        rows if rows is not None else [
            {"event_time": "2024-06-03T13:10:10+00:00", "value": 2.0},
            {"event_time": "2024-06-03T13:10:20+00:00", "value": 3.0},
            {"event_time": "2024-06-03T13:12:10+00:00", "value": 100.0},
        ],
    )
    service.register_source(source.document)
    register_planning_catalog(service)
    document = feature_set_document or make_planning_feature_set_document()
    draft = service.save_feature_set(document)
    frozen = service.freeze_feature_set(
        f"{document['feature_set_id']}@{document['version']}",
        expected_revision=draft["draft_revision"],
    )
    study_document = make_planning_study_document(
        feature_set_ref=FEATURE_SET_REF,
    )
    service.create_study(study_document)
    split_ref = split_ref_from_fixture(source)
    request = make_plan_request(
        split_ref,
        selected_item_ids=["quote"],
        include_selection=True,
        start=RUN_START,
        end=RUN_END,
    )
    return registry, service, source, frozen, request


__all__ = [
    "EXCHANGE",
    "INSTRUMENT",
    "RUN_END",
    "RUN_START",
    "SESSION_ID",
    "TRADE_DATE",
    "make_execution_environment",
    "make_numeric_source_fixture",
]
