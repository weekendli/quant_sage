"""Serialized integration coverage for the 01-B Study/DataSource API."""

from __future__ import annotations

import copy
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from quantsage_discovery.core import Registry
from quantsage_discovery.research.errors import (
    ImmutableVersionError,
    ResearchValidationError,
    RevisionConflictError,
    StudyNotActiveError,
    UnknownReferenceError,
)
from quantsage_discovery.research.service import ResearchService
from tests.research_fixtures import (
    make_source_fixture,
    make_study_document,
    market_metadata_document,
    refresh_source_fixture_refs,
)


@pytest.mark.parametrize("artifact,field,value", [
    ("schema", "fields", {"event_time": []}),
    ("split", "split_id", ["not-a-string"]),
    ("split", "schema_version", "unsupported"),
])
def test_referenced_schema_and_split_payload_types_are_validated(tmp_path, artifact, field, value):
    fixture = make_source_fixture(tmp_path)
    path = fixture.paths[artifact]
    payload = json.loads(path.read_text())
    payload[field] = value
    path.write_text(json.dumps(payload))
    refresh_source_fixture_refs(fixture, refresh_split=artifact == "split")
    with (
        Registry(tmp_path / "workspace.duckdb") as registry,
        pytest.raises(ResearchValidationError),
    ):
        ResearchService(registry).register_source(fixture.document)


def test_study_rejects_source_without_research_permission(tmp_path):
    fixture = make_source_fixture(tmp_path)
    path = fixture.paths["access_policy"]
    payload = json.loads(path.read_text())
    payload["allowed_uses"] = ["audit_only"]
    path.write_text(json.dumps(payload))
    refresh_source_fixture_refs(fixture)
    with Registry(tmp_path / "workspace.duckdb") as registry:
        service = ResearchService(registry)
        service.register_source(fixture.document)
        with pytest.raises(ResearchValidationError, match="does not permit research"):
            service.create_study(make_study_document())


def _service(path: Path):
    registry = Registry(path)
    return registry, ResearchService(registry)


def _metadata_fixture(directory: Path, *, product: str = "T", tick_size: str = "0.005",
                      valid_from: str = "2024-01-01", valid_to: str | None = None):
    return make_source_fixture(
        directory,
        include_metadata=True,
        metadata_document=market_metadata_document(
            product=product, tick_size=tick_size, valid_from=valid_from, valid_to=valid_to
        ),
    )


def test_source_registration_persists_and_reopens_exact_serialized_document(tmp_path):
    fixture = make_source_fixture(tmp_path)
    registry, service = _service(tmp_path / "workspace.duckdb")
    try:
        registered = service.register_source(fixture.document)
        assert registered["source_id"] == "synthetic_l2"
        assert registered["version"] == 1
        assert registered["content_hash"] == fixture.document["content_hash"]
        assert registered["time_rules"] == {
            "timezone": "Asia/Shanghai", "event_time_field": "event_time"
        }
        assert "available_time_field" not in registered["time_rules"]
    finally:
        registry.close()

    reopened, reopened_service = _service(tmp_path / "workspace.duckdb")
    try:
        source = reopened_service.get_source("synthetic_l2@1")
        assert source == registered
    finally:
        reopened.close()


def test_source_versions_are_immutable_and_metadata_supplementation_reuses_data(tmp_path):
    original = make_source_fixture(tmp_path, include_metadata=False)
    original_bytes = original.data_path.read_bytes()
    original_hash = original.data_ref["content_hash"]
    registry, service = _service(tmp_path / "workspace.duckdb")
    try:
        v1 = service.register_source(original.document)
        with pytest.raises(ImmutableVersionError):
            service.register_source(original.document)

        supplemented = make_source_fixture(
            tmp_path,
            version=2,
            include_metadata=True,
            shared_data_ref=original.data_ref,
            shared_data_path=original.data_path,
        )
        v2 = service.register_source(supplemented.document)

        assert v2["version"] == 2
        assert v2["content_hash"] == v1["content_hash"] == supplemented.document["content_hash"]
        assert v2["market_metadata_ref"] != v1.get("market_metadata_ref")
        assert v2["market_metadata_ref"]["content_hash"].startswith("sha256:")
        assert original.data_path.read_bytes() == original_bytes
        assert original.data_ref["content_hash"] == original_hash
        assert service.get_source("synthetic_l2@1") == v1
        assert service.get_source("synthetic_l2@2") == v2
    finally:
        registry.close()


@pytest.mark.parametrize("bad_field", ["source_unknown", "nested_unknown"])
def test_source_document_schema_rejects_unknown_fields(tmp_path, bad_field):
    fixture = make_source_fixture(tmp_path)
    document = copy.deepcopy(fixture.document)
    if bad_field == "source_unknown":
        document["unexpected"] = "not part of data_source_v1"
    else:
        document["time_rules"]["available_time"] = "invented"

    registry, service = _service(tmp_path / "workspace.duckdb")
    try:
        with pytest.raises(ResearchValidationError):
            service.register_source(document)
    finally:
        registry.close()


def test_source_rejects_malformed_content_hash_and_tampered_referenced_file(tmp_path):
    malformed = make_source_fixture(tmp_path / "malformed")
    malformed_doc = copy.deepcopy(malformed.document)
    malformed_doc["schema_ref"]["content_hash"] = "sha256:not-a-lowercase-64-digit-hash"
    registry, service = _service(tmp_path / "malformed.duckdb")
    try:
        with pytest.raises(ResearchValidationError):
            service.register_source(malformed_doc)
    finally:
        registry.close()

    tampered = make_source_fixture(tmp_path / "tampered")
    tampered.paths["schema"].write_text('{"schema_version":"source_schema_v1"}\n',
                                        encoding="utf-8")
    registry, service = _service(tmp_path / "tampered.duckdb")
    try:
        with pytest.raises(ResearchValidationError):
            service.register_source(tampered.document)
    finally:
        registry.close()


def test_source_hashes_large_data_artifacts_instead_of_trusting_the_manifest(tmp_path):
    # Exceeds the validator's former inline hash limit so large-artifact identity is covered.
    fixture = make_source_fixture(tmp_path, data_bytes=b"x" * (16 * 1024 * 1024 + 1))
    fixture.data_path.write_bytes(b"y" * (16 * 1024 * 1024 + 1))
    registry, service = _service(tmp_path / "workspace.duckdb")
    try:
        with pytest.raises(ResearchValidationError):
            service.register_source(fixture.document)
    finally:
        registry.close()


@pytest.mark.parametrize("invalid_artifact", ["schema_time_field", "calendar_timezone", "session"])
def test_source_validates_schema_time_calendar_and_session_references(tmp_path, invalid_artifact):
    fixture = make_source_fixture(tmp_path)
    if invalid_artifact == "schema_time_field":
        fixture.paths["schema"].write_text(
            '{"fields":{"last_price":"float64"},"schema_version":"source_schema_v1"}\n',
            encoding="utf-8",
        )
        refresh_source_fixture_refs(fixture)
    elif invalid_artifact == "calendar_timezone":
        fixture.paths["calendar"].write_text(
            '{"calendar_id":"wrong_tz","schema_version":"calendar_v1",'
            '"timezone":"UTC","trading_dates":["2024-06-03","2024-06-04"]}\n',
            encoding="utf-8",
        )
        refresh_source_fixture_refs(fixture)
    else:
        fixture.document["coverage"]["partitions"][0]["session_ids"] = ["unlisted_session"]

    registry, service = _service(tmp_path / f"{invalid_artifact}.duckdb")
    try:
        with pytest.raises(ResearchValidationError):
            service.register_source(fixture.document)
    finally:
        registry.close()


def test_source_validates_access_policy_and_split_references(tmp_path):
    fixture = make_source_fixture(tmp_path)
    fixture.paths["split"].write_text('{"split_id":"missing_version"}\n', encoding="utf-8")
    refresh_source_fixture_refs(fixture, refresh_split=True)
    registry, service = _service(tmp_path / "workspace.duckdb")
    try:
        with pytest.raises(ResearchValidationError):
            service.register_source(fixture.document)
    finally:
        registry.close()


@pytest.mark.parametrize("instrument", ["TF2603", "TS2603", "TL2603"])
def test_metadata_T_rule_does_not_falsely_cover_longer_product_prefixes(tmp_path, instrument):
    fixture = make_source_fixture(
        tmp_path,
        source_id=f"source_{instrument.lower()}",
        instrument_id=instrument,
        include_metadata=True,
        metadata_document=market_metadata_document(product="T"),
    )
    registry, service = _service(tmp_path / "workspace.duckdb")
    try:
        with pytest.raises(ResearchValidationError):
            service.register_source(fixture.document)
    finally:
        registry.close()


@pytest.mark.parametrize("tick_size", ["0", "-0.005", "NaN", "Infinity"])
def test_market_metadata_rejects_nonpositive_or_nonfinite_tick_size(tmp_path, tick_size):
    fixture = _metadata_fixture(tmp_path, tick_size=tick_size)
    registry, service = _service(tmp_path / "workspace.duckdb")
    try:
        with pytest.raises(ResearchValidationError):
            service.register_source(fixture.document)
    finally:
        registry.close()


def test_market_metadata_rejects_overlapping_rules_and_incomplete_date_coverage(tmp_path):
    overlap = make_source_fixture(
        tmp_path / "overlap",
        include_metadata=True,
        metadata_document={
            **market_metadata_document(),
            "tick_rules": [
                {"product": "T", "valid_from": "2024-01-01", "valid_to": "2024-06-10",
                 "tick_size": "0.005", "unit": "quoted_price_unit"},
                {"product": "T", "valid_from": "2024-06-01", "valid_to": None,
                 "tick_size": "0.005", "unit": "quoted_price_unit"},
            ],
        },
    )
    coverage_gap = _metadata_fixture(
        tmp_path / "coverage_gap", valid_from="2024-06-04", valid_to=None
    )
    for fixture in (overlap, coverage_gap):
        registry, service = _service(tmp_path / f"{fixture.document['source_id']}.duckdb")
        try:
            with pytest.raises(ResearchValidationError):
                service.register_source(fixture.document)
        finally:
            registry.close()


@pytest.mark.parametrize("bad_field", ["study_unknown", "limits_unknown"])
def test_study_document_schema_rejects_unknown_fields(tmp_path, bad_field):
    fixture = make_source_fixture(tmp_path)
    document = make_study_document()
    if bad_field == "study_unknown":
        document["not_in_study_v1"] = True
    else:
        document["limits"]["unbounded_access"] = True

    registry, service = _service(tmp_path / "workspace.duckdb")
    try:
        service.register_source(fixture.document)
        with pytest.raises(ResearchValidationError):
            service.create_study(document)
    finally:
        registry.close()


def test_study_requires_registered_sources_and_covered_scope(tmp_path):
    fixture = make_source_fixture(tmp_path)
    registry, service = _service(tmp_path / "workspace.duckdb")
    try:
        missing_source = make_study_document(source_refs=("unknown@1",))
        with pytest.raises(UnknownReferenceError):
            service.create_study(missing_source)

        service.register_source(fixture.document)
        out_of_coverage = make_study_document()
        out_of_coverage["limits"]["allowed_scope"][0]["start"] = "2024-06-05T00:00:00+08:00"
        out_of_coverage["limits"]["allowed_scope"][0]["end"] = "2024-06-06T00:00:00+08:00"
        with pytest.raises(ResearchValidationError):
            service.create_study(out_of_coverage)
    finally:
        registry.close()


def test_study_scope_can_include_a_known_internal_source_gap(tmp_path):
    fixture = make_source_fixture(
        tmp_path,
        trade_dates=("2024-06-03", "2024-06-04", "2024-06-05"),
    )
    fixture.document["coverage"]["partitions"] = [
        partition for partition in fixture.document["coverage"]["partitions"]
        if partition["trade_date"] != "2024-06-04"
    ]
    registry, service = _service(tmp_path / "workspace.duckdb")
    try:
        service.register_source(fixture.document)
        study = make_study_document()
        study["limits"]["allowed_scope"][0]["end"] = "2024-06-06T00:00:00+08:00"
        created = service.create_study(study)
        assert created["source_refs"] == ["synthetic_l2@1"]
    finally:
        registry.close()


def test_study_revision_history_and_stale_revision_conflict(tmp_path):
    fixture = make_source_fixture(tmp_path)
    registry, service = _service(tmp_path / "workspace.duckdb")
    try:
        service.register_source(fixture.document)
        created = service.create_study(make_study_document())
        assert created["revision"] == 1

        def update(objective: str):
            return service.update_study(
                "synthetic_study",
                {"objective": objective},
                expected_revision=1,
                actor={"kind": "human", "id": objective},
                reason=f"Update objective to {objective}.",
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(update, "concurrent objective A"),
                       pool.submit(update, "concurrent objective B")]
            outcomes = []
            for future in futures:
                try:
                    outcomes.append(("ok", future.result()))
                except RevisionConflictError as exc:
                    outcomes.append(("conflict", exc))

        assert [kind for kind, _ in outcomes].count("ok") == 1
        assert [kind for kind, _ in outcomes].count("conflict") == 1
        current = service.get_study("synthetic_study")
        previous = service.get_study("synthetic_study", revision=1)
        assert current["revision"] == 2
        assert previous["revision"] == 1
        assert previous["objective"] == make_study_document()["objective"]
        assert current["objective"] in {"concurrent objective A", "concurrent objective B"}
        history = service.study_history("synthetic_study")
        assert [entry["revision"] for entry in history] == [1, 2]
        assert history[1]["actor"]["kind"] == "human"
        assert history[1]["reason"].startswith("Update objective to concurrent objective ")
    finally:
        registry.close()


@pytest.mark.parametrize("status", ["paused", "closed"])
def test_paused_and_closed_studies_remain_queryable(tmp_path, status):
    fixture = make_source_fixture(tmp_path)
    registry, service = _service(tmp_path / "workspace.duckdb")
    try:
        service.register_source(fixture.document)
        study = make_study_document(status=status)
        service.create_study(study)
        assert service.get_study("synthetic_study")["status"] == status
        with pytest.raises(StudyNotActiveError):
            service.ensure_active_for_execution("synthetic_study")
    finally:
        registry.close()


def test_active_study_passes_execution_guard(tmp_path):
    fixture = make_source_fixture(tmp_path)
    registry, service = _service(tmp_path / "workspace.duckdb")
    try:
        service.register_source(fixture.document)
        created = service.create_study(make_study_document())
        assert service.ensure_active_for_execution("synthetic_study") == created
    finally:
        registry.close()
