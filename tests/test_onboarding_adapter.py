"""02-A integration tests for frozen onboarding to DataSource adaptation."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from quantsage_discovery.core.artifacts import artifact_path
from quantsage_discovery.core.registry import Registry
from quantsage_discovery.ingest.calendar import sha256_file
from quantsage_discovery.research import ResearchService
from quantsage_discovery.research.errors import ResearchValidationError
from quantsage_discovery.research.onboarding_adapter import (
    GAP_EVENT_SCHEMA,
    OnboardingAdapterError,
    OnboardingDataSourceReader,
    adapt_onboarding_entry,
)
from quantsage_discovery.research.schemas import (
    validate_gap_manifest_document,
    validate_market_metadata_document,
)
from tests.test_onboarding_stage import _run_fixture_workflow

ROOT = Path(__file__).resolve().parents[1]
RAW_FIXTURE = ROOT / "tests/fixtures/onboarding_acceptance/part-0.parquet"


def _fixture_entry(tmp_path: Path) -> tuple[Path, Path, Path]:
    summary, _ = _run_fixture_workflow(
        RAW_FIXTURE,
        tmp_path / "workflow",
        "onboarding-adapter-fixture",
    )
    assert summary.status == "SUCCEEDED"
    stage = summary.stage_results["onboard"][0]
    ref = stage["artifacts"]["onboarding"]
    entry_path = artifact_path(ref["uri"])
    entry_manifest = artifact_path(ref["manifest_uri"])
    split_path = next((tmp_path / "workflow").rglob("split_manifest.json"))
    return entry_path, entry_manifest, split_path


def _metadata() -> dict[str, Any]:
    return {
        "schema_version": "market_metadata_v1",
        "metadata_id": "fixture_cffex_metadata",
        "version": 1,
        "provided_by": {"kind": "human", "id": "fixture-user"},
        "source": "Synthetic fixture rule; not market evidence.",
        "units": {
            "price_unit": "quoted_price_unit",
            "size_unit": "contract",
            "turnover_unit": "quoted_currency",
        },
        "tick_rules": [
            {
                "product": product,
                "valid_from": "2026-01-01",
                "valid_to": None,
                "tick_size": "0.005",
                "unit": "quoted_price_unit",
            }
            for product in ("T", "TF")
        ] + [{
            "product": "T",
            "instrument_id": "T2603",
            "valid_from": "2026-01-06",
            "valid_to": None,
            "tick_size": "0.010",
            "unit": "quoted_price_unit",
        }],
    }


def _adapt(
    tmp_path: Path,
    *,
    version: int = 1,
    metadata_path: Path | None = None,
):
    entry, entry_manifest, split = _fixture_entry(tmp_path)
    return adapt_onboarding_entry(
        entry,
        entry_manifest,
        split,
        tmp_path / f"adapted-v{version}",
        source_id="fixture_cffex_l2",
        version=version,
        metadata_path=metadata_path,
    )


def test_onboarding_adapter_registers_and_reads_one_frozen_partition(tmp_path: Path) -> None:
    adapted = _adapt(tmp_path)
    source = adapted.source_document
    assert source["format"] == "l2_onboarding_source_v1"
    assert source["availability_mode"] == "research_only"
    assert source["instruments"] == ["T2603", "TF2603"]
    assert len(source["coverage"]["partitions"]) >= 3
    assert adapted.summary["gap_events"] > adapted.summary["quarantined_rows"]

    quote_partition = next(
        item for item in source["coverage"]["partitions"]
        if item["instrument_id"] == "T2603" and item["trade_date"] == "2026-01-05"
    )
    quote_ref = next(
        item["artifact_ref"] for item in json.loads(
            adapted.source_index_path.read_text(encoding="utf-8")
        )["partitions"]
        if item["instrument_id"] == "T2603" and item["trade_date"] == "2026-01-05"
    )
    quote_path = artifact_path(quote_ref["uri"])
    quote_hash = sha256_file(quote_path)
    assert quote_hash == quote_ref["content_hash"]
    assert quote_partition["session_ids"]

    registry = Registry(tmp_path / "research.duckdb")
    try:
        service = ResearchService(registry)
        registered = service.register_source(source)
        assert {key: registered[key] for key in source} == source
        assert service.get_source("fixture_cffex_l2@1") == registered
    finally:
        registry.close()

    reader = OnboardingDataSourceReader(source)
    columns = [
        "exchange", "instrument_id", "trade_date", "event_time", "session_id",
        "bid_price_1", "ask_price_1", "bid_size_1", "ask_size_1", "available_time",
    ]
    quotes = reader.read_quotes(
        "T2603", "2026-01-05", columns=columns,
    )
    assert quotes.num_rows > 0
    assert set(quotes["session_id"].to_pylist()) == {
        "CN_STOCK:2026-01-05:opening_auction_matching",
        "CN_STOCK:2026-01-05:continuous_morning",
        "CN_STOCK:2026-01-05:continuous_afternoon",
    }
    assert all(value is None for value in quotes["available_time"].to_pylist())
    assert reader.tick_size("T2603", "2026-01-05") is None
    only_prices = reader.read_quotes(
        "T2603", "2026-01-05", columns=["bid_price_1", "ask_price_1"],
    )
    assert only_prices.column_names == ["bid_price_1", "ask_price_1"]
    assert only_prices.num_rows == quotes.num_rows

    first = quotes.slice(0, 1).to_pylist()[0]
    mid = (first["bid_price_1"] + first["ask_price_1"]) / 2
    price_spread = first["ask_price_1"] - first["bid_price_1"]
    l1_imbalance = (
        (first["bid_size_1"] - first["ask_size_1"])
        / (first["bid_size_1"] + first["ask_size_1"])
    )
    assert mid == Decimal("100.125")
    assert price_spread == Decimal("0.250")
    assert l1_imbalance == Decimal("0.100")

    for session_id in set(quotes["session_id"].to_pylist()):
        session = quotes.filter(pc.equal(quotes["session_id"], session_id))
        times = session["event_time"].to_pylist()
        assert times == sorted(times)

    events = reader.gap_events(instrument_id="T2603", trade_date="2026-01-05")
    assert events.schema.equals(GAP_EVENT_SCHEMA, check_metadata=False)
    kinds = set(events["event_kind"].to_pylist())
    assert "QUARANTINED_RECORD" in kinds
    assert "SOURCE_INTERVAL_START_UNKNOWN" in kinds
    assert sha256_file(quote_path) == quote_hash

    gap_manifest = json.loads(adapted.gap_manifest_path.read_text(encoding="utf-8"))
    validate_gap_manifest_document(gap_manifest)
    assert gap_manifest["policy_version"] == "gap_policy_v1"
    assert gap_manifest["raw_interval_semantics"]["differencing"] == "forbidden"
    assert gap_manifest["raw_interval_semantics"]["warning_only"] is True


def test_metadata_supplement_creates_new_version_without_rewriting_data(tmp_path: Path) -> None:
    entry, entry_manifest, split = _fixture_entry(tmp_path)
    v1 = adapt_onboarding_entry(
        entry,
        entry_manifest,
        split,
        tmp_path / "adapted-v1",
        source_id="fixture_cffex_l2",
        version=1,
    )
    metadata_path = tmp_path / "market-metadata.json"
    metadata_bytes = (json.dumps(_metadata(), sort_keys=True) + "\n").encode()
    metadata_path.write_bytes(metadata_bytes)
    v2 = adapt_onboarding_entry(
        entry,
        entry_manifest,
        split,
        tmp_path / "adapted-v2",
        source_id="fixture_cffex_l2",
        version=2,
        metadata_path=metadata_path,
    )

    assert v1.source_document["content_hash"] == v2.source_document["content_hash"]
    assert v1.source_document["manifest_ref"]["content_hash"] != \
        v2.source_document["manifest_ref"]["content_hash"]
    assert "market_metadata_ref" not in v1.source_document
    assert v2.source_document["market_metadata_ref"]["content_hash"] == \
        sha256_file(v2.output_directory / "market_metadata.json")
    assert (v2.output_directory / "market_metadata.json").read_bytes() == metadata_bytes

    registry = Registry(tmp_path / "research.duckdb")
    try:
        service = ResearchService(registry)
        first = service.register_source(v1.source_document)
        second = service.register_source(v2.source_document)
    finally:
        registry.close()
    assert first["content_hash"] == second["content_hash"]
    assert second["market_metadata_ref"]["content_hash"].startswith("sha256:")

    reader = OnboardingDataSourceReader(v2.source_document)
    assert reader.tick_size("T2603", "2026-01-05") == Decimal("0.005")
    assert reader.tick_size("T2603", "2026-01-06") == Decimal("0.010")
    assert reader.tick_size("T2603", "2025-12-31") is None
    assert reader.units() == {
        "price_unit": "quoted_price_unit",
        "size_unit": "contract",
        "turnover_unit": "quoted_currency",
    }


def test_changed_entry_and_zero_accepted_source_are_rejected(tmp_path: Path) -> None:
    entry, entry_manifest, split = _fixture_entry(tmp_path)
    tampered_entry = tmp_path / "tampered-onboarding.json"
    tampered_entry.write_bytes(entry.read_bytes() + b"\n")
    with pytest.raises(OnboardingAdapterError, match="uri|content_hash"):
        adapt_onboarding_entry(
            tampered_entry,
            entry_manifest,
            split,
            tmp_path / "tampered-output",
            source_id="fixture_cffex_l2",
            version=1,
        )

    empty_entry = json.loads(entry.read_text(encoding="utf-8"))
    empty_entry["quality_report"]["accepted_rows"] = 0
    empty_entry["quality_report"]["quarantined_rows"] = \
        empty_entry["quality_report"]["input_rows"]
    empty_path = tmp_path / "empty-onboarding.json"
    empty_path.write_text(json.dumps(empty_entry, sort_keys=True) + "\n", encoding="utf-8")
    outer = json.loads(entry_manifest.read_text(encoding="utf-8"))
    outer["uri"] = empty_path.resolve().as_uri()
    outer["content_hash"] = sha256_file(empty_path)
    outer["artifact_id"] = "art_" + outer["content_hash"].removeprefix("sha256:")
    empty_manifest = tmp_path / "empty-onboarding-manifest.json"
    empty_manifest.write_text(json.dumps(outer, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(OnboardingAdapterError, match="zero accepted"):
        adapt_onboarding_entry(
            empty_path,
            empty_manifest,
            split,
            tmp_path / "empty-output",
            source_id="fixture_cffex_l2",
            version=1,
        )
    assert not (tmp_path / "empty-output").exists()


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda document: document["tick_rules"][0].update(tick_size="0"),
            "positive decimal",
        ),
        (
            lambda document: document["tick_rules"].append({
                "product": "T",
                "valid_from": "2026-01-01",
                "valid_to": None,
                "tick_size": "0.010",
                "unit": "quoted_price_unit",
            }),
            "overlapping",
        ),
    ],
)
def test_market_metadata_validation_rejects_bad_ticks(
    mutation,
    message: str,
) -> None:
    document = _metadata()
    mutation(document)
    with pytest.raises(ResearchValidationError, match=message):
        validate_market_metadata_document(document)


def test_gap_event_file_is_serialized_and_traceable(tmp_path: Path) -> None:
    adapted = _adapt(tmp_path)
    event_file = pq.ParquetFile(adapted.gap_events_path)
    assert event_file.schema_arrow.equals(GAP_EVENT_SCHEMA, check_metadata=False)
    table = event_file.read()
    reasons = {
        reason
        for values in table["reason_codes"].to_pylist()
        for reason in values
    }
    assert {
        "DUPLICATE_EVENT_KEY_IDENTICAL",
        "INVALID_L1_BOOK",
        "SESSION_UNCLASSIFIED",
        "SOURCE_INTERVAL_START_UNKNOWN",
    }.issubset(reasons)
    quarantined = table.filter(pc.equal(table["event_kind"], "QUARANTINED_RECORD"))
    assert quarantined.num_rows == adapted.summary["quarantined_rows"]
    assert set(quarantined["evidence_sha256"].to_pylist())
    assert all(record_id for record_id in quarantined["evidence_record_id"].to_pylist())
