from __future__ import annotations

import hashlib
import json
from collections import Counter
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from quantsage_discovery.core.artifacts import ArtifactStore, artifact_path
from quantsage_discovery.core.registry import Registry
from quantsage_discovery.core.utils import canonical_json
from quantsage_discovery.core.workflow import WorkflowEngine
from quantsage_discovery.ingest.calendar import sha256_file
from quantsage_discovery.ingest.readers import data_snapshot_id, prepare_onboarding_config
from quantsage_discovery.ingest.schema import load_source_schema
from quantsage_discovery.stages.bootstrap import BootstrapStage
from quantsage_discovery.stages.onboard import (
    OnboardStage,
    _merge_partition_files,
    _register_global_duplicate_indexes,
)
from scripts.verify_onboarding import verify_entry

ROOT = Path(__file__).resolve().parents[1]
SOURCE_SCHEMA = ROOT / "docs/references/gov_bond_l2.schema.json"
SESSION_EVIDENCE = ROOT / "configs/reference/cffex_calendar_evidence.json"


def _prepare_fixture_config(
    raw_path: Path,
    tmp_path: Path,
    *,
    max_quarantine_rate: float | None = None,
) -> dict:
    template = {
        "project": "Step02 serialized integration fixture",
        "input": {
            "adapter": "l2_snapshots",
            "paths": [str(raw_path)],
            "source_timezone": "Asia/Shanghai",
            "source_schema_path": str(SOURCE_SCHEMA),
        },
        "calendar": {
            "provider": "local_duckdb",
            "calendar_id": "CN_STOCK",
            "data_root": "/data/marketdata",
            "evidence_path": str(SESSION_EVIDENCE),
        },
        "sessions": {"evidence_path": str(SESSION_EVIDENCE)},
        "resources": {"memory_limit": "1GB", "threads": 2, "temporary_uri": str(tmp_path / "tmp")},
        "storage": {
            "artifacts_uri": str(tmp_path / "artifacts"),
            "registry_uri": str(tmp_path / "registry.sqlite"),
        },
        "workflow": {"stages": [
            "quantsage_discovery.stages.bootstrap:BootstrapStage",
            "quantsage_discovery.stages.onboard:OnboardStage",
        ]},
    }
    if max_quarantine_rate is not None:
        template["onboarding"] = {"max_quarantine_rate": max_quarantine_rate}
    return prepare_onboarding_config(template)


def _run_fixture_workflow(
    raw_path: Path,
    tmp_path: Path,
    run_id: str,
    *,
    max_quarantine_rate: float | None = None,
):
    config = _prepare_fixture_config(raw_path, tmp_path, max_quarantine_rate=max_quarantine_rate)
    store = ArtifactStore(config["storage"]["artifacts_uri"])
    engine = WorkflowEngine(
        stages=[BootstrapStage(), OnboardStage()],
        config=config,
        artifact_store=store,
        registry=Registry(config["storage"]["registry_uri"]),
    )
    return engine.run(until_stage="onboard", run_id=run_id), store


def test_serialized_bootstrap_onboard_reconciles_and_publishes_entry(tmp_path: Path) -> None:
    fixture_path = ROOT / "tests/fixtures/onboarding_acceptance/part-0.parquet"
    source_table = pq.read_table(fixture_path)
    source_rows = source_table.to_pylist()
    for row in source_rows:
        if row["SYMBOL"] == "T2603" and row["TRADINGDATE"].isoformat() == "2026-01-05":
            time_text = row["TRADINGTIME"].strftime("%H:%M:%S.%f")
            counters = {
                "09:29:00.000000": ("0.00", "0", "0.000", "0.000"),
                "09:30:00.000000": ("10.00", "10", "100.000", "100.000"),
                "10:00:00.000000": ("20.00", "5", "200.000", "50.000"),
                "10:01:00.000000": ("20.00", "0", "200.000", "0.000"),
                "11:29:59.500000": ("15.00", "0", "150.000", "0.000"),
            }
            if time_text in counters:
                (row["TOTALVOLUME"], row["TRADEVOLUME"], row["TOTALAMOUNT"], row["TRADEAMOUNT"]) = tuple(
                    Decimal(value) for value in counters[time_text]
                )
    for row_index, level in zip((1, 3, 10, 12), range(2, 6), strict=True):
        source_rows[row_index][f"BUYVOLUME{level:02d}"] = Decimal("-1.00")
    source_table = pa.Table.from_pylist(source_rows, schema=source_table.schema)
    symbols = source_table["SYMBOL"].to_pylist()
    dates = source_table["TRADINGDATE"].to_pylist()
    times = source_table["TRADINGTIME"].to_pylist()
    order = list(range(source_table.num_rows))
    reordered = False
    for earlier in range(source_table.num_rows):
        for later in range(earlier + 1, source_table.num_rows):
            if symbols[earlier] == symbols[later] and dates[earlier] == dates[later] and times[earlier] < times[later]:
                order[earlier], order[later] = order[later], order[earlier]
                reordered = True
                break
        if reordered:
            break
    assert reordered, "acceptance fixture needs two distinct ordered events in one contract/date scope"
    raw_path = tmp_path / "reordered source with spaces.parquet"
    pq.write_table(source_table.take(pa.array(order)), raw_path, row_group_size=4, compression="zstd")
    result, _ = _run_fixture_workflow(raw_path, tmp_path, "onboarding-fixture")

    assert result.status == "SUCCEEDED", result.to_dict()
    stage = result.stage_results["onboard"][0]
    refs = stage["artifacts"]
    entry_path = artifact_path(refs["onboarding"]["uri"])
    report = verify_entry(entry_path)
    assert report["status"] == "PASS"
    quality = report["quality_summary"]
    assert quality["input_rows"] == 14
    assert quality["accepted_rows"] == 9
    assert quality["quarantined_rows"] == 5
    assert quality["partition_count"] >= 3
    assert quality["quality_rule_outcomes"]["WARN"] >= 1

    entry = json.loads(entry_path.read_text(encoding="utf-8"))
    report = entry["quality_report"]
    assert entry["inputs"]["raw_files"][0]["trade_date_count"] == 2
    for rule in report["rules"]:
        assert "example_source_refs" in rule
        if rule.get("hit_count"):
            assert rule["example_source_refs"]
    scopes = report["scopes"]
    assert sum(item["row_count"] for item in scopes) == 14
    assert sum(item["accepted_rows"] + item["quarantined_rows"] for item in scopes) == 14
    assert sum(item["duplicate_event_groups"] for item in scopes) == 1
    assert sum(item["duplicate_event_extra_rows"] for item in scopes) == 1
    assert sum(item["duplicate_member_rows"] for item in scopes) == 2
    assert report["duplicate_rate"]["numerator"] == 1
    assert report["duplicate_rate"]["denominator"] == 14
    assert report["duplicate_rate"]["value"] == 1 / 14
    duplicate_scope = next(item for item in scopes if item["duplicate_event_groups"])
    assert duplicate_scope["duplicate_rate"]["numerator"] == 1
    assert duplicate_scope["duplicate_rate"]["denominator"] == duplicate_scope["row_count"]
    assert duplicate_scope["duplicate_rate"]["value"] == 1 / duplicate_scope["row_count"]
    assert sum(item["invalid_quote_rows"] for item in scopes) == 2
    assert sum(item["time_reorder_count"] for item in scopes) >= 1
    assert report["volume_turnover_increment_mismatch_rows"] > 0
    assert report["cumulative_reset_rows"] > 0
    assert "DEPTH_BOOK_INVALID" not in report["quarantine_reason_counts"]
    assert report["quality_flag_counts"]["DEPTH_BOOK_INVALID"] == 4
    assert report["quality_flag_counts"]["END_OF_DAY_FIELDS_UNVERIFIED"] == 9
    assert report["quality_flag_counts"]["VOLUME_INCREMENT_UNVERIFIED"] == 9
    depth_rule = next(rule for rule in report["rules"] if rule["rule_id"] == "DEPTH_BOOK_INVALID")
    assert depth_rule["scope"] == "accepted_quote_row"
    assert depth_rule["denominator"] == 9
    for item in scopes:
        assert item["duplicate_rate"]["denominator"] == item["row_count"]
        assert item["invalid_quote_rate"]["denominator"] == item["row_count"]
        assert item["null_rate"]["denominator"] == item["row_count"] * 66
        assert item["stale_quote_segments"]["value"] is None
        assert item["stale_quote_segments"]["reason"]
    session_scopes = report["session_scopes"]
    assert sum(item["row_count"] for item in session_scopes) == 14
    assert sum(item["row_count"] for item in session_scopes if item["classification"] == "unclassified") == 1
    gap_summaries = report["event_time_gap_quantiles"]
    evaluated = [item for item in gap_summaries if item["status"] == "EVALUATED"]
    assert evaluated
    assert all(item["sample_count"] == item["positive_interval_count"] for item in evaluated)
    assert all(item["p50_seconds"] <= item["p90_seconds"] <= item["p99_seconds"] for item in evaluated)

    quote_tables = [
        pq.read_table(artifact_path(ref["uri"]))
        for alias, ref in refs.items()
        if alias.startswith("quote_")
    ]
    all_quotes = pa.concat_tables(quote_tables)
    assert all(value == Decimal("0.000") for value in all_quotes["SETTLEPRICE"].to_pylist())
    assert all(value == Decimal("0.000") for value in all_quotes["CLOSEPRICE"].to_pylist())
    assert sum("DEPTH_BOOK_INVALID" in flags for flags in all_quotes["quality_flags"].to_pylist()) == 4
    observed_flags = Counter(flag for flags in all_quotes["quality_flags"].to_pylist() for flag in flags)
    assert dict(observed_flags) == report["quality_flag_counts"]

    first_ref = refs["quote_0000_0000"]
    quote_path = artifact_path(first_ref["uri"])
    quote = pq.read_table(quote_path)
    split = quote.num_rows // 3
    later_path = tmp_path / "a-later.parquet"
    earlier_path = tmp_path / "b-earlier.parquet"
    pq.write_table(quote.slice(split), later_path)
    pq.write_table(quote.slice(0, split), earlier_path)
    merged_path = tmp_path / "merged.parquet"
    with duckdb.connect(database=":memory:") as connection:
        merged_rows = _merge_partition_files(
            connection,
            [later_path, earlier_path],
            merged_path,
            expected_rows=quote.num_rows,
            expected_schema=quote.schema,
        )
        assert merged_rows == quote.num_rows
        merged = pq.read_table(merged_path, columns=["event_time", "raw_file_sha256", "raw_row_index"])
        order = list(zip(merged["event_time"].to_pylist(), merged["raw_file_sha256"].to_pylist(),
                         merged["raw_row_index"].to_pylist(), strict=True))
        assert order == sorted(order)

        missing_part_path = tmp_path / "missing-part-merge.parquet"
        try:
            _merge_partition_files(
                connection,
                [later_path],
                missing_part_path,
                expected_rows=quote.num_rows,
                expected_schema=quote.schema,
            )
        except ValueError as exc:
            assert "footer reconciliation failed" in str(exc)
        else:  # pragma: no cover - protects the prepublish all-part footer gate
            raise AssertionError("missing partition piece was accepted")
        assert not missing_part_path.exists()


def test_global_duplicate_extra_rows_have_deterministic_source_attribution(tmp_path: Path) -> None:
    first_path = ROOT / "tests/fixtures/onboarding_acceptance/part-0.parquet"
    second_path = tmp_path / "second-copy.parquet"
    pq.write_table(pq.read_table(first_path), second_path, compression="snappy", row_group_size=4)
    paths = [first_path, second_path]
    inventory = [{
        "uri": path.resolve().as_uri(),
        "sha256": sha256_file(path),
        "source_format": "parquet",
    } for path in paths]
    source = load_source_schema(SOURCE_SCHEMA)
    with duckdb.connect(database=":memory:", config={"memory_limit": "1GB", "threads": "2"}) as connection:
        _register_global_duplicate_indexes(
            connection, inventory, source, event_keys_overlap=True, provenance_pairs_overlap=False
        )
        actual = set(connection.execute(
            "SELECT raw_file_sha256,raw_row_index,MARKET,SYMBOL,TRADINGDATE,TRADINGTIME "
            "FROM _global_event_extra"
        ).fetchall())

    expected_members: dict[tuple[object, ...], list[tuple[str, int]]] = {}
    for path, item in zip(paths, inventory, strict=True):
        parquet = pq.ParquetFile(path)
        raw_index = 0
        for group_index in range(parquet.num_row_groups):
            table = parquet.read_row_group(group_index, columns=["MARKET", "SYMBOL", "TRADINGDATE", "TRADINGTIME"])
            for row in table.to_pylist():
                key = (row["MARKET"], row["SYMBOL"], row["TRADINGDATE"], row["TRADINGTIME"])
                expected_members.setdefault(key, []).append((item["sha256"].removeprefix("sha256:"), raw_index))
                raw_index += 1
    expected = set()
    for key, members in expected_members.items():
        if len(members) > 1:
            for raw_sha, raw_index in sorted(members)[1:]:
                expected.add((raw_sha, raw_index, *key))
    assert actual == expected
    assert len(actual) == 15


def test_product_difference_makes_repeated_event_payload_conflicting(tmp_path: Path) -> None:
    source_table = pq.read_table(ROOT / "tests/fixtures/onboarding_acceptance/part-0.parquet")
    source_rows = source_table.to_pylist()
    assert source_rows[6]["SYMBOL"] == source_rows[7]["SYMBOL"] == "T2603"
    assert source_rows[6]["TRADINGTIME"] == source_rows[7]["TRADINGTIME"]
    assert source_rows[6]["product"] == source_rows[7]["product"] == "T"

    source_rows[7]["product"] = "TF"
    conflicting_table = pa.Table.from_pylist(source_rows, schema=source_table.schema)
    raw_path = tmp_path / "product-only event conflict.parquet"
    pq.write_table(conflicting_table, raw_path, compression="zstd", row_group_size=4)
    result, _ = _run_fixture_workflow(raw_path, tmp_path, "product-payload-conflict")

    assert result.status == "SUCCEEDED", result.to_dict()
    refs = result.stage_results["onboard"][0]["artifacts"]
    entry_path = artifact_path(refs["onboarding"]["uri"])
    entry = json.loads(entry_path.read_text(encoding="utf-8"))
    quality = entry["quality_report"]
    assert quality["input_rows"] == 14
    assert quality["accepted_rows"] == 9
    assert quality["quarantined_rows"] == 5
    assert quality["duplicate_classification"]["conflicting_payload_groups"] == 1
    assert quality["duplicate_classification"]["conflicting_payload_rows"] == 2
    assert quality["duplicate_classification"]["identical_payload_groups"] == 0
    assert quality["quarantine_reason_counts"]["DUPLICATE_EVENT_KEY_CONFLICTING"] == 2
    assert quality["quarantine_reason_counts"]["DUPLICATE_EVENT_KEY_IDENTICAL"] == 0

    quarantine_tables = [
        pq.read_table(artifact_path(ref["uri"]))
        for alias, ref in refs.items()
        if alias.startswith("quarantine_")
    ]
    quarantine_rows = pa.concat_tables(quarantine_tables).to_pylist()
    duplicate_members = [
        row for row in quarantine_rows
        if row["SYMBOL"] == "T2603"
        and row["TRADINGDATE"].isoformat() == "2026-01-05"
        and row["TRADINGTIME"].strftime("%H:%M:%S.%f") == "10:01:00.000000"
    ]
    assert len(duplicate_members) == 2
    assert {row["product"] for row in duplicate_members} == {"T", "TF"}
    assert all("DUPLICATE_EVENT_KEY_CONFLICTING" in row["reason_codes"]
               for row in duplicate_members)
    assert verify_entry(entry_path)["status"] == "PASS"


def _assert_onboard_failure_unpublished(result, store: ArtifactStore, message_part: str) -> None:
    assert result.status == "FAILED"
    stage = result.stage_results["onboard"][0]
    assert stage["status"] == "FAILED"
    assert message_part in stage["error"]["message"]
    assert stage["artifacts"] == {}
    final_bundle = store.root / "runs" / result.run_id / "stages" / stage["stage_run_id"]
    assert not final_bundle.exists()
    staging = store.root / ".staging"
    assert not staging.exists() or not list(staging.iterdir())
    assert not list(store.root.rglob("onboarding.json"))


def test_empty_source_fails_without_publishing_ready_entry(tmp_path: Path) -> None:
    source_schema = load_source_schema(SOURCE_SCHEMA)
    arrow_schema = pa.schema([
        pa.field(field.name, field.arrow_type, nullable=field.nullable)
        for field in source_schema.fields
    ])
    empty_path = tmp_path / "empty-input.parquet"
    pq.write_table(pa.Table.from_batches([], schema=arrow_schema), empty_path)

    result, store = _run_fixture_workflow(empty_path, tmp_path, "onboarding-empty")

    _assert_onboard_failure_unpublished(result, store, "no usable TRADINGDATE values")


def test_missing_required_physical_field_fails_workflow_without_ready_entry(tmp_path: Path) -> None:
    original = ROOT / "tests/fixtures/onboarding_acceptance/part-0.parquet"
    config = _prepare_fixture_config(original, tmp_path)
    inventory_path = Path(config["bootstrap"]["source_inventory_path"])
    split_path = Path(config["bootstrap"]["split_manifest_path"])
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    split_manifest = json.loads(split_path.read_text(encoding="utf-8"))

    malformed_path = tmp_path / "missing-TRADINGTIME.parquet"
    table = pq.read_table(original)
    pq.write_table(table.drop(["TRADINGTIME"]), malformed_path, row_group_size=4, compression="zstd")
    file_item = inventory["raw_files"][0]
    file_item.update({
        "uri": malformed_path.as_uri(),
        "sha256": sha256_file(malformed_path),
        "size_bytes": malformed_path.stat().st_size,
    })
    snapshot_id = data_snapshot_id(
        inventory["adapter"], inventory["source_schema_sha256"], inventory["raw_files"]
    )
    inventory["data_snapshot_id"] = snapshot_id
    inventory_payload = {key: value for key, value in inventory.items() if key != "inventory_sha256"}
    inventory["inventory_sha256"] = "sha256:" + hashlib.sha256(
        canonical_json(inventory_payload).encode("utf-8")
    ).hexdigest()
    split_payload = {key: value for key, value in split_manifest.items() if key != "split_id"}
    split_payload["source_data_snapshot_id"] = snapshot_id
    split_manifest = {
        **split_payload,
        "split_id": "split_" + hashlib.sha256(canonical_json(split_payload).encode("utf-8")).hexdigest(),
    }
    inventory_path.write_text(canonical_json(inventory) + "\n", encoding="utf-8")
    split_path.write_text(canonical_json(split_manifest) + "\n", encoding="utf-8")
    config["data_snapshot_id"] = snapshot_id
    config["splits"]["split_id"] = split_manifest["split_id"]
    config["input"]["paths"] = [str(malformed_path)]
    config["bootstrap"]["source_inventory_sha256"] = sha256_file(inventory_path)
    config["bootstrap"]["split_manifest_sha256"] = sha256_file(split_path)

    store = ArtifactStore(config["storage"]["artifacts_uri"])
    engine = WorkflowEngine(
        stages=[BootstrapStage(), OnboardStage()],
        config=config,
        artifact_store=store,
        registry=Registry(config["storage"]["registry_uri"]),
    )
    result = engine.run(until_stage="onboard", run_id="onboarding-missing-required-field")

    _assert_onboard_failure_unpublished(result, store, "TRADINGTIME")
    error = result.stage_results["onboard"][0]["error"]
    assert error["code"] == "MISSING_FIELD"


def test_zero_accepted_rows_fails_and_aborts_private_outputs(tmp_path: Path) -> None:
    source_path = ROOT / "tests/fixtures/onboarding_acceptance/part-0.parquet"
    table = pq.read_table(source_path)
    records = table.to_pylist()
    for row in records:
        row["BUYPRICE01"] = Decimal("101.000")
        row["SELLPRICE01"] = Decimal("100.000")
    all_crossed = tmp_path / "all-crossed.parquet"
    pq.write_table(pa.Table.from_pylist(records, schema=table.schema), all_crossed, row_group_size=4)

    result, store = _run_fixture_workflow(all_crossed, tmp_path, "onboarding-zero-accepted")

    _assert_onboard_failure_unpublished(result, store, "zero accepted rows")


def test_prepublication_quality_threshold_aborts_reserved_artifacts(tmp_path: Path) -> None:
    source_path = ROOT / "tests/fixtures/onboarding_acceptance/part-0.parquet"

    result, store = _run_fixture_workflow(
        source_path,
        tmp_path,
        "onboarding-quality-rollback",
        max_quarantine_rate=0.0,
    )

    _assert_onboard_failure_unpublished(result, store, "exceeds configured maximum")
