import json
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv

from quantsage_discovery.core.artifacts import ArtifactStore, artifact_path
from quantsage_discovery.core.registry import Registry
from quantsage_discovery.core.types import ArtifactRef
from quantsage_discovery.core.workflow import WorkflowEngine
from quantsage_discovery.ingest.readers import (
    inspect_source_file,
    prepare_onboarding_config,
    verify_source_inventory,
)
from quantsage_discovery.ingest.schema import load_source_schema
from quantsage_discovery.stages.bootstrap import BootstrapStage

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "docs/references/gov_bond_l2.schema.json"


def _write_two_row_l2(path: Path, *, include_null_identity: bool = False) -> None:
    source = load_source_schema(SCHEMA_PATH)
    arrays = {field.name: pa.nulls(2, type=field.arrow_type) for field in source.fields}
    arrays.update(
        {
            "TRADINGDATE": pa.array(
                [date(2026, 1, 5), None if include_null_identity else date(2026, 1, 5)],
                pa.date32(),
            ),
            "TRADINGTIME": pa.array(
                [
                    datetime.fromisoformat("2026-01-05T09:30:00"),
                    None
                    if include_null_identity
                    else datetime.fromisoformat("2026-01-05T09:30:01"),
                ],
                pa.timestamp("us"),
            ),
            "SYMBOL": pa.array(["T2603", None if include_null_identity else "T2603"], pa.string()),
            "MARKET": pa.array(["CFFEX", "CFFEX"], pa.string()),
            "UNIX": pa.array([1767605400000, 1767605401000], pa.int64()),
            "product": pa.array(["T", "T"], pa.string()),
        }
    )
    table = pa.table(arrays).select(source.names)
    pq.write_table(table, path)


def test_preflight_freezes_actual_file_hashes_and_unassigned_split(tmp_path: Path) -> None:
    source_path = tmp_path / "fixture.parquet"
    _write_two_row_l2(source_path)
    config = {
        "project": "QuantSage fixture",
        "input": {
            "adapter": "l2_snapshots",
            "paths": [str(source_path)],
            "source_timezone": "Asia/Shanghai",
            "source_schema_path": str(SCHEMA_PATH),
        },
        "storage": {"artifacts_uri": str(tmp_path / "artifacts")},
        "workflow": {"stages": []},
    }

    prepared = prepare_onboarding_config(config)
    inventory_path = Path(prepared["bootstrap"]["source_inventory_path"])
    split_path = Path(prepared["bootstrap"]["split_manifest_path"])
    inventory = __import__("json").loads(inventory_path.read_text(encoding="utf-8"))
    split = __import__("json").loads(split_path.read_text(encoding="utf-8"))

    assert prepared["data_snapshot_id"] == inventory["data_snapshot_id"]
    assert prepared["splits"]["split_id"] == split["split_id"]
    assert inventory["raw_files"][0]["row_count"] == 2
    assert inventory["source_date_range"] == {"start": "2026-01-05", "end": "2026-01-05"}
    assert split["method"] == "unassigned_research_only"
    assert split["fitting_allowed"] is False
    assert split["hypothesis_stages_allowed"] is False
    assert split["train"] is split["development"] is split["final_holdout"] is None
    assert Path(prepared["bootstrap"]["resolved_config_path"]).is_file()
    assert prepared["bootstrap"]["source_inventory_sha256"].startswith("sha256:")
    assert prepared["bootstrap"]["split_manifest_sha256"].startswith("sha256:")


def test_preflight_snapshot_and_split_ids_are_stable_for_same_inputs(tmp_path: Path) -> None:
    source_path = tmp_path / "fixture.parquet"
    _write_two_row_l2(source_path)
    base = {
        "input": {
            "adapter": "l2_snapshots",
            "paths": [str(source_path)],
            "source_timezone": "Asia/Shanghai",
            "source_schema_path": str(SCHEMA_PATH),
        },
        "storage": {"artifacts_uri": str(tmp_path / "artifacts")},
        "workflow": {"stages": []},
    }

    first = prepare_onboarding_config(base)
    second = prepare_onboarding_config(base)

    assert first["data_snapshot_id"] == second["data_snapshot_id"]
    assert first["splits"]["split_id"] == second["splits"]["split_id"]


def test_file_uri_source_inventory_decodes_partition_equals_and_spaces(tmp_path: Path) -> None:
    source_path = tmp_path / "year=2026" / "part with spaces.parquet"
    source_path.parent.mkdir(parents=True)
    _write_two_row_l2(source_path)
    prepared = prepare_onboarding_config({
        "input": {
            "adapter": "l2_snapshots",
            "paths": [str(source_path)],
            "source_timezone": "Asia/Shanghai",
            "source_schema_path": str(SCHEMA_PATH),
        },
        "storage": {"artifacts_uri": str(tmp_path / "artifacts")},
        "workflow": {"stages": []},
    })
    inventory_path = Path(prepared["bootstrap"]["source_inventory_path"])
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))

    assert "%3D" in inventory["raw_files"][0]["uri"]
    assert "%20" in inventory["raw_files"][0]["uri"]
    verify_source_inventory(inventory, verify_hashes=True)


def test_nullable_date_and_identity_values_are_counted_not_fatal(tmp_path: Path) -> None:
    source_path = tmp_path / "nullable.parquet"
    _write_two_row_l2(source_path, include_null_identity=True)
    config = {
        "input": {
            "adapter": "l2_snapshots",
            "paths": [str(source_path)],
            "source_timezone": "Asia/Shanghai",
            "source_schema_path": str(SCHEMA_PATH),
        },
        "storage": {"artifacts_uri": str(tmp_path / "artifacts")},
        "workflow": {"stages": []},
    }

    prepared = prepare_onboarding_config(config)
    import json

    inventory = json.loads(
        Path(prepared["bootstrap"]["source_inventory_path"]).read_text(encoding="utf-8")
    )
    file_info = inventory["raw_files"][0]

    assert inventory["input_row_count"] == 2
    assert inventory["source_date_range"] == {"start": "2026-01-05", "end": "2026-01-05"}
    assert file_info["null_trade_date_count"] == 1
    assert file_info["null_instrument_id_count"] == 1


def test_csv_adapter_uses_schema_arrow_types_and_counts_nulls(tmp_path: Path) -> None:
    parquet_path = tmp_path / "nullable.parquet"
    csv_path = tmp_path / "nullable.csv"
    _write_two_row_l2(parquet_path, include_null_identity=True)
    csv.write_csv(pq.read_table(parquet_path), csv_path)

    info = inspect_source_file(csv_path, load_source_schema(SCHEMA_PATH))

    assert info["source_format"] == "csv"
    assert info["row_count"] == 2
    assert info["null_trade_date_count"] == 1
    assert info["null_instrument_id_count"] == 1


def test_bootstrap_publishes_serialized_snapshot_and_split_manifests(tmp_path: Path) -> None:
    source_path = tmp_path / "fixture.parquet"
    _write_two_row_l2(source_path)
    template = {
        "project": "QuantSage bootstrap test",
        "input": {
            "adapter": "l2_snapshots",
            "paths": [str(source_path)],
            "source_timezone": "Asia/Shanghai",
            "source_schema_path": str(SCHEMA_PATH),
        },
        "storage": {"artifacts_uri": str(tmp_path / "artifacts")},
        "workflow": {"stages": ["quantsage_discovery.stages.bootstrap:BootstrapStage"]},
    }
    config = prepare_onboarding_config(template)
    store = ArtifactStore(config["storage"]["artifacts_uri"])
    engine = WorkflowEngine(
        stages=[BootstrapStage()],
        config=config,
        artifact_store=store,
        registry=Registry(tmp_path / "registry.sqlite"),
    )

    summary = engine.run(until_stage="bootstrap", run_id="bootstrap-test")

    assert summary.status == "SUCCEEDED"
    result = summary.stage_results["bootstrap"][0]
    assert result["status"] == "SUCCEEDED"
    refs = result["artifacts"]
    snapshot = refs["source_snapshot"]
    split = refs["split_manifest"]
    snapshot_manifest = store.verify(ArtifactRef.from_dict(snapshot))
    split_manifest = store.verify(ArtifactRef.from_dict(split))

    assert snapshot_manifest.data_snapshot_id == config["data_snapshot_id"]
    assert split_manifest.split_id == config["splits"]["split_id"]
    assert artifact_path(snapshot["uri"]).is_file()
