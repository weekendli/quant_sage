"""Input discovery, physical schema checks, and content-addressed preflight snapshots."""

from __future__ import annotations

import glob
import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from pyarrow import csv

from quantsage_discovery.core.utils import canonical_json
from quantsage_discovery.ingest.calendar import sha256_file
from quantsage_discovery.ingest.schema import (
    SourceSchema,
    load_source_schema,
    validate_physical_schema,
)


def _resolve_path(value: str | Path, base_dir: Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (base_dir / path).resolve()


def _path_from_uri(value: str) -> Path:
    parsed = urlparse(value)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path)).resolve()
    return Path(value).expanduser().resolve()


def resolve_source_paths(input_config: Mapping[str, Any], *, base_dir: str | Path) -> list[Path]:
    base = Path(base_dir).expanduser().resolve()
    found: list[Path] = []
    explicit = input_config.get("paths", [])
    if explicit and (not isinstance(explicit, list) or any(not isinstance(x, str) for x in explicit)):
        raise ValueError("input.paths must be a list of path strings")
    path_glob = input_config.get("path_glob")
    patterns = list(explicit)
    if path_glob is not None:
        if not isinstance(path_glob, str) or not path_glob:
            raise ValueError("input.path_glob must be a non-empty string")
        patterns.append(path_glob)
    if not patterns:
        raise ValueError("input must declare paths or path_glob")
    for pattern in patterns:
        resolved = _resolve_path(pattern, base)
        if any(token in str(resolved) for token in ("*", "?", "[")):
            found.extend(Path(match).resolve() for match in glob.glob(str(resolved), recursive=True))
        elif resolved.is_file():
            found.append(resolved)
        else:
            raise ValueError(f"input source path does not exist or is not a file: {resolved}")
    unique = sorted(set(found), key=lambda path: path.as_posix())
    if not unique:
        raise ValueError("input source selection matched no files")
    missing = [str(path) for path in unique if not path.is_file()]
    if missing:
        raise ValueError("input selection contains missing files: " + ", ".join(missing[:5]))
    extensions = {path.suffix.lower() for path in unique}
    if not extensions.issubset({".parquet", ".csv"}):
        raise ValueError(f"input selection contains unsupported extensions: {sorted(extensions)}")
    if len(extensions) != 1:
        raise ValueError("a single L2 snapshot run cannot mix Parquet and CSV source files")
    return unique


def _csv_type(field: pa.Field) -> pa.DataType:
    return field.arrow_type


def _inspect_parquet(
    path: Path,
    schema: SourceSchema,
    *,
    memory_limit: str,
    threads: int,
    temporary_root: Path,
) -> dict[str, Any]:
    parquet = pq.ParquetFile(path)
    validate_physical_schema(parquet.schema_arrow, schema)
    temporary_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="inspect-", dir=temporary_root) as scratch, duckdb.connect(
        ":memory:",
        config={
            "memory_limit": memory_limit,
            "threads": str(threads),
            "temp_directory": scratch,
        },
    ) as connection:
        result = connection.execute(
            "SELECT count(*), min(TRADINGDATE), max(TRADINGDATE), "
            "count(DISTINCT TRADINGDATE), "
            "list(DISTINCT TRADINGDATE ORDER BY TRADINGDATE) "
            "FILTER (WHERE TRADINGDATE IS NOT NULL), "
            "list(DISTINCT SYMBOL ORDER BY SYMBOL) FILTER (WHERE SYMBOL IS NOT NULL), "
            "list(DISTINCT MARKET ORDER BY MARKET) FILTER (WHERE MARKET IS NOT NULL), "
            "list(DISTINCT product ORDER BY product) FILTER (WHERE product IS NOT NULL), "
            "list(DISTINCT source_file ORDER BY source_file) "
            "FILTER (WHERE source_file IS NOT NULL), "
            "list(DISTINCT ingest_run_id ORDER BY ingest_run_id) "
            "FILTER (WHERE ingest_run_id IS NOT NULL), "
            "count(*) FILTER (WHERE TRADINGDATE IS NULL), "
            "count(*) FILTER (WHERE SYMBOL IS NULL), "
            "count(*) FILTER (WHERE MARKET IS NULL), "
            "count(*) FILTER (WHERE source_row IS NULL) "
            "FROM read_parquet(?, hive_partitioning=false)",
            [str(path)],
        ).fetchone()
        source_identity_pairs = connection.execute(
            "SELECT DISTINCT source_file, ingest_run_id "
            "FROM read_parquet(?, hive_partitioning=false) "
            "WHERE source_file IS NOT NULL AND ingest_run_id IS NOT NULL "
            "ORDER BY source_file, ingest_run_id",
            [str(path)],
        ).fetchall()
    group_counts = [int(parquet.metadata.row_group(index).num_rows) for index in range(parquet.metadata.num_row_groups)]
    offsets: list[int] = []
    running = 0
    for count in group_counts:
        offsets.append(running)
        running += count
    return {
        "source_format": "parquet",
        "row_count": int(result[0]),
        "row_group_count": int(parquet.metadata.num_row_groups),
        "row_group_offsets": offsets,
        "row_group_row_counts": group_counts,
        "trade_date_min": result[1].isoformat() if result[1] else None,
        "trade_date_max": result[2].isoformat() if result[2] else None,
        "trade_date_count": int(result[3]),
        "trade_dates": [value.isoformat() for value in (result[4] or [])],
        "instrument_ids": list(result[5] or []),
        "exchanges": list(result[6] or []),
        "products": list(result[7] or []),
        "source_file_values": list(result[8] or []),
        "ingest_run_id_values": list(result[9] or []),
        "source_provenance_pairs": [
            {"source_file": row[0], "ingest_run_id": row[1]}
            for row in source_identity_pairs
        ],
        "null_trade_date_count": int(result[10]),
        "null_instrument_id_count": int(result[11]),
        "null_exchange_count": int(result[12]),
        "null_source_row_count": int(result[13]),
    }


def _inspect_csv(path: Path, schema: SourceSchema) -> dict[str, Any]:
    reader = csv.open_csv(
        path,
        read_options=csv.ReadOptions(autogenerate_column_names=False, block_size=1 << 20),
        convert_options=csv.ConvertOptions(
            column_types={field.name: _csv_type(field) for field in schema.fields},
            strings_can_be_null=True,
        ),
    )
    validate_physical_schema(reader.schema, schema)
    row_count = 0
    dates: set[date] = set()
    instruments: set[str] = set()
    exchanges: set[str] = set()
    products: set[str] = set()
    source_files: set[str] = set()
    ingest_runs: set[str] = set()
    source_provenance_pairs: set[tuple[str, str]] = set()
    null_source_row_count = 0
    null_trade_date_count = 0
    null_instrument_id_count = 0
    null_exchange_count = 0
    for batch in reader:
        row_count += batch.num_rows
        trade_dates = batch.column("TRADINGDATE").to_pylist()
        symbols = batch.column("SYMBOL").to_pylist()
        markets = batch.column("MARKET").to_pylist()
        dates.update(value for value in trade_dates if value is not None)
        instruments.update(value for value in symbols if value is not None)
        exchanges.update(value for value in markets if value is not None)
        products.update(value for value in batch.column("product").to_pylist() if value is not None)
        source_file_values = batch.column("source_file").to_pylist()
        ingest_run_values = batch.column("ingest_run_id").to_pylist()
        source_files.update(value for value in source_file_values if value is not None)
        ingest_runs.update(value for value in ingest_run_values if value is not None)
        source_provenance_pairs.update(
            (source_file, ingest_run)
            for source_file, ingest_run in zip(source_file_values, ingest_run_values, strict=True)
            if source_file is not None and ingest_run is not None
        )
        null_source_row_count += batch.column("source_row").null_count
        null_trade_date_count += batch.column("TRADINGDATE").null_count
        null_instrument_id_count += batch.column("SYMBOL").null_count
        null_exchange_count += batch.column("MARKET").null_count
    ordered_dates = sorted(dates)
    return {
        "source_format": "csv",
        "row_count": row_count,
        "row_group_count": None,
        "row_group_offsets": [],
        "row_group_row_counts": [],
        "trade_date_min": ordered_dates[0].isoformat() if ordered_dates else None,
        "trade_date_max": ordered_dates[-1].isoformat() if ordered_dates else None,
        "trade_date_count": len(ordered_dates),
        "trade_dates": [value.isoformat() for value in ordered_dates],
        "instrument_ids": sorted(instruments),
        "exchanges": sorted(exchanges),
        "products": sorted(products),
        "source_file_values": sorted(source_files),
        "ingest_run_id_values": sorted(ingest_runs),
        "source_provenance_pairs": [
            {"source_file": source_file, "ingest_run_id": ingest_run}
            for source_file, ingest_run in sorted(source_provenance_pairs)
        ],
        "null_trade_date_count": null_trade_date_count,
        "null_instrument_id_count": null_instrument_id_count,
        "null_exchange_count": null_exchange_count,
        "null_source_row_count": null_source_row_count,
    }


def inspect_source_file(
    path: str | Path,
    schema: SourceSchema,
    *,
    memory_limit: str = "1GB",
    threads: int = 2,
    temporary_root: str | Path | None = None,
) -> dict[str, Any]:
    source = Path(path).expanduser().resolve()
    if source.suffix.lower() == ".parquet":
        scratch_root = Path(temporary_root).resolve() if temporary_root else Path(tempfile.gettempdir())
        measured = _inspect_parquet(
            source,
            schema,
            memory_limit=memory_limit,
            threads=threads,
            temporary_root=scratch_root,
        )
    elif source.suffix.lower() == ".csv":
        measured = _inspect_csv(source, schema)
    else:
        raise ValueError(f"unsupported L2 source file extension: {source.suffix}")
    measured.update(
        {
            "uri": source.as_uri(),
            "sha256": sha256_file(source),
            "size_bytes": source.stat().st_size,
        }
    )
    return measured


def _identity_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def data_snapshot_id(adapter: str, source_schema_sha256: str, raw_files: list[dict[str, Any]]) -> str:
    fingerprint = {
        "adapter": adapter,
        "source_schema_sha256": source_schema_sha256,
        "raw_files": [
            {
                "uri": item["uri"],
                "sha256": item["sha256"],
                "size_bytes": item["size_bytes"],
                "row_count": item["row_count"],
            }
            for item in raw_files
        ],
    }
    return "snapshot_" + _identity_hash(fingerprint)


def verify_source_inventory(inventory: Mapping[str, Any], *, verify_hashes: bool = True) -> None:
    required = ("data_snapshot_id", "source_schema_ref", "source_schema_sha256", "raw_files")
    if any(key not in inventory for key in required):
        raise ValueError("source inventory is missing required snapshot fields")
    raw_files = inventory["raw_files"]
    if not isinstance(raw_files, list) or not raw_files:
        raise ValueError("source inventory raw_files must be a non-empty list")
    schema_uri = inventory["source_schema_ref"]
    schema_path = _path_from_uri(str(schema_uri))
    if not schema_path.is_file() or sha256_file(schema_path) != inventory["source_schema_sha256"]:
        raise ValueError("source schema reference is missing or its hash changed")
    for entry in raw_files:
        source = _path_from_uri(str(entry["uri"]))
        if not source.is_file():
            raise ValueError(f"frozen source file is missing: {source}")
        if source.stat().st_size != entry["size_bytes"]:
            raise ValueError(f"frozen source file size changed: {source}")
        if verify_hashes and sha256_file(source) != entry["sha256"]:
            raise ValueError(f"frozen source file hash changed: {source}")
    expected_snapshot = data_snapshot_id(
        str(inventory.get("adapter", "")),
        str(inventory["source_schema_sha256"]),
        raw_files,
    )
    if expected_snapshot != inventory["data_snapshot_id"]:
        raise ValueError("source inventory data_snapshot_id does not match its file hashes")
    inventory_payload = {key: value for key, value in inventory.items() if key != "inventory_sha256"}
    expected_inventory_hash = "sha256:" + _identity_hash(inventory_payload)
    if inventory.get("inventory_sha256") != expected_inventory_hash:
        raise ValueError("source inventory content hash does not match its fields")


def _read_template(template: Mapping[str, Any] | str | Path) -> tuple[dict[str, Any], Path]:
    if isinstance(template, Mapping):
        return json.loads(canonical_json(dict(template))), Path.cwd().resolve()
    config_path = Path(template).expanduser().resolve()
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("configuration root must be a mapping")
    return raw, config_path.parent


def _absolutize_config_paths(config: dict[str, Any], base_dir: Path) -> None:
    for section_name, names in {
        "storage": ("artifacts_uri", "registry_uri", "temporary_uri", "preflight_uri"),
        "resources": ("temporary_uri",),
        "calendar": ("data_root", "evidence_path", "frozen_snapshot_path", "ca_bundle"),
        "sessions": ("evidence_path",),
        "input": ("source_schema_path", "frozen_calendar_snapshot_path"),
    }.items():
        section = config.get(section_name)
        if not isinstance(section, dict):
            continue
        for name in names:
            value = section.get(name)
            if isinstance(value, str) and value:
                section[name] = str(_resolve_path(value, base_dir))


def prepare_onboarding_config(
    template: Mapping[str, Any] | str | Path,
    *,
    preflight_root: str | Path | None = None,
) -> dict[str, Any]:
    """Freeze raw input hashes and a research-only split before WorkflowEngine startup."""
    config, base_dir = _read_template(template)
    _absolutize_config_paths(config, base_dir)
    input_config = config.get("input")
    if not isinstance(input_config, dict) or input_config.get("adapter") != "l2_snapshots":
        raise ValueError("preflight currently supports the l2_snapshots adapter")
    timezone_name = input_config.get("source_timezone")
    if not isinstance(timezone_name, str) or not timezone_name:
        raise ValueError("input.source_timezone must be explicit")
    try:
        ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown input.source_timezone {timezone_name!r}") from exc
    schema_value = input_config.get("source_schema_path", "docs/references/gov_bond_l2.schema.json")
    schema_path = _resolve_path(schema_value, base_dir)
    source_schema = load_source_schema(schema_path)
    if "TRADINGTIME" not in source_schema.names or not all(
        f"{side}PRICE{level:02d}" in source_schema.names
        for side in ("BUY", "SELL")
        for level in range(1, 6)
    ):
        raise ValueError("L2 source schema is missing required event-time or five-level book fields")
    sources = resolve_source_paths(input_config, base_dir=base_dir)
    resources = config.get("resources", {})
    memory_limit = str(resources.get("preflight_memory_limit", "1GB"))
    threads = int(resources.get("preflight_threads", min(int(resources.get("threads", 2)), 2)))
    scratch_root = Path(tempfile.gettempdir()) / "quantsage-onboarding-preflight"
    raw_files = [
        inspect_source_file(
            path,
            source_schema,
            memory_limit=memory_limit,
            threads=threads,
            temporary_root=scratch_root,
        )
        for path in sources
    ]
    footer_total = sum(item["row_count"] for item in raw_files)
    all_dates = sorted({value for item in raw_files for value in item["trade_dates"]})
    date_file_counts: dict[str, int] = {}
    provenance_pair_file_counts: dict[tuple[str, str], int] = {}
    for item in raw_files:
        for trade_date in item["trade_dates"]:
            date_file_counts[trade_date] = date_file_counts.get(trade_date, 0) + 1
        for pair in item["source_provenance_pairs"]:
            key = (pair["source_file"], pair["ingest_run_id"])
            provenance_pair_file_counts[key] = provenance_pair_file_counts.get(key, 0) + 1
    instruments = sorted({value for item in raw_files for value in item["instrument_ids"]})
    exchanges = sorted({value for item in raw_files for value in item["exchanges"]})
    products = sorted({value for item in raw_files for value in item["products"]})
    snapshot_id = data_snapshot_id("l2_snapshots", source_schema.sha256, raw_files)
    date_range = {
        "start": all_dates[0] if all_dates else None,
        "end": all_dates[-1] if all_dates else None,
    }
    split_content = {
        "schema_version": "1.0.0",
        "method": "unassigned_research_only",
        "status": "UNASSIGNED",
        "source_data_snapshot_id": snapshot_id,
        "source_date_range": date_range,
        "train": None,
        "development": None,
        "final_holdout": None,
        "fitting_allowed": False,
        "hypothesis_stages_allowed": False,
        "allowed_stages": ["bootstrap", "onboard"],
        "notes": [
            "No research split has been assigned; this artifact only supports raw-data onboarding.",
            "Do not fit baselines or run hypothesis/holdout stages under this split.",
        ],
    }
    split_id = "split_" + _identity_hash(split_content)
    split_manifest = {**split_content, "split_id": split_id}
    inventory = {
        "schema_version": "1.0.0",
        "stage_name": "bootstrap",
        "data_snapshot_id": snapshot_id,
        "source_schema_ref": schema_path.as_uri(),
        "source_schema_sha256": source_schema.sha256,
        "source_schema_version": source_schema.version,
        "adapter": "l2_snapshots",
        "source_timezone": timezone_name,
        "raw_files": raw_files,
        "input_row_count": footer_total,
        "source_date_range": date_range,
        "source_dates": all_dates,
        "instrument_ids": instruments,
        "exchanges": exchanges,
        "products": products,
        "source_file_intervals_disjoint": all(
            set(left["trade_dates"]).isdisjoint(right["trade_dates"])
            for index, left in enumerate(raw_files)
            for right in raw_files[index + 1 :]
        ),
        "overlapping_trade_dates": sorted(
            trade_date for trade_date, count in date_file_counts.items() if count > 1
        ),
        "source_provenance_pairs_disjoint": all(count <= 1 for count in provenance_pair_file_counts.values()),
    }
    inventory["inventory_sha256"] = "sha256:" + _identity_hash(inventory)

    storage = config.setdefault("storage", {})
    default_artifact_root = Path(storage.get("artifacts_uri", "artifacts")).expanduser()
    if not default_artifact_root.is_absolute():
        default_artifact_root = (base_dir / default_artifact_root).resolve()
    storage["artifacts_uri"] = str(default_artifact_root)
    root = Path(preflight_root).expanduser().resolve() if preflight_root else Path(
        storage.get("preflight_uri", default_artifact_root / "preflight")
    ).expanduser()
    if not root.is_absolute():
        root = (base_dir / root).resolve()
    input_config["paths"] = [str(path) for path in sources]
    input_config.pop("path_glob", None)
    input_config["source_schema_path"] = str(schema_path)
    config["data_snapshot_id"] = snapshot_id
    config["splits"] = {**config.get("splits", {}), "split_id": split_id}
    for section_name, path_key in (("calendar", "evidence_path"), ("sessions", "evidence_path")):
        section = config.get(section_name)
        if isinstance(section, dict) and section.get(path_key):
            evidence_path = Path(section[path_key]).expanduser().resolve()
            if not evidence_path.is_file():
                raise ValueError(f"configured {section_name}.{path_key} is missing: {evidence_path}")
            section[path_key] = str(evidence_path)
            section["evidence_sha256"] = sha256_file(evidence_path)
    config["bootstrap"] = {**config.get("bootstrap", {})}
    preflight_fingerprint = _identity_hash(config)
    snapshot_dir = root / snapshot_id / preflight_fingerprint
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    inventory_path = snapshot_dir / "source_inventory.json"
    split_path = snapshot_dir / "split_manifest.json"
    inventory_text = canonical_json(inventory) + "\n"
    split_text = canonical_json(split_manifest) + "\n"
    inventory_hash = "sha256:" + hashlib.sha256(inventory_text.encode("utf-8")).hexdigest()
    split_hash = "sha256:" + hashlib.sha256(split_text.encode("utf-8")).hexdigest()
    _atomic_write(inventory_path, inventory_text)
    _atomic_write(split_path, split_text)
    config["bootstrap"].update(
        {
            "source_inventory_path": str(inventory_path),
            "source_inventory_sha256": inventory_hash,
            "split_manifest_path": str(split_path),
            "split_manifest_sha256": split_hash,
            "preflight_fingerprint": preflight_fingerprint,
            "resolved_config_path": str(snapshot_dir / "resolved.yaml"),
        }
    )
    _atomic_write(snapshot_dir / "resolved.yaml", yaml.safe_dump(config, sort_keys=False))
    return config


def _atomic_write(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(contents)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
    except Exception:
        Path(temporary_name).unlink(missing_ok=True)
        raise
