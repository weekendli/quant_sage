"""Independently validate a published Step 02 onboarding entry and its artifacts.

The verifier reads only the entry, its referenced output artifacts, the referenced
source schema, and optional frozen evidence refs. It never opens raw market data or
the live Marketdata catalog. Large files are hashed in chunks and inspected with
Arrow/DuckDB under a configurable resource budget (default: 1 GB / 4 threads).
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import math
import os
import re
import tempfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, timedelta
from datetime import time as clock_time
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from itertools import pairwise
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
HASH_RE = re.compile(r"^sha256:([0-9a-f]{64})$")
DUCKDB_MEMORY_LIMIT_RE = re.compile(r"^[1-9][0-9]*\s*(?:B|KB|MB|GB|TB|PB)$", re.IGNORECASE)
RECORD_COLUMNS = {
    "record_id": pa.string(),
    "raw_file_sha256": pa.string(),
    "raw_row_index": pa.int64(),
    "raw_row_group_index": pa.int32(),
}
QUOTE_DERIVED_COLUMNS = {
    **RECORD_COLUMNS,
    "session_id": pa.string(),
    "available_time": pa.timestamp("us", tz="UTC"),
    "quality_flags": pa.list_(pa.string()),
}
QUARANTINE_DERIVED_COLUMNS = {
    **RECORD_COLUMNS,
    "reason_codes": pa.list_(pa.string()),
}
QUOTE_CORE_COLUMNS = {
    "exchange": pa.string(),
    "instrument_id": pa.string(),
    "trade_date": pa.date32(),
    "event_time": pa.timestamp("us", tz="UTC"),
    "source_event_unix_ms": pa.int64(),
    "bid_price_1": pa.decimal128(9, 3),
    "ask_price_1": pa.decimal128(9, 3),
    "bid_size_1": pa.decimal128(19, 2),
    "ask_size_1": pa.decimal128(19, 2),
}
QUARANTINE_REASON_CODES = (
    "MISSING_REQUIRED_FIELD", "NON_TRADING_DATE", "EVENT_DATE_MISMATCH",
    "UNIX_TIME_MISMATCH", "SESSION_UNCLASSIFIED", "INVALID_L1_BOOK",
    "DUPLICATE_EVENT_KEY_IDENTICAL", "DUPLICATE_EVENT_KEY_CONFLICTING",
    "DUPLICATE_SOURCE_RECORD",
)
LEGACY_CHECKPOINT_SCANNER_REVISIONS = {
    # Full verifier revision used by the completed r2 scan. Changes after it
    # only fix source-date aggregation; scan functions and their dependencies
    # are unchanged at this fingerprint.
    "sha256:426b5eb219f9bc4e3c548ee19ba7cdd9bae8b7b1877c968226736645c8de550f":
        "sha256:1fb64d23c7f6fb00ec31de43483571382b0f2349f8b98272c196043a91a8433a",
}


class VerificationError(ValueError):
    """An onboarding entry or one of its referenced artifacts is invalid."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def hash_path(path: Path) -> str:
    """Match the workflow engine's content hash for files and directory bundles."""
    if path.is_file():
        return sha256_file(path)
    if not path.is_dir():
        raise VerificationError(f"artifact path does not exist: {path}")
    digest = hashlib.sha256()
    for child in sorted(path.rglob("*")):
        if child.is_symlink():
            raise VerificationError(f"artifact bundle contains a symlink: {child}")
        if not child.is_file():
            continue
        relative = child.relative_to(path).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(bytes.fromhex(sha256_file(child).removeprefix("sha256:")))
    return "sha256:" + digest.hexdigest()


def payload_size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(child.stat().st_size for child in path.rglob("*") if child.is_file())


def file_uri_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise VerificationError(f"only local file artifact URIs are supported: {uri!r}")
    raw_path = Path(unquote(parsed.path)).expanduser()
    if raw_path.is_symlink():
        raise VerificationError(f"artifact URI resolves through a symlink: {uri!r}")
    path = raw_path.resolve()
    if not path.exists():
        raise VerificationError(f"referenced file is missing: {uri}")
    return path


def _safe_json(path: Path, what: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise VerificationError(f"{what} is not readable JSON: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise VerificationError(f"{what} must be a JSON object: {path}")
    return value


def _hash_text(value: Any, context: str) -> str:
    if not isinstance(value, str) or not HASH_RE.fullmatch(value):
        raise VerificationError(f"{context} must be sha256:<64 lowercase hex>")
    return value


def _resolve_schema_ref(entry_path: Path, schema_ref: str) -> Path:
    parsed = urlparse(schema_ref)
    if parsed.scheme == "file":
        return file_uri_path(schema_ref)
    path = Path(schema_ref).expanduser()
    candidates = [path] if path.is_absolute() else [entry_path.parent / path, REPO_ROOT / path]
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    raise VerificationError(f"referenced source schema is missing: {schema_ref}")


def _arrow_type(type_name: str) -> pa.DataType:
    value = type_name.strip().lower()
    if value in {"string", "utf8"}:
        return pa.string()
    if value in {"large_string", "large_utf8"}:
        return pa.large_string()
    if value == "date32[day]":
        return pa.date32()
    if value == "timestamp[us]":
        return pa.timestamp("us")
    if value == "timestamp[us, utc]":
        return pa.timestamp("us", tz="UTC")
    if value in {"int64", "int32", "int16", "int8"}:
        return {"int64": pa.int64, "int32": pa.int32, "int16": pa.int16, "int8": pa.int8}[value]()
    if value in {"uint64", "uint32", "uint16", "uint8"}:
        return {"uint64": pa.uint64, "uint32": pa.uint32, "uint16": pa.uint16, "uint8": pa.uint8}[value]()
    decimal_match = re.fullmatch(r"decimal128\((\d+)\s*,\s*(\d+)\)", value)
    if decimal_match:
        return pa.decimal128(int(decimal_match.group(1)), int(decimal_match.group(2)))
    if value in {"float", "float32"}:
        return pa.float32()
    if value in {"double", "float64"}:
        return pa.float64()
    if value in {"bool", "boolean"}:
        return pa.bool_()
    raise VerificationError(f"unrecognized source schema Arrow type: {type_name!r}")


def _check_schema(path: Path, expected: Mapping[str, pa.DataType], kind: str) -> None:
    try:
        parquet = pq.ParquetFile(path)
        schema = parquet.schema_arrow
    except Exception as exc:
        raise VerificationError(f"cannot read {kind} Parquet schema/footer: {path}: {exc}") from exc
    names = schema.names
    duplicates = sorted(name for name, count in Counter(names).items() if count > 1)
    if duplicates:
        raise VerificationError(f"{kind} Parquet schema has ambiguous duplicate fields: {duplicates}")
    fields = {field.name: field for field in schema}
    missing = sorted(set(expected) - set(fields))
    extra = sorted(set(fields) - set(expected))
    if missing:
        raise VerificationError(f"{kind} Parquet schema is missing fields: {missing}")
    if extra:
        raise VerificationError(f"{kind} Parquet schema has unexpected fields: {extra}")
    wrong = {
        name: {"expected": str(expected_type), "actual": str(fields[name].type)}
        for name, expected_type in expected.items()
        if fields[name].type != expected_type
    }
    if wrong:
        raise VerificationError(f"{kind} Parquet schema has incompatible field types: {wrong}")
    return parquet


def _load_source_schema(entry_path: Path, inputs: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, pa.DataType]]:
    schema_ref = inputs.get("schema_ref")
    if not isinstance(schema_ref, str) or not schema_ref:
        raise VerificationError("inputs.schema_ref is required")
    schema_path = _resolve_schema_ref(entry_path, schema_ref)
    expected_hash = _hash_text(inputs.get("schema_sha256"), "inputs.schema_sha256")
    actual_hash = sha256_file(schema_path)
    if actual_hash != expected_hash:
        raise VerificationError(f"source schema hash mismatch: expected {expected_hash}, found {actual_hash}")
    schema = _safe_json(schema_path, "source schema")
    fields = schema.get("sample", {}).get("fields")
    if not isinstance(fields, list) or not fields:
        raise VerificationError("source schema must define sample.fields")
    mapping = inputs.get("mapping")
    if not isinstance(mapping, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) or not key or not value
        for key, value in mapping.items()
    ):
        raise VerificationError("inputs.mapping must map physical source names to standardized names")
    source_names = [field.get("name") for field in fields if isinstance(field, dict)]
    if len(source_names) != len(fields) or any(not isinstance(name, str) for name in source_names):
        raise VerificationError("source schema fields must have unique string names")
    unknown_map = sorted(set(mapping) - set(source_names))
    if unknown_map:
        raise VerificationError(f"mapping references fields absent from source schema: {unknown_map}")
    target_names = [mapping.get(name, name) for name in source_names]
    if len(target_names) != len(set(target_names)):
        raise VerificationError("inputs.mapping creates ambiguous standardized field names")
    source_types = {field["name"]: _arrow_type(field["arrow_type"]) for field in fields}
    output_types = {mapping.get(name, name): source_types[name] for name in source_names}
    if output_types.get("event_time") is not None:
        output_types["event_time"] = pa.timestamp("us", tz="UTC")
    if output_types.get("trade_date") is not None:
        output_types["trade_date"] = pa.date32()
    if output_types.get("source_event_unix_ms") is not None:
        output_types["source_event_unix_ms"] = pa.int64()
    return schema, output_types


def _manifest_semantics(manifest: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "artifact_id", "artifact_type", "schema_version", "uri", "content_hash", "manifest_uri",
        "run_id", "stage_run_id", "stage_name", "stage_version", "cache_key", "config_hash",
        "code_revision", "input_artifact_ids", "data_snapshot_id", "split_id", "partition_keys",
        "time_min", "time_max", "row_count", "quality_flags",
    )
    return {key: manifest.get(key) for key in keys}


def _ref_semantics(ref: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "artifact_id", "artifact_type", "schema_version", "uri", "content_hash", "manifest_uri",
        "run_id", "stage_run_id", "stage_name", "stage_version", "cache_key", "partition_keys",
        "time_min", "time_max", "row_count",
    )
    return {key: ref.get(key) for key in keys}


def _compare_ref_manifest(ref: Mapping[str, Any], manifest: Mapping[str, Any]) -> None:
    """Compare fields mirrored by ArtifactRef, then validate manifest-only lineage separately."""
    ref_fields = _ref_semantics(ref)
    manifest_fields = _manifest_semantics(manifest)
    mismatched = [key for key, value in ref_fields.items() if manifest_fields.get(key) != value]
    if mismatched:
        raise VerificationError(
            f"ArtifactRef and sidecar manifest disagree for {ref.get('artifact_id')}: {mismatched}"
        )


def _validate_manifest_structure(manifest: Mapping[str, Any], context: str) -> None:
    required_text = (
        "artifact_id", "artifact_type", "schema_version", "uri", "content_hash", "manifest_uri",
        "run_id", "stage_run_id", "stage_name", "stage_version", "cache_key", "config_hash",
        "code_revision", "created_at",
    )
    if any(not isinstance(manifest.get(key), str) or not manifest[key] for key in required_text):
        raise VerificationError(f"{context} is missing required common manifest strings")
    _hash_text(manifest["content_hash"], f"{context}.content_hash")
    for key in ("data_snapshot_id", "split_id"):
        if manifest.get(key) is not None and not isinstance(manifest[key], str):
            raise VerificationError(f"{context}.{key} must be a string or null")
    input_ids = manifest.get("input_artifact_ids")
    if not isinstance(input_ids, list) or any(not isinstance(item, str) for item in input_ids):
        raise VerificationError(f"{context}.input_artifact_ids must be a list of strings")
    flags = manifest.get("quality_flags")
    if not isinstance(flags, list) or any(not isinstance(item, str) for item in flags):
        raise VerificationError(f"{context}.quality_flags must be a list of strings")
    partitions = manifest.get("partition_keys")
    if not isinstance(partitions, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in partitions.items()
    ):
        raise VerificationError(f"{context}.partition_keys must be a string mapping")
    row_count = manifest.get("row_count")
    if row_count is not None and (
        isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0
    ):
        raise VerificationError(f"{context}.row_count must be a non-negative integer or null")
    time_min, time_max = manifest.get("time_min"), manifest.get("time_max")
    if (time_min is None) != (time_max is None):
        raise VerificationError(f"{context} must set both time bounds or neither")
    if time_min is not None:
        minimum = _parse_datetime(time_min, f"{context}.time_min")
        maximum = _parse_datetime(time_max, f"{context}.time_max")
        if minimum > maximum:
            raise VerificationError(f"{context}.time_min is after time_max")


def _flatten_embedded_manifests(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        output: list[dict[str, Any]] = []
        for item in value:
            output.extend(_flatten_embedded_manifests(item))
        return output
    if isinstance(value, dict):
        if isinstance(value.get("artifact_id"), str) and isinstance(value.get("uri"), str):
            return [value]
        output = []
        for item in value.values():
            output.extend(_flatten_embedded_manifests(item))
        return output
    return []


def _all_refs(artifacts: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    refs: dict[str, list[dict[str, Any]]] = {}
    for alias in ("quotes", "quarantine"):
        value = artifacts.get(alias)
        if not isinstance(value, list) or not value:
            raise VerificationError(f"artifacts.{alias} must be a non-empty list of ArtifactRefs")
        if any(not isinstance(ref, dict) for ref in value):
            raise VerificationError(f"artifacts.{alias} contains a non-object ref")
        refs[alias] = value
    for alias in ("calendar", "session_rules"):
        value = artifacts.get(alias)
        if not isinstance(value, dict):
            raise VerificationError(f"artifacts.{alias} must be one ArtifactRef object")
        refs[alias] = [value]
    return refs


def _verify_ref(ref: Mapping[str, Any], entry: Mapping[str, Any]) -> tuple[Path, dict[str, Any]]:
    required = ("artifact_id", "artifact_type", "schema_version", "uri", "content_hash", "manifest_uri")
    for key in required:
        if not isinstance(ref.get(key), str) or not ref[key]:
            raise VerificationError(f"ArtifactRef missing required {key}")
    if not isinstance(ref.get("partition_keys", {}), dict):
        raise VerificationError("ArtifactRef.partition_keys must be an object")
    content_hash = _hash_text(ref["content_hash"], "ArtifactRef.content_hash")
    path = file_uri_path(ref["uri"])
    actual_hash = hash_path(path)
    if actual_hash != content_hash:
        raise VerificationError(
            f"artifact content hash mismatch for {ref['artifact_id']}: expected {content_hash}, found {actual_hash}"
        )
    expected_id = "art_" + content_hash.removeprefix("sha256:")
    if ref["artifact_id"] != expected_id:
        raise VerificationError(f"artifact_id is not content-addressed: {ref['artifact_id']}")
    manifest_path = file_uri_path(ref["manifest_uri"])
    manifest = _safe_json(manifest_path, "artifact manifest")
    _validate_manifest_structure(manifest, "artifact manifest")
    _compare_ref_manifest(ref, manifest)
    if manifest.get("artifact_id") != expected_id or manifest.get("content_hash") != actual_hash:
        raise VerificationError(f"artifact manifest content identity mismatch for {ref['artifact_id']}")
    for key in ("run_id", "stage_run_id", "stage_name", "config_hash", "code_revision",
                "data_snapshot_id", "split_id"):
        expected = entry.get(key)
        if expected is not None and manifest.get(key) != expected:
            raise VerificationError(
                f"artifact {ref['artifact_id']} {key} does not match onboarding entry"
            )
    if manifest.get("stage_name") != "onboard":
        raise VerificationError(f"artifact {ref['artifact_id']} was not produced by onboard")
    return path, manifest


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@lru_cache(maxsize=1)
def _checkpoint_scanner_revision() -> str:
    """Fingerprint per-file scan code, policy setup, and schema contracts."""
    function_names = (
        "_scan_quote_file", "_assert_sorted_quote_file", "_scan_quarantine_file",
        "_depth_invalid_expression", "_register_input_file_policy", "_parquet_paths",
        "_check_schema", "_validate_input_evidence", "_validate_session_rules",
        "_validate_partition_session_coverage", "_quarantine_session_partitions",
        "_expiry_date", "_register_session_intervals", "_sql_ident",
    )
    functions = {name: inspect.getsource(globals()[name]) for name in function_names}
    schemas = {}
    for name in (
        "RECORD_COLUMNS", "QUOTE_DERIVED_COLUMNS", "QUARANTINE_DERIVED_COLUMNS",
        "QUOTE_CORE_COLUMNS",
    ):
        schemas[name] = {field_name: str(dtype)
                         for field_name, dtype in globals()[name].items()}
    contracts = {
        "schemas": schemas,
        "quarantine_reason_codes": list(QUARANTINE_REASON_CODES),
        "runtime_versions": {"duckdb": duckdb.__version__, "pyarrow": pa.__version__},
    }
    return _canonical_hash({
        "scan_schema_version": "1.0.0",
        "functions": functions,
        "contracts": contracts,
    })


def _legacy_checkpoint_binding_compatible(
    old_binding: Any, current_binding: Mapping[str, Any]
) -> bool:
    if not isinstance(old_binding, dict):
        return False
    old_verifier_revision = old_binding.get("verifier_revision")
    expected_scanner_revision = LEGACY_CHECKPOINT_SCANNER_REVISIONS.get(
        old_verifier_revision
    )
    if (expected_scanner_revision is None
            or expected_scanner_revision != current_binding.get("scanner_revision")):
        return False
    old_inputs = {key: value for key, value in old_binding.items()
                  if key != "verifier_revision"}
    current_inputs = {key: value for key, value in current_binding.items()
                      if key != "scanner_revision"}
    return old_inputs == current_inputs


def _checkpoint_binding(
    entry_file: Path,
    entry_manifest: Mapping[str, Any] | None,
    refs_by_alias: Mapping[str, list[dict[str, Any]]],
    sidecars: Mapping[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    artifacts = []
    for alias, refs in sorted(refs_by_alias.items()):
        for ref in sorted(refs, key=lambda item: (item["artifact_id"], item["uri"])):
            sidecar = sidecars[(ref["artifact_id"], ref["uri"])]
            artifacts.append({
                "alias": alias,
                "artifact_id": ref["artifact_id"],
                "uri": ref["uri"],
                "content_hash": ref["content_hash"],
                "manifest_uri": ref["manifest_uri"],
                "manifest_hash": _canonical_hash(sidecar),
            })
    return {
        "schema_version": "1.0.0",
        "entry_uri": entry_file.as_uri(),
        "entry_hash": sha256_file(entry_file),
        "outer_entry_manifest_hash": _canonical_hash(entry_manifest) if entry_manifest is not None else None,
        "scanner_revision": _checkpoint_scanner_revision(),
        "artifacts": artifacts,
    }


def _checkpoint_key(kind: str, ref: Mapping[str, Any]) -> str:
    return _canonical_hash({"kind": kind, "artifact_id": ref["artifact_id"], "uri": ref["uri"]})


def _checkpoint_json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _checkpoint_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_checkpoint_json_value(item) for item in value]
    return value


def _open_verification_checkpoint(
    checkpoint_path: str | Path | None,
    binding: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], Any | None, str]:
    """Load scan records only when inputs and per-file scan semantics match."""
    if checkpoint_path is None:
        return {}, None, "DISABLED"
    path = Path(checkpoint_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    records: dict[str, dict[str, Any]] = {}
    status = "CREATED"
    if path.is_file():
        try:
            with path.open(encoding="utf-8") as stream:
                header = json.loads(next(stream))
                header_binding = header.get("binding")
                valid_header = (
                    header.get("record_type") == "header"
                    and isinstance(header_binding, dict)
                    and header.get("binding_hash") == _canonical_hash(header_binding)
                )
                exact_binding = valid_header and header_binding == binding
                compatible_legacy = (
                    valid_header and _legacy_checkpoint_binding_compatible(header_binding, binding)
                )
                if exact_binding or compatible_legacy:
                    status = "RESUMED" if exact_binding else "RESUMED_COMPATIBLE_REVISION"
                    for line in stream:
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError:
                            break  # a process interruption may leave one incomplete last line
                        if (not isinstance(record, dict) or record.get("record_type") != "scan"
                                or record.get("kind") not in {"quote", "quarantine"}
                                or not isinstance(record.get("key"), str)
                                or not isinstance(record.get("artifact_id"), str)
                                or not isinstance(record.get("uri"), str)
                                or not isinstance(record.get("content_hash"), str)
                                or not isinstance(record.get("result"), dict)
                                or record.get("record_hash") != _canonical_hash({
                                    key: value for key, value in record.items() if key != "record_hash"
                                })):
                            continue
                        expected_key = _checkpoint_key(record["kind"], {
                            "artifact_id": record["artifact_id"], "uri": record["uri"]
                        })
                        if record["key"] == expected_key:
                            records[expected_key] = record
                else:
                    status = "INVALIDATED_BINDING"
        except (OSError, StopIteration, json.JSONDecodeError, AttributeError):
            status = "INVALIDATED_FORMAT"
    if status not in {"RESUMED", "RESUMED_COMPATIBLE_REVISION"}:
        temporary = path.with_name(path.name + ".tmp")
        header = {
            "record_type": "header", "binding": binding,
            "binding_hash": _canonical_hash(binding),
        }
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write(json.dumps(header,
                                    sort_keys=True, separators=(",", ":")) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        records = {}
    return records, path.open("a", encoding="utf-8"), status


def _append_checkpoint_record(
    stream: Any | None,
    records: dict[str, dict[str, Any]],
    kind: str,
    ref: Mapping[str, Any],
    result: Mapping[str, Any],
) -> None:
    if stream is None:
        return
    key = _checkpoint_key(kind, ref)
    record = {
        "record_type": "scan", "key": key, "kind": kind,
        "artifact_id": ref["artifact_id"], "uri": ref["uri"],
        "content_hash": ref["content_hash"],
        "result": _checkpoint_json_value(dict(result)),
    }
    record["record_hash"] = _canonical_hash(record)
    stream.write(json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n")
    stream.flush()
    records[key] = record


def _checkpoint_scan(
    records: Mapping[str, Mapping[str, Any]], kind: str, ref: Mapping[str, Any]
) -> dict[str, Any] | None:
    record = records.get(_checkpoint_key(kind, ref))
    if record is None or record.get("content_hash") != ref.get("content_hash"):
        return None
    return dict(record["result"])


def _new_identity_bitmaps(raw_files: list[dict[str, Any]]) -> tuple[dict[str, np.ndarray], dict[str, int]]:
    expected = {
        item["sha256"].removeprefix("sha256:"): int(item["row_count"])
        for item in raw_files
    }
    bitmaps = {
        raw_hash: np.zeros((row_count + 7) // 8, dtype=np.uint8)
        for raw_hash, row_count in expected.items()
    }
    return bitmaps, expected


def _consume_batch_identity_rows(
    connection: duckdb.DuckDBPyConnection,
    bitmaps: Mapping[str, np.ndarray],
    expected: Mapping[str, int],
    rows_seen: Counter[str],
) -> bool:
    """Prove each `(source hash, source row index)` appears once across all date batches."""
    cursor = connection.execute(
        "SELECT raw_file_sha256,raw_row_index FROM _consumer_batch_rows "
        "ORDER BY raw_file_sha256,raw_row_index"
    )
    reader = cursor.to_arrow_reader(65_536)
    bad_identity = False
    for batch in reader:
        hashes = batch.column(0)
        row_indexes = batch.column(1)
        if hashes.null_count or row_indexes.null_count:
            bad_identity = True
            continue
        encoded = pc.dictionary_encode(hashes)
        codes = encoded.indices.to_numpy(zero_copy_only=False)
        index_values = row_indexes.to_numpy(zero_copy_only=False).astype(np.int64, copy=False)
        if not len(codes):
            continue
        boundaries = np.concatenate((
            np.array([0], dtype=np.int64),
            np.flatnonzero(codes[1:] != codes[:-1]).astype(np.int64) + 1,
            np.array([len(codes)], dtype=np.int64),
        ))
        dictionary = encoded.dictionary.to_pylist()
        for start, end in pairwise(boundaries):
            raw_hash = str(dictionary[int(codes[start])])
            indexes = index_values[start:end]
            rows_seen[raw_hash] += len(indexes)
            bitmap = bitmaps.get(raw_hash)
            limit = expected.get(raw_hash)
            if bitmap is None or limit is None:
                bad_identity = True
                continue
            valid = (indexes >= 0) & (indexes < limit)
            if not np.all(valid):
                bad_identity = True
                indexes = indexes[valid]
            if len(indexes) > 1 and np.any(indexes[1:] == indexes[:-1]):
                bad_identity = True
            byte_indexes = indexes >> 3
            masks = np.left_shift(np.uint8(1), (indexes & 7).astype(np.uint8, copy=False))
            if np.any(np.bitwise_and(bitmap[byte_indexes], masks)):
                bad_identity = True
            np.bitwise_or.at(bitmap, byte_indexes, masks)
    return bad_identity


def _verify_embedded_manifests(embedded: Any, refs: Iterable[Mapping[str, Any]],
                               sidecars: Mapping[tuple[str, str], Mapping[str, Any]]) -> None:
    manifests = _flatten_embedded_manifests(embedded)
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for manifest in manifests:
        _validate_manifest_structure(manifest, "embedded artifact manifest")
        key = (manifest.get("artifact_id"), manifest.get("uri"))
        if key[0] is None or key[1] is None or key in by_key:
            raise VerificationError("artifact_manifests contains a missing or duplicate (artifact_id, uri)")
        by_key[key] = manifest
    expected_refs = {(ref["artifact_id"], ref["uri"]) for ref in refs}
    if set(by_key) != expected_refs:
        missing = sorted(expected_refs - by_key.keys())
        extra = sorted(by_key.keys() - expected_refs)
        raise VerificationError(f"embedded artifact manifest set differs from refs; missing={missing}, extra={extra}")
    for key, embedded_manifest in by_key.items():
        sidecar = sidecars[key]
        if _manifest_semantics(embedded_manifest) != _manifest_semantics(sidecar):
            raise VerificationError(f"embedded and sidecar manifests disagree for {key[0]}")


def _parquet_paths(path: Path) -> list[Path]:
    if path.is_file():
        if path.suffix.lower() != ".parquet":
            raise VerificationError(f"expected a Parquet artifact: {path}")
        return [path]
    paths = sorted(p for p in path.rglob("*.parquet") if p.is_file())
    if not paths:
        raise VerificationError(f"Parquet artifact bundle contains no .parquet files: {path}")
    return paths


def _manifest_row_count(ref: Mapping[str, Any], actual_rows: int, what: str) -> None:
    if isinstance(ref.get("row_count"), bool) or not isinstance(ref.get("row_count"), int):
        raise VerificationError(f"{what} ArtifactRef.row_count must be an integer")
    if ref["row_count"] != actual_rows:
        raise VerificationError(f"{what} row_count mismatch: manifest={ref['row_count']}, files={actual_rows}")


def _date_text(value: Any) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _parse_datetime(value: Any, what: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        try:
            result = datetime.fromisoformat(str(value))
        except ValueError as exc:
            raise VerificationError(f"invalid {what} timestamp {value!r}") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise VerificationError(f"{what} timestamp must be timezone-aware UTC: {value!r}")
    if result.utcoffset().total_seconds() != 0:
        raise VerificationError(f"{what} timestamp must be UTC: {value!r}")
    return result.astimezone(UTC)


def _compare_time_bounds(ref: Mapping[str, Any], actual_min: Any, actual_max: Any, what: str) -> None:
    for field, actual in (("time_min", actual_min), ("time_max", actual_max)):
        expected = ref.get(field)
        if expected is None:
            raise VerificationError(f"{what} manifest is missing {field}")
        if _parse_datetime(expected, f"{what}.{field}") != _parse_datetime(actual, f"actual {what}.{field}"):
            raise VerificationError(f"{what} {field} mismatch: manifest={expected}, actual={actual}")


def _compare_partition_time_bounds(partition: Mapping[str, Any], actual_min: Any,
                                   actual_max: Any, what: str) -> None:
    for field, actual in (("event_time_min", actual_min), ("event_time_max", actual_max)):
        expected = partition.get(field)
        if expected is None:
            raise VerificationError(f"{what} is missing {field}")
        if _parse_datetime(expected, f"{what}.{field}") != _parse_datetime(actual, f"actual {what}.{field}"):
            raise VerificationError(f"{what} {field} mismatch: manifest={expected}, actual={actual}")


def _depth_invalid_expression(alias: str, *, standardized: bool) -> str:
    source_prefixes = {
        "bid_price": "BUYPRICE", "ask_price": "SELLPRICE",
        "bid_size": "BUYVOLUME", "ask_size": "SELLVOLUME",
    }
    invalid_terms = []
    for level in range(2, 6):
        values = {}
        for field, source_prefix in source_prefixes.items():
            name = f"{field}_{level}" if standardized else f"{source_prefix}{level:02}"
            values[field] = f"{alias}.{_sql_ident(name)}"
        invalid_terms.extend(f"{values[field]} IS NULL" for field in values)
        invalid_terms.extend((
            f"{values['bid_price']} <= 0", f"{values['ask_price']} <= 0",
            f"{values['bid_size']} < 0", f"{values['ask_size']} < 0",
        ))
    return "(" + " OR ".join(invalid_terms) + ")"


def _scan_quote_file(connection: duckdb.DuckDBPyConnection, path: Path,
                     source_timezone: str, expected_exchange: str,
                     expected_instrument: str, expected_trade_date: str,
                     calendar_id: str) -> dict[str, Any]:
    depth_invalid = _depth_invalid_expression("q", standardized=True)
    query = f"""
        SELECT count(*) AS rows,
               min(q.event_time) AS time_min,
               max(q.event_time) AS time_max,
               count(*) FILTER (WHERE q.available_time IS NOT NULL) AS available_rows,
               count(*) FILTER (WHERE q.exchange IS NULL OR q.instrument_id IS NULL OR q.trade_date IS NULL
                                 OR q.event_time IS NULL OR q.session_id IS NULL) AS missing_identity_rows,
               count(*) FILTER (WHERE q.bid_price_1 IS NULL OR q.ask_price_1 IS NULL
                                 OR q.bid_size_1 IS NULL OR q.ask_size_1 IS NULL) AS missing_l1_rows,
               count(*) FILTER (WHERE q.bid_price_1 <= 0 OR q.ask_price_1 <= 0
                                 OR q.bid_size_1 < 0 OR q.ask_size_1 < 0
                                 OR q.ask_price_1 < q.bid_price_1) AS invalid_l1_rows,
               count(*) FILTER (WHERE q.quality_flags IS NULL) AS missing_quality_flags,
               count(*) FILTER (WHERE {depth_invalid}) AS depth_invalid_rows,
               count(*) FILTER (WHERE {depth_invalid} <>
                                  list_contains(q.quality_flags, 'DEPTH_BOOK_INVALID'))
                   AS bad_depth_quality_flags,
               count(*) FILTER (WHERE f.raw_file_sha256 IS NULL OR q.raw_row_index IS NULL
                                 OR q.raw_row_index < 0 OR q.raw_row_index >= f.row_count
                                 OR (f.source_format = 'csv' AND q.raw_row_group_index IS NOT NULL)
                                 OR (f.source_format = 'parquet' AND
                                     (q.raw_row_group_index IS NULL OR q.raw_row_group_index < 0
                                      OR q.raw_row_group_index >= f.row_group_count
                                      OR g.raw_file_sha256 IS NULL OR q.raw_row_index < g.row_start
                                      OR q.raw_row_index >= g.row_end))
                                 OR q.record_id IS NULL
                                 OR q.record_id != sha256(q.raw_file_sha256 || ':' || CAST(q.raw_row_index AS VARCHAR)))
                   AS bad_record_ids,
               count(*) FILTER (WHERE q.source_event_unix_ms IS NULL
                                 OR CAST(epoch_ms(q.event_time) AS BIGINT) != CAST(q.source_event_unix_ms AS BIGINT))
                   AS bad_event_unix_rows,
               count(*) FILTER (WHERE CAST(timezone(?, q.event_time) AS DATE) != q.trade_date)
                   AS bad_trade_date_rows,
               count(*) FILTER (WHERE NOT regexp_matches(q.session_id,
                   '^[^:]+:' || CAST(q.trade_date AS VARCHAR) || ':[^:]+$')) AS bad_session_ids,
               count(*) FILTER (WHERE q.instrument_id IS NULL OR q.trade_date IS NULL) AS bad_partition_rows,
               count(*) FILTER (WHERE q.exchange <> ? OR q.instrument_id <> ?
                                 OR q.trade_date <> CAST(? AS DATE)) AS bad_partition_values,
               count(*) FILTER (WHERE NOT starts_with(q.session_id,
                   ? || ':' || CAST(q.trade_date AS VARCHAR) || ':')) AS bad_calendar_session_ids,
               count(*) FILTER (WHERE s.segment_id IS NULL OR q.session_id != s.session_id)
                   AS bad_session_classification_rows
        FROM read_parquet(?) q
        LEFT JOIN _input_file_policy f USING (raw_file_sha256)
        LEFT JOIN _input_group_policy g
          ON g.raw_file_sha256 = q.raw_file_sha256
         AND g.row_group_index = q.raw_row_group_index
        LEFT JOIN _session_intervals s
          ON q.instrument_id = s.instrument_id AND q.trade_date = s.trade_date
         AND CAST(timezone(?, q.event_time) AS TIME) >= s.start_time
         AND CAST(timezone(?, q.event_time) AS TIME) < s.end_time
    """
    row = connection.execute(query, [
        source_timezone, expected_exchange, expected_instrument, expected_trade_date,
        calendar_id, path.as_posix(), source_timezone, source_timezone,
    ]).fetchone()
    return dict(zip((column[0] for column in connection.description), row, strict=True))


def _assert_sorted_quote_file(path: Path, batch_size: int = 65_536) -> None:
    """Check physical quote order in bounded Arrow batches, including batch boundaries."""
    parquet = pq.ParquetFile(path)
    previous: tuple[datetime, str, int] | None = None
    columns = ["event_time", "raw_file_sha256", "raw_row_index"]
    for batch in parquet.iter_batches(batch_size=batch_size, columns=columns):
        event_times = batch.column(0)
        source_hashes = batch.column(1)
        row_indexes = batch.column(2)
        count = batch.num_rows
        if not count:
            continue
        if event_times.null_count or source_hashes.null_count or row_indexes.null_count:
            raise VerificationError(f"quote order keys contain nulls: {path}")
        current_first = (event_times[0].as_py(), source_hashes[0].as_py(), row_indexes[0].as_py())
        if previous is not None and current_first < previous:
            raise VerificationError(f"quote rows are not sorted by event_time and source identity: {path}")
        if count > 1:
            prior_time, next_time = event_times.slice(0, count - 1), event_times.slice(1)
            prior_hash, next_hash = source_hashes.slice(0, count - 1), source_hashes.slice(1)
            prior_row, next_row = row_indexes.slice(0, count - 1), row_indexes.slice(1)
            time_equal = pc.equal(prior_time, next_time)
            hash_equal = pc.equal(prior_hash, next_hash)
            time_decreases = pc.less(next_time, prior_time)
            hash_decreases = pc.less(next_hash, prior_hash)
            row_decreases = pc.less(next_row, prior_row)
            same_time_out_of_order = pc.and_(time_equal, pc.or_(
                hash_decreases, pc.and_(hash_equal, row_decreases)
            ))
            out_of_order = pc.fill_null(pc.or_(time_decreases, same_time_out_of_order), True)
            if pc.any(out_of_order).as_py():
                raise VerificationError(f"quote rows are not sorted by event_time and source identity: {path}")
        previous = (event_times[count - 1].as_py(), source_hashes[count - 1].as_py(),
                    row_indexes[count - 1].as_py())


def _scan_quarantine_file(connection: duckdb.DuckDBPyConnection, path: Path) -> dict[str, Any]:
    depth_invalid = _depth_invalid_expression("q", standardized=False)
    query = f"""
        SELECT count(*) AS rows,
               count(*) FILTER (WHERE {depth_invalid}) AS depth_invalid_rows,
               count(*) FILTER (WHERE f.raw_file_sha256 IS NULL OR raw_row_index IS NULL
                                 OR raw_row_index < 0 OR raw_row_index >= f.row_count
                                 OR (f.source_format = 'csv' AND raw_row_group_index IS NOT NULL)
                                 OR (f.source_format = 'parquet' AND
                                     (raw_row_group_index IS NULL OR raw_row_group_index < 0
                                      OR raw_row_group_index >= f.row_group_count
                                      OR g.raw_file_sha256 IS NULL OR raw_row_index < g.row_start
                                      OR raw_row_index >= g.row_end))
                                 OR record_id IS NULL
                                 OR record_id != sha256(raw_file_sha256 || ':' || CAST(raw_row_index AS VARCHAR)))
                   AS bad_record_ids,
               count(*) FILTER (WHERE reason_codes IS NULL OR len(reason_codes) = 0
                                 OR len(reason_codes) <> len(list_distinct(reason_codes))) AS missing_reason_rows,
               count(*) FILTER (WHERE (TRADINGTIME IS NULL OR TRADINGDATE IS NULL
                                 OR SYMBOL IS NULL OR MARKET IS NULL)
                                 AND NOT list_contains(reason_codes, 'MISSING_REQUIRED_FIELD'))
                   AS missing_source_key_rows
        FROM read_parquet(?) q
        LEFT JOIN _input_file_policy f USING (raw_file_sha256)
        LEFT JOIN _input_group_policy g
          ON g.raw_file_sha256 = q.raw_file_sha256
         AND g.row_group_index = q.raw_row_group_index
    """
    row = connection.execute(query, [path.as_posix()]).fetchone()
    return dict(zip((column[0] for column in connection.description), row, strict=True))


def _quote_raw_mapping(mapping: Mapping[str, str]) -> dict[str, str]:
    return {standard: source for source, standard in mapping.items()}


def _sql_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _global_identity_audit(connection: duckdb.DuckDBPyConnection,
                           quote_paths: list[Path], quarantine_paths: list[Path],
                           mapping: Mapping[str, str], source_timezone: str) -> dict[str, Any]:
    standard_to_source = _quote_raw_mapping(mapping)
    source_exchange = standard_to_source.get("exchange", "MARKET")
    source_instrument = standard_to_source.get("instrument_id", "SYMBOL")
    source_date = standard_to_source.get("trade_date", "TRADINGDATE")
    source_time = standard_to_source.get("event_time", "TRADINGTIME")
    quote_relation = "read_parquet(?)"
    quarantine_relation = "read_parquet(?)"
    query = f"""
        WITH all_rows AS (
            SELECT exchange::VARCHAR AS exchange_key,
                   instrument_id::VARCHAR AS instrument_key,
                   trade_date::DATE AS trade_date_key,
                   event_time::TIMESTAMPTZ AS event_time_key,
                   record_id::VARCHAR AS record_id,
                   []::VARCHAR[] AS reason_codes,
                   'quotes'::VARCHAR AS output_kind
            FROM {quote_relation}
            UNION ALL
            SELECT {_sql_ident(source_exchange)}::VARCHAR AS exchange_key,
                   {_sql_ident(source_instrument)}::VARCHAR AS instrument_key,
                   {_sql_ident(source_date)}::DATE AS trade_date_key,
                   timezone(?, {_sql_ident(source_time)})::TIMESTAMPTZ AS event_time_key,
                   record_id::VARCHAR AS record_id,
                   reason_codes,
                   'quarantine'::VARCHAR AS output_kind
            FROM {quarantine_relation}
        ), event_groups AS (
            SELECT exchange_key, instrument_key, trade_date_key, event_time_key,
                   count(*) AS n_rows,
                   count(*) FILTER (WHERE output_kind='quotes') AS quote_rows,
                   count(*) FILTER (WHERE output_kind='quarantine' AND NOT (
                       list_contains(reason_codes, 'DUPLICATE_EVENT_KEY_IDENTICAL') OR
                       list_contains(reason_codes, 'DUPLICATE_EVENT_KEY_CONFLICTING')
                   )) AS duplicate_reason_missing_rows
            FROM all_rows
            WHERE exchange_key IS NOT NULL AND instrument_key IS NOT NULL
              AND trade_date_key IS NOT NULL AND event_time_key IS NOT NULL
            GROUP BY ALL
        ), record_groups AS (
            SELECT record_id, count(*) AS n_rows
            FROM all_rows
            GROUP BY record_id
        )
        SELECT (SELECT count(*) FROM all_rows) AS rows,
               (SELECT count(*) FROM event_groups WHERE n_rows > 1 AND quote_rows > 0) AS repeated_event_keys_with_accepted,
               (SELECT count(*) FROM event_groups WHERE n_rows > 1 AND duplicate_reason_missing_rows > 0)
                   AS repeated_event_keys_missing_duplicate_reason,
               (SELECT count(*) FROM record_groups WHERE record_id IS NULL OR n_rows > 1) AS duplicate_or_missing_record_ids
    """
    row = connection.execute(query, [
        [path.as_posix() for path in quote_paths],
        source_timezone,
        [path.as_posix() for path in quarantine_paths],
    ]).fetchone()
    return dict(zip((column[0] for column in connection.description), row, strict=True))


def _quarantine_reason_counts(connection: duckdb.DuckDBPyConnection,
                              quarantine_paths: list[Path]) -> dict[str, int]:
    rows = connection.execute(
        "SELECT reason_code, count(*) FROM ("
        "SELECT unnest(reason_codes) AS reason_code FROM read_parquet(?)"
        ") GROUP BY reason_code ORDER BY reason_code",
        [[path.as_posix() for path in quarantine_paths]],
    ).fetchall()
    return {str(reason): int(count) for reason, count in rows}


def _accepted_quality_flag_counts(connection: duckdb.DuckDBPyConnection,
                                  quote_paths: list[Path]) -> dict[str, int]:
    """Count row flags from retained quote artifacts, not quarantine reasons."""
    rows = connection.execute(
        "SELECT flag, count(*) FROM ("
        "SELECT unnest(quality_flags) AS flag FROM read_parquet(?)"
        ") WHERE flag IS NOT NULL GROUP BY flag ORDER BY flag",
        [[path.as_posix() for path in quote_paths]],
    ).fetchall()
    return {str(flag): int(count) for flag, count in rows}


def _calendar_dates(connection: duckdb.DuckDBPyConnection, path: Path) -> tuple[list[str], list[str]]:
    try:
        rows = connection.execute(
            "SELECT calendar_id::VARCHAR, trade_date::DATE FROM read_parquet(?) ORDER BY trade_date",
            [path.as_posix()],
        ).fetchall()
    except Exception as exc:
        raise VerificationError(f"calendar.parquet must contain calendar_id:string and trade_date:date32: {exc}") from exc
    if not rows:
        raise VerificationError("calendar.parquet has no trading dates")
    ids = sorted({str(row[0]) for row in rows})
    dates = [_date_text(row[1]) for row in rows]
    if len(dates) != len(set(dates)):
        raise VerificationError("calendar.parquet contains duplicate trade_date keys")
    if len(ids) != 1:
        raise VerificationError(f"calendar.parquet must contain one calendar_id, found {ids}")
    return ids, dates


def _validate_session_rules(path: Path, expected_timezone: str) -> dict[str, Any]:
    rules = _safe_json(path, "session_rules.json")
    if not isinstance(rules.get("schema_version"), str) or not isinstance(rules.get("version"), str):
        raise VerificationError("session_rules.json requires schema_version and version")
    if rules.get("timezone") != expected_timezone:
        raise VerificationError("session_rules.json timezone does not match inputs.source_timezone")
    if rules.get("interval_convention") != "[start, end)":
        raise VerificationError("session_rules.json must declare half-open [start, end) intervals")
    if not isinstance(rules.get("last_trading_day_rule"), str) or not rules["last_trading_day_rule"]:
        raise VerificationError("session_rules.json requires last_trading_day_rule evidence")
    if not isinstance(rules.get("rules"), list) or not rules["rules"]:
        raise VerificationError("session_rules.json requires non-empty rules")
    if not isinstance(rules.get("sample_classifications"), list) or not rules["sample_classifications"]:
        raise VerificationError("session_rules.json requires sample_classifications")
    if not isinstance(rules.get("source_refs"), list) or not rules["source_refs"]:
        raise VerificationError("session_rules.json requires source_refs")
    if not isinstance(rules.get("evidence_sha256"), str):
        raise VerificationError("session_rules.json requires the frozen evidence hash")
    for item in rules["rules"]:
        if not isinstance(item, dict) or not isinstance(item.get("products"), list):
            raise VerificationError("session_rules.json contains a malformed product rule")
        start = item.get("effective_from", item.get("effective_for_scope_start"))
        end = item.get("effective_for_scope_end", "9999-12-31")
        try:
            first_date = date.fromisoformat(start) if start else date.min
            last_date = date.fromisoformat(end)
        except (TypeError, ValueError) as exc:
            raise VerificationError("session rule has invalid effective date bounds") from exc
        if first_date > last_date:
            raise VerificationError("session rule has reversed effective date bounds")
        segments = []
        auction = item.get("opening_auction")
        if isinstance(auction, dict):
            for start_key, end_key in (("start", "order_submission_end"),
                                       ("matching_start", "end")):
                segments.append((auction.get(start_key), auction.get(end_key)))
        for key in ("morning_continuous", "afternoon_continuous_regular_day"):
            segment = item.get(key)
            if isinstance(segment, dict):
                segments.append((segment.get("start"), segment.get("end")))
        if not segments:
            raise VerificationError("session rule has no intraday intervals")
        parsed = []
        try:
            parsed = [(clock_time.fromisoformat(a), clock_time.fromisoformat(b)) for a, b in segments]
        except (TypeError, ValueError) as exc:
            raise VerificationError("session rule has malformed intraday interval") from exc
        parsed.sort()
        if any(start_time >= end_time for start_time, end_time in parsed):
            raise VerificationError("session rule has empty or reversed intraday interval")
        if any(right[0] < left[1] for left, right in pairwise(parsed)):
            raise VerificationError("session rule contains overlapping intervals")
    return rules


def _validate_partition_session_coverage(rules: Mapping[str, Any], partitions: list[dict[str, Any]]) -> None:
    """Reject stale rules that do not cover every published contract/date partition."""
    for part in partitions:
        instrument = str(part.get("instrument_id", ""))
        product = next((name for name in ("TF", "TS", "TL", "T")
                        if instrument.startswith(name)), None)
        if product is None:
            raise VerificationError(f"cannot derive a product for session coverage: {instrument}")
        try:
            trade_date = date.fromisoformat(str(part["trade_date"]))
        except (KeyError, ValueError) as exc:
            raise VerificationError(f"invalid partition trade_date for {instrument}") from exc
        applicable = []
        for rule in rules["rules"]:
            first_raw = rule.get("effective_from", rule.get("effective_for_scope_start"))
            last_raw = rule.get("effective_for_scope_end", "9999-12-31")
            first = date.fromisoformat(first_raw) if first_raw else date.min
            last = date.fromisoformat(last_raw)
            if product in rule["products"] and first <= trade_date <= last:
                applicable.append(rule)
        if len(applicable) != 1:
            raise VerificationError(
                f"session rules cover {instrument} {trade_date} {len(applicable)} times; expected exactly one"
            )


def _expiry_date(instrument_id: str, trade_date: date,
                 calendar_dates: set[date]) -> date | None:
    match = re.fullmatch(r"(?:TF|TS|TL|T)(\d{2})(\d{2})", instrument_id)
    if match is None:
        return None
    year = 2000 + int(match.group(1))
    month = int(match.group(2))
    if not 1 <= month <= 12:
        return None
    first = date(year, month, 1)
    nominal = first + timedelta(days=(4 - first.weekday()) % 7 + 7)
    if nominal in calendar_dates:
        return nominal
    if trade_date < nominal:
        return nominal
    return min((day for day in calendar_dates if day > nominal), default=None)


def _register_session_intervals(connection: duckdb.DuckDBPyConnection,
                                rules: Mapping[str, Any], partitions: list[dict[str, Any]],
                                calendar_dates: list[str], calendar_id: str) -> None:
    calendar = {date.fromisoformat(item) for item in calendar_dates}
    rows: list[tuple[str, date, str, str, clock_time, clock_time]] = []
    for part in partitions:
        instrument_id = str(part["instrument_id"])
        product = next(name for name in ("TF", "TS", "TL", "T") if instrument_id.startswith(name))
        trade_date = date.fromisoformat(str(part["trade_date"]))
        applicable = []
        for rule in rules["rules"]:
            first_raw = rule.get("effective_from", rule.get("effective_for_scope_start"))
            last_raw = rule.get("effective_for_scope_end", "9999-12-31")
            first = date.fromisoformat(first_raw) if first_raw else date.min
            last = date.fromisoformat(last_raw)
            if product in rule["products"] and first <= trade_date <= last:
                applicable.append(rule)
        if len(applicable) != 1:
            raise VerificationError(f"session rule coverage changed while building intervals for {instrument_id}")
        rule = applicable[0]
        segments: list[tuple[str, str, str]] = []
        auction = rule.get("opening_auction")
        if isinstance(auction, dict):
            segments.extend([
                ("opening_auction_submission", auction["start"], auction["order_submission_end"]),
                ("opening_auction_matching", auction["matching_start"], auction["end"]),
            ])
        morning = rule.get("morning_continuous")
        if isinstance(morning, dict):
            segments.append(("continuous_morning", morning["start"], morning["end"]))
        afternoon = rule.get("afternoon_continuous_regular_day")
        expiry = _expiry_date(instrument_id, trade_date, calendar)
        if isinstance(afternoon, dict) and (
            rule.get("last_trading_day_afternoon") != "closed" or expiry != trade_date
        ):
            segments.append(("continuous_afternoon", afternoon["start"], afternoon["end"]))
        for segment_id, start, end in segments:
            rows.append((instrument_id, trade_date, segment_id,
                         f"{calendar_id}:{trade_date.isoformat()}:{segment_id}",
                         clock_time.fromisoformat(start), clock_time.fromisoformat(end)))
    table = pa.table({
        "instrument_id": [row[0] for row in rows],
        "trade_date": pa.array([row[1] for row in rows], type=pa.date32()),
        "segment_id": [row[2] for row in rows],
        "session_id": [row[3] for row in rows],
        "start_time": pa.array([row[4] for row in rows], type=pa.time64("us")),
        "end_time": pa.array([row[5] for row in rows], type=pa.time64("us")),
    })
    connection.register("_session_intervals", table)


def calculate_l1(bid_price: Any, ask_price: Any, bid_size: Any, ask_size: Any,
                 tick_size: Any = None) -> dict[str, str | None]:
    """Small consumer check, intentionally not a Stage 03 feature factory."""
    try:
        bid = Decimal(str(bid_price))
        ask = Decimal(str(ask_price))
        bid_qty = Decimal(str(bid_size))
        ask_qty = Decimal(str(ask_size))
    except (InvalidOperation, ValueError) as exc:
        raise VerificationError(f"cannot calculate L1 values from invalid fixture row: {exc}") from exc
    if bid <= 0 or ask <= 0 or ask < bid or bid_qty < 0 or ask_qty < 0:
        raise VerificationError("fixture L1 quote is invalid")
    spread = ask - bid
    total_size = bid_qty + ask_qty
    imbalance = (bid_qty - ask_qty) / total_size if total_size else None
    spread_ticks = None
    if tick_size is not None:
        tick = Decimal(str(tick_size))
        if tick <= 0:
            raise VerificationError("known tick_size must be positive")
        spread_ticks = spread / tick
    return {
        "mid": format((bid + ask) / 2, "f"),
        "spread_price_units": format(spread, "f"),
        "l1_obi": format(imbalance, "f") if imbalance is not None else None,
        "spread_ticks": format(spread_ticks, "f") if spread_ticks is not None else None,
    }


def session_local_intervals(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return only adjacent intervals within the same contract and session."""
    parsed = []
    for row in rows:
        timestamp = _parse_datetime(row["event_time"], "fixture.event_time")
        parsed.append((str(row["instrument_id"]), str(row["session_id"]), timestamp, row))
    parsed.sort(key=lambda item: (item[0], item[1], item[2]))
    intervals = []
    prior: tuple[str, str, datetime, Mapping[str, Any]] | None = None
    for current in parsed:
        if prior and current[0] == prior[0] and current[1] == prior[1]:
            seconds = (current[2] - prior[2]).total_seconds()
            if seconds < 0:
                raise VerificationError("fixture event times decrease inside a session")
            intervals.append({"instrument_id": current[0], "session_id": current[1],
                              "from": prior[2].isoformat(), "to": current[2].isoformat(),
                              "seconds": seconds})
        prior = current
    return intervals


def horizon_pairs(rows: Iterable[Mapping[str, Any]], horizon_seconds: float) -> list[tuple[Any, Any]]:
    """Fixture-only next-observation pairing; never crosses a contract/session or long gap."""
    if horizon_seconds < 0 or not math.isfinite(horizon_seconds):
        raise ValueError("horizon_seconds must be finite and non-negative")
    parsed = []
    for row in rows:
        parsed.append((str(row["instrument_id"]), str(row["session_id"]),
                       _parse_datetime(row["event_time"], "fixture.event_time"), row))
    parsed.sort(key=lambda item: (item[0], item[1], item[2]))
    pairs = []
    for left, right in pairwise(parsed):
        if left[0] != right[0] or left[1] != right[1]:
            continue
        elapsed = (right[2] - left[2]).total_seconds()
        if 0 <= elapsed <= horizon_seconds:
            pairs.append((left[3], right[3]))
    return pairs


def _sample_rows(connection: duckdb.DuckDBPyConnection, quote_paths: list[Path], limit: int = 8) -> list[dict[str, Any]]:
    query = """
        SELECT instrument_id, session_id, event_time, bid_price_1, ask_price_1,
               bid_size_1, ask_size_1, trade_date
        FROM read_parquet(?)
        LIMIT ?
    """
    cursor = connection.execute(query, [[path.as_posix() for path in quote_paths], limit])
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _metadata_for_instrument(passport: Mapping[str, Any], instrument_id: str) -> Mapping[str, Any]:
    metadata = passport.get("instrument_metadata", [])
    if isinstance(metadata, list):
        for item in metadata:
            if isinstance(item, dict) and item.get("instrument_id") == instrument_id:
                return item
    elif isinstance(metadata, dict):
        value = metadata.get(instrument_id)
        if isinstance(value, dict):
            return value
    return {}


def _evidenced_cffex_dates(first: date, last: date, evidence: Mapping[str, Any]) -> list[str]:
    scope = evidence.get("scope", {})
    evidence_start = date.fromisoformat(scope["trade_date_start"])
    evidence_end = date.fromisoformat(scope["trade_date_end"])
    if first < evidence_start or last > evidence_end:
        raise VerificationError("calendar date range falls outside frozen CFFEX applicability evidence")
    closed_ranges = [
        (date.fromisoformat(span["start"]), date.fromisoformat(span["end"]))
        for notice in evidence.get("annual_notices", [])
        for span in notice.get("closure_ranges_inclusive", [])
    ]
    result = []
    current = first
    while current <= last:
        if current.weekday() < 5 and not any(start <= current <= end for start, end in closed_ranges):
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _validate_input_evidence(entry_path: Path, inputs: Mapping[str, Any]) -> dict[str, Any]:
    timezone_name = inputs.get("source_timezone")
    if not isinstance(timezone_name, str):
        raise VerificationError("inputs.source_timezone is required")
    try:
        ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise VerificationError(f"unknown source timezone {timezone_name!r}") from exc
    raw_files = inputs.get("raw_files")
    if not isinstance(raw_files, list) or not raw_files:
        raise VerificationError("inputs.raw_files must list frozen source file metadata")
    source_rows = 0
    source_file_hashes: set[str] = set()
    for item in raw_files:
        if not isinstance(item, dict):
            raise VerificationError("inputs.raw_files contains a non-object entry")
        source_hash = _hash_text(item.get("sha256"), "raw_files.sha256")
        if source_hash in source_file_hashes:
            raise VerificationError("inputs.raw_files contains duplicate content hashes")
        source_file_hashes.add(source_hash)
        if not isinstance(item.get("row_count"), int) or isinstance(item.get("row_count"), bool):
            raise VerificationError("raw_files.row_count must be an integer")
        if item["row_count"] < 0:
            raise VerificationError("raw_files.row_count must be non-negative")
        source_format = item.get("source_format")
        if source_format not in {"csv", "parquet"}:
            raise VerificationError("raw_files.source_format must be csv or parquet")
        row_group_count = item.get("row_group_count")
        if source_format == "parquet" and (
            isinstance(row_group_count, bool) or not isinstance(row_group_count, int)
            or row_group_count <= 0
        ):
            raise VerificationError("Parquet raw_files require a positive row_group_count")
        if source_format == "parquet":
            offsets = item.get("row_group_offsets")
            group_rows = item.get("row_group_row_counts")
            if (not isinstance(offsets, list) or not isinstance(group_rows, list)
                    or len(offsets) != row_group_count or len(group_rows) != row_group_count):
                raise VerificationError("Parquet raw_files require row_group_offsets and row_group_row_counts")
            next_offset = 0
            for offset, count in zip(offsets, group_rows, strict=True):
                if (isinstance(offset, bool) or not isinstance(offset, int) or offset != next_offset
                        or isinstance(count, bool) or not isinstance(count, int) or count <= 0):
                    raise VerificationError("Parquet raw row group boundaries are invalid")
                next_offset += count
            if next_offset != item["row_count"]:
                raise VerificationError("Parquet raw row group counts do not sum to file row_count")
        if source_format == "csv" and row_group_count is not None:
            raise VerificationError("CSV raw_files must have null row_group_count")
        if source_format == "csv" and (item.get("row_group_offsets") != []
                                        or item.get("row_group_row_counts") != []):
            raise VerificationError("CSV raw_files must have empty row-group boundary arrays")
        if not isinstance(item.get("uri"), str) or not item["uri"]:
            raise VerificationError("raw_files.uri is required")
        size_bytes = item.get("size_bytes")
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes < 0:
            raise VerificationError("raw_files.size_bytes must be a non-negative integer")
        source_rows += item["row_count"]
    schema, source_output_types = _load_source_schema(entry_path, inputs)
    evidence_ref = inputs.get("session_evidence_ref")
    if not isinstance(evidence_ref, dict) or not isinstance(evidence_ref.get("uri"), str):
        raise VerificationError("inputs.session_evidence_ref is required")
    evidence_path = file_uri_path(evidence_ref["uri"])
    evidence_hash = _hash_text(evidence_ref.get("sha256"), "session_evidence_ref.sha256")
    if sha256_file(evidence_path) != evidence_hash:
        raise VerificationError("session evidence reference hash mismatch")
    calendar_input = inputs.get("calendar")
    if not isinstance(calendar_input, dict):
        raise VerificationError("inputs.calendar must be a frozen calendar provenance object")
    provenance = calendar_input.get("provenance", calendar_input)
    if not isinstance(provenance, dict):
        raise VerificationError("inputs.calendar.provenance must be an object")
    provider = calendar_input.get("provider", provenance.get("provider"))
    provenance_provider = provenance.get("provider", provider)
    if not isinstance(provider, str) or not provider or provider != provenance_provider:
        raise VerificationError("inputs.calendar provider is missing or disagrees with provenance")
    calendar_id = calendar_input.get("calendar_id", provenance.get("calendar_id"))
    if not isinstance(calendar_id, str) or not calendar_id:
        raise VerificationError("inputs.calendar.calendar_id is required")
    evidence_uri = provenance.get("evidence_uri")
    evidence_sha256 = _hash_text(provenance.get("evidence_sha256"),
                                 "inputs.calendar.provenance.evidence_sha256")
    if evidence_uri != evidence_ref["uri"] or evidence_sha256 != evidence_hash:
        raise VerificationError("calendar provenance does not match the pinned CFFEX evidence ref")
    evidence = _safe_json(evidence_path, "CFFEX calendar evidence")
    calendar_evidence = evidence.get("calendar_comparison", {})
    approved_id = calendar_evidence.get("candidate_calendar_id")
    if calendar_id != approved_id:
        raise VerificationError(f"calendar_id {calendar_id!r} is not approved by frozen evidence")
    date_starts = [date.fromisoformat(item["trade_date_min"]) for item in raw_files
                   if isinstance(item.get("trade_date_min"), str)]
    date_ends = [date.fromisoformat(item["trade_date_max"]) for item in raw_files
                 if isinstance(item.get("trade_date_max"), str)]
    if not date_starts or not date_ends:
        raise VerificationError("raw file evidence has no non-null trade-date range")
    source_start, source_end = min(date_starts), max(date_ends)
    scope = evidence.get("scope", {})
    evidence_start = date.fromisoformat(scope["trade_date_start"])
    evidence_end = date.fromisoformat(scope["trade_date_end"])
    if source_start < evidence_start or source_end > evidence_end:
        raise VerificationError("source date range falls outside frozen calendar applicability evidence")
    expected_dates = _evidenced_cffex_dates(source_start, source_end, evidence)
    frozen_snapshot_path: Path | None = None
    frozen_snapshot_dates: list[str] | None = None
    if provider == "verified_frozen_snapshot_fallback":
        snapshot_uri = provenance.get("snapshot_uri", calendar_input.get("snapshot_uri"))
        snapshot_hash = provenance.get("snapshot_sha256", calendar_input.get("snapshot_sha256"))
        if not isinstance(snapshot_uri, str) or not snapshot_uri:
            raise VerificationError("frozen calendar fallback requires snapshot_uri")
        snapshot_hash = _hash_text(snapshot_hash, "inputs.calendar.snapshot_sha256")
        snapshot_path = file_uri_path(snapshot_uri)
        if sha256_file(snapshot_path) != snapshot_hash:
            raise VerificationError("frozen calendar snapshot hash mismatch")
        try:
            snapshot = pq.read_table(snapshot_path)
        except Exception as exc:
            raise VerificationError(f"cannot read frozen calendar snapshot: {exc}") from exc
        expected_schema = pa.schema([pa.field("calendar_id", pa.string()),
                                     pa.field("trade_date", pa.date32())])
        if snapshot.schema.names != expected_schema.names or snapshot.schema.types != expected_schema.types:
            raise VerificationError("frozen calendar snapshot schema is invalid")
        snapshot_ids = set(snapshot.column("calendar_id").to_pylist())
        snapshot_dates = sorted(_date_text(value) for value in snapshot.column("trade_date").to_pylist())
        if snapshot_ids != {calendar_id} or not set(expected_dates).issubset(snapshot_dates):
            raise VerificationError("frozen calendar snapshot ID or coverage differs from evidence")
        if not snapshot_dates or snapshot_dates != _evidenced_cffex_dates(
            date.fromisoformat(snapshot_dates[0]), date.fromisoformat(snapshot_dates[-1]), evidence
        ):
            raise VerificationError("frozen calendar snapshot contains dates outside CFFEX evidence")
        if date.fromisoformat(str(provenance.get("start"))) > source_start \
                or date.fromisoformat(str(provenance.get("end"))) < source_end:
            raise VerificationError("frozen calendar snapshot range does not cover raw input dates")
        if provenance.get("row_count") != snapshot.num_rows:
            raise VerificationError("frozen calendar snapshot row_count mismatch")
        frozen_snapshot_path = snapshot_path
        frozen_snapshot_dates = snapshot_dates
    elif provider == "local_duckdb":
        local_fields = ("catalog_pointer_uri", "database_uri", "db_manifest_uri", "selected_file_uri")
        if any(not isinstance(calendar_input.get(key), str) or not calendar_input[key]
               for key in local_fields):
            raise VerificationError("local calendar provenance is missing catalog/database/file refs")
        if not isinstance(calendar_input.get("release"), str) or not calendar_input["release"]:
            raise VerificationError("local calendar provenance requires a release ID")
        _hash_text(calendar_input.get("selected_file_sha256"),
                   "inputs.calendar.selected_file_sha256")
    elif provider in {"api", "marketdata_api"}:
        if not isinstance(calendar_input.get("base_url"), str) or not calendar_input["base_url"]:
            raise VerificationError("API calendar provenance requires base_url")
        if not isinstance(calendar_input.get("release"), str) or not calendar_input["release"]:
            raise VerificationError("API calendar provenance requires a pinned release")
        if not isinstance(calendar_input.get("query"), str) or not calendar_input["query"]:
            raise VerificationError("API calendar provenance requires the executed query")
        if not isinstance(calendar_input.get("params"), list):
            raise VerificationError("API calendar provenance requires query params")
    else:
        raise VerificationError(f"unsupported frozen calendar provider: {provider}")
    return {"source_timezone": timezone_name, "raw_input_rows": source_rows,
            "raw_files": raw_files,
            "calendar_id": calendar_id, "expected_calendar_dates": expected_dates,
            "frozen_calendar_path": frozen_snapshot_path,
            "frozen_calendar_dates": frozen_snapshot_dates,
            "calendar_evidence": evidence,
            "source_schema": schema, "quote_source_types": source_output_types,
            "schema_path": str(_resolve_schema_ref(entry_path, inputs["schema_ref"]))}


def _register_input_file_policy(connection: duckdb.DuckDBPyConnection,
                                raw_files: list[dict[str, Any]]) -> None:
    table = pa.table({
        "raw_file_sha256": [item["sha256"].removeprefix("sha256:") for item in raw_files],
        "source_format": [item["source_format"] for item in raw_files],
        "row_count": pa.array([item["row_count"] for item in raw_files], type=pa.int64()),
        "row_group_count": pa.array([item["row_group_count"] for item in raw_files], type=pa.int64()),
    })
    connection.register("_input_file_policy", table)
    group_hashes = []
    group_indexes = []
    row_starts = []
    row_ends = []
    for item in raw_files:
        if item["source_format"] != "parquet":
            continue
        content_hash = item["sha256"].removeprefix("sha256:")
        for index, (start, count) in enumerate(zip(
            item["row_group_offsets"], item["row_group_row_counts"], strict=True
        )):
            group_hashes.append(content_hash)
            group_indexes.append(index)
            row_starts.append(start)
            row_ends.append(start + count)
    group_table = pa.table({
        "raw_file_sha256": pa.array(group_hashes, type=pa.string()),
        "row_group_index": pa.array(group_indexes, type=pa.int32()),
        "row_start": pa.array(row_starts, type=pa.int64()),
        "row_end": pa.array(row_ends, type=pa.int64()),
    })
    connection.register("_input_group_policy", group_table)


def _quarantine_session_partitions(connection: duckdb.DuckDBPyConnection,
                                   quarantine_paths: list[Path],
                                   mapping: Mapping[str, str],
                                   existing: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Include quarantined contract/date keys when reconstructing input session scopes."""
    standard_to_source = _quote_raw_mapping(mapping)
    instrument_col = _sql_ident(standard_to_source.get("instrument_id", "SYMBOL"))
    date_col = _sql_ident(standard_to_source.get("trade_date", "TRADINGDATE"))
    rows = connection.execute(
        f"SELECT DISTINCT {instrument_col}::VARCHAR, {date_col}::DATE "
        f"FROM read_parquet(?) WHERE {instrument_col} IS NOT NULL AND {date_col} IS NOT NULL",
        [[path.as_posix() for path in quarantine_paths]],
    ).fetchall()
    result = list(existing)
    known = {(str(item["instrument_id"]), str(item["trade_date"])) for item in result}
    for instrument, trade_day in rows:
        instrument_text = str(instrument)
        trade_day_text = _date_text(trade_day)
        if not any(instrument_text.startswith(product) for product in ("TF", "TS", "TL", "T")):
            continue
        key = (instrument_text, trade_day_text)
        if key not in known:
            result.append({"exchange": "CFFEX", "instrument_id": instrument_text,
                           "trade_date": trade_day_text})
            known.add(key)
    return result


def _projected_source_rows_sql(
    fields: list[dict[str, Any]], mapping: Mapping[str, str], source_timezone: str,
    *, is_quote: bool
) -> tuple[str, list[Any]]:
    """Project only the consumer audit columns from one source relation."""
    timezone_params: list[Any] = []
    source_projection = []
    for field in fields:
        source_name = field["name"]
        output_name = mapping.get(source_name, source_name) if is_quote else source_name
        if is_quote and source_name == "TRADINGTIME":
            source_projection.append(
                f"timezone(?, q.{_sql_ident(output_name)}) AS {_sql_ident(source_name)}"
            )
            timezone_params.append(source_timezone)
        else:
            source_projection.append(
                f"q.{_sql_ident(output_name)} AS {_sql_ident(source_name)}"
            )
    null_field_count = " + ".join(
        f"CASE WHEN r.{_sql_ident(field['name'])} IS NULL THEN 1 ELSE 0 END"
        for field in fields
    )
    payload_fields = [
        field["name"] for field in fields
        if field["name"] not in {"source_file", "source_row", "ingest_run_id"}
    ]
    payload_entries = ",".join(
        f"{_sql_ident(name)} := r.{_sql_ident(name)}" for name in payload_fields
    )
    raw_projection = ",".join(source_projection)
    is_quote_sql = "'quotes'::VARCHAR" if is_quote else "'quarantine'::VARCHAR"
    reasons_sql = "[]::VARCHAR[]" if is_quote else "q.reason_codes"
    inner = (
        "SELECT q.raw_file_sha256::VARCHAR AS raw_file_sha256,"
        "q.raw_row_index::BIGINT AS raw_row_index,"
        f"{is_quote_sql} AS row_kind,{reasons_sql} AS reason_codes,{raw_projection} "
        "FROM read_parquet(?) q"
    )
    valid_price_pair = (
        "(r.BUYPRICE01 IS NOT NULL AND r.SELLPRICE01 IS NOT NULL "
        "AND r.BUYPRICE01>0 AND r.SELLPRICE01>0)"
    )
    crossed_book = f"({valid_price_pair} AND r.BUYPRICE01>r.SELLPRICE01)"
    invalid_l1 = (
        "(r.BUYPRICE01 IS NULL OR r.SELLPRICE01 IS NULL "
        "OR r.BUYVOLUME01 IS NULL OR r.SELLVOLUME01 IS NULL "
        "OR r.BUYPRICE01<=0 OR r.SELLPRICE01<=0 "
        "OR r.BUYVOLUME01<0 OR r.SELLVOLUME01<0 "
        "OR r.SELLPRICE01<r.BUYPRICE01)"
    )
    depth_invalid = _depth_invalid_expression("r", standardized=False)
    sql = (
        "SELECT r.raw_file_sha256,r.raw_row_index,r.row_kind,r.reason_codes,"
        "r.MARKET,r.SYMBOL,r.TRADINGDATE,r.TRADINGTIME,r.UNIX,r.ingest_run_id,r.source_file,"
        f"({null_field_count})::BIGINT AS null_field_values,"
        f"{invalid_l1} AS invalid_quote,"
        "list_contains(r.reason_codes,'INVALID_L1_BOOK') AS has_invalid_l1_reason,"
        f"{valid_price_pair} AS valid_price_pair,{crossed_book} AS crossed_book,"
        f"{depth_invalid} AS depth_invalid,"
        "sha256(to_json(struct_pack(" + payload_entries + "))) AS payload_hash "
        f"FROM ({inner}) r"
    )
    return sql, timezone_params


def _rate_matches(metric: Any, numerator: int, denominator: int, formula: str, what: str) -> None:
    if not isinstance(metric, dict):
        raise VerificationError(f"{what} must be a rate object")
    value = numerator / denominator if denominator else None
    if (metric.get("numerator") != numerator or metric.get("denominator") != denominator
            or metric.get("formula") != formula):
        raise VerificationError(f"{what} numerator/denominator/formula differs from serialized rows")
    reported_value = metric.get("value")
    if value is None:
        if reported_value is not None:
            raise VerificationError(f"{what} value must be null when denominator is zero")
    elif (isinstance(reported_value, bool) or not isinstance(reported_value, (int, float))
          or not math.isclose(float(reported_value), value, rel_tol=1e-12, abs_tol=1e-12)):
        raise VerificationError(f"{what} value differs from serialized numerator/denominator")


def _scope_key(item: Mapping[str, Any]) -> tuple[Any, ...]:
    trade_day = item.get("trade_date")
    if isinstance(trade_day, (date, datetime)):
        trade_day = trade_day.isoformat()[:10]
    elif trade_day is not None:
        trade_day = str(trade_day)
    return (item.get("raw_file_sha256"), item.get("exchange"), item.get("instrument_id"), trade_day)


def _session_scope_key(item: Mapping[str, Any]) -> tuple[Any, ...]:
    return (*_scope_key(item), item.get("session_id"))


def _gap_scope_key(item: Mapping[str, Any]) -> tuple[Any, ...]:
    return (*_scope_key(item), item.get("session_id"))


def _validate_scoped_quality_report_batched(
    connection: duckdb.DuckDBPyConnection,
    quote_paths_by_date: Mapping[str, list[Path]],
    quarantine_paths: list[Path],
    source_schema: Mapping[str, Any],
    mapping: Mapping[str, str],
    source_timezone: str,
    raw_files: list[dict[str, Any]],
    quality: Mapping[str, Any],
    calendar_dates: list[str],
) -> dict[str, int]:
    """Recompute the complete quality report one trade date at a time.

    Trade date is part of the event key, so a duplicate group cannot cross batches.
    A compact source-row bitset separately proves input-row identity uniqueness across dates.
    """
    fields = source_schema["sample"]["fields"]
    quarantine_sql, quarantine_params = _projected_source_rows_sql(
        fields, mapping, source_timezone, is_quote=False
    )
    connection.execute(
        "CREATE TEMP TABLE _consumer_quarantine_rows AS "
        "SELECT r.*,s.session_id FROM (" + quarantine_sql + ") r "
        "LEFT JOIN _session_intervals s ON r.SYMBOL::VARCHAR=s.instrument_id "
        "AND r.TRADINGDATE::DATE=s.trade_date "
        "AND CAST(r.TRADINGTIME AS TIME)>=s.start_time "
        "AND CAST(r.TRADINGTIME AS TIME)<s.end_time",
        [*quarantine_params, [path.as_posix() for path in quarantine_paths]],
    )
    quarantine_dates = connection.execute(
        "SELECT DISTINCT TRADINGDATE::DATE FROM _consumer_quarantine_rows"
    ).fetchall()
    batch_dates: set[str | None] = set(quote_paths_by_date)
    batch_dates.update(_date_text(row[0]) if row[0] is not None else None for row in quarantine_dates)
    ordered_dates: list[str | None] = sorted(day for day in batch_dates if day is not None)
    if None in batch_dates:
        ordered_dates.append(None)

    raw_source_uri = {
        item["sha256"].removeprefix("sha256:"): item["uri"] for item in raw_files
    }
    identity_bitmaps, identity_expected = _new_identity_bitmaps(raw_files)
    identity_seen: Counter[str] = Counter()
    identity_bad = False
    identity_audit_rows = 0
    actual_scopes: dict[tuple[Any, ...], dict[str, Any]] = {}
    actual_sessions: dict[tuple[Any, ...], dict[str, int]] = {}
    actual_gaps: dict[tuple[Any, ...], dict[str, Any]] = {}
    duplicate_classification = Counter({
        "identical_payload_groups": 0, "identical_payload_rows": 0,
        "identical_payload_extra_rows": 0, "conflicting_payload_groups": 0,
        "conflicting_payload_rows": 0, "conflicting_payload_extra_rows": 0,
    })
    duplicate_event_key_groups = 0
    duplicate_event_group_member_rows = 0
    duplicate_event_extra_rows = 0
    repeated_event_keys_with_accepted = 0
    repeated_event_keys_missing_duplicate_reason = 0
    duplicate_reason_category_mismatch_rows = 0
    source_duplicate_rows = 0
    three_rule_quarantine_union_rows = 0
    three_rule_overlap_rows = 0
    invalid_l1_reason_rows = 0
    session_unclassified_reason_rows = 0
    duplicate_event_reason_rows = 0
    calendar_set = set(calendar_dates)

    for trade_day in ordered_dates:
        quote_day_paths = quote_paths_by_date.get(trade_day, []) if trade_day is not None else []
        branches: list[str] = []
        params: list[Any] = []
        if quote_day_paths:
            quote_sql, quote_params = _projected_source_rows_sql(
                fields, mapping, source_timezone, is_quote=True
            )
            branches.append(
                "SELECT r.*,s.session_id FROM (" + quote_sql + ") r "
                "LEFT JOIN _session_intervals s ON r.SYMBOL::VARCHAR=s.instrument_id "
                "AND r.TRADINGDATE::DATE=s.trade_date "
                "AND CAST(r.TRADINGTIME AS TIME)>=s.start_time "
                "AND CAST(r.TRADINGTIME AS TIME)<s.end_time "
                "WHERE r.TRADINGDATE::DATE=CAST(? AS DATE)"
            )
            params.extend(quote_params)
            params.extend([[path.as_posix() for path in quote_day_paths], trade_day])
        if trade_day is None:
            branches.append(
                "SELECT * FROM _consumer_quarantine_rows WHERE TRADINGDATE IS NULL"
            )
        else:
            branches.append(
                "SELECT * FROM _consumer_quarantine_rows WHERE TRADINGDATE::DATE=CAST(? AS DATE)"
            )
            params.append(trade_day)
        connection.execute(
            "CREATE TEMP TABLE _consumer_batch_rows AS " + " UNION ALL ".join(branches), params
        )
        try:
            identity_bad |= _consume_batch_identity_rows(
                connection, identity_bitmaps, identity_expected, identity_seen
            )

            connection.execute(
                "CREATE TEMP TABLE _consumer_event_groups AS "
                "SELECT MARKET,SYMBOL,TRADINGDATE,TRADINGTIME,count(*) AS event_count,"
                "count(*) FILTER(WHERE row_kind='quotes') AS quote_rows,"
                "count(*) FILTER(WHERE row_kind='quarantine' AND NOT ("
                "list_contains(reason_codes,'DUPLICATE_EVENT_KEY_IDENTICAL') OR "
                "list_contains(reason_codes,'DUPLICATE_EVENT_KEY_CONFLICTING'))) "
                "AS duplicate_reason_missing_rows,"
                "count(DISTINCT payload_hash) AS payload_variants "
                "FROM _consumer_batch_rows WHERE MARKET IS NOT NULL AND SYMBOL IS NOT NULL "
                "AND TRADINGDATE IS NOT NULL AND TRADINGTIME IS NOT NULL "
                "GROUP BY ALL HAVING count(*)>1"
            )
            group_counts = connection.execute(
                "SELECT count(*),coalesce(sum(event_count),0),coalesce(sum(event_count-1),0),"
                "count(*) FILTER(WHERE quote_rows>0),"
                "count(*) FILTER(WHERE duplicate_reason_missing_rows>0),"
                "count(*) FILTER(WHERE payload_variants=1),"
                "coalesce(sum(event_count) FILTER(WHERE payload_variants=1),0),"
                "coalesce(sum(event_count-1) FILTER(WHERE payload_variants=1),0),"
                "count(*) FILTER(WHERE payload_variants>1),"
                "coalesce(sum(event_count) FILTER(WHERE payload_variants>1),0),"
                "coalesce(sum(event_count-1) FILTER(WHERE payload_variants>1),0) "
                "FROM _consumer_event_groups"
            ).fetchone()
            duplicate_event_key_groups += int(group_counts[0])
            duplicate_event_group_member_rows += int(group_counts[1])
            duplicate_event_extra_rows += int(group_counts[2])
            repeated_event_keys_with_accepted += int(group_counts[3])
            repeated_event_keys_missing_duplicate_reason += int(group_counts[4])
            for name, value in zip(duplicate_classification, group_counts[5:], strict=True):
                duplicate_classification[name] += int(value)

            connection.execute(
                "CREATE TEMP TABLE _consumer_duplicate_members AS "
                "SELECT r.raw_file_sha256,r.MARKET,r.SYMBOL,r.TRADINGDATE,r.TRADINGTIME,"
                "r.row_kind,r.reason_codes,e.payload_variants,"
                "row_number() OVER (PARTITION BY r.MARKET,r.SYMBOL,r.TRADINGDATE,r.TRADINGTIME "
                "ORDER BY r.raw_file_sha256,r.raw_row_index) AS event_rank "
                "FROM _consumer_batch_rows r JOIN _consumer_event_groups e "
                "USING(MARKET,SYMBOL,TRADINGDATE,TRADINGTIME)"
            )
            duplicate_reason_category_mismatch_rows += int(connection.execute(
                "SELECT count(*) FROM _consumer_duplicate_members WHERE row_kind='quarantine' AND ("
                "(payload_variants=1 AND ("
                "NOT list_contains(reason_codes,'DUPLICATE_EVENT_KEY_IDENTICAL') OR "
                "list_contains(reason_codes,'DUPLICATE_EVENT_KEY_CONFLICTING'))) OR "
                "(payload_variants>1 AND ("
                "NOT list_contains(reason_codes,'DUPLICATE_EVENT_KEY_CONFLICTING') OR "
                "list_contains(reason_codes,'DUPLICATE_EVENT_KEY_IDENTICAL'))))"
            ).fetchone()[0])
            duplicate_rows = connection.execute(
                "SELECT raw_file_sha256,MARKET,SYMBOL,TRADINGDATE,"
                "count(DISTINCT TRADINGTIME) AS duplicate_event_groups,"
                "count(*) AS duplicate_member_rows,"
                "count(*) FILTER(WHERE event_rank>1) AS duplicate_event_extra_rows "
                "FROM _consumer_duplicate_members GROUP BY ALL"
            ).fetchall()
            duplicate_by_scope = {
                (row[0], row[1], row[2], _date_text(row[3]) if row[3] is not None else None): {
                    "duplicate_event_groups": int(row[4]),
                    "duplicate_member_rows": int(row[5]),
                    "duplicate_event_extra_rows": int(row[6]),
                }
                for row in duplicate_rows
            }

            reorder_rows = connection.execute(
                "WITH ordered AS (SELECT raw_file_sha256,MARKET,SYMBOL,TRADINGDATE,TRADINGTIME,"
                "lag(TRADINGTIME) OVER (PARTITION BY raw_file_sha256,ingest_run_id,source_file,"
                "SYMBOL,TRADINGDATE ORDER BY raw_row_index) AS prior_time "
                "FROM _consumer_batch_rows) "
                "SELECT raw_file_sha256,MARKET,SYMBOL,TRADINGDATE,"
                "count(*) FILTER(WHERE prior_time IS NOT NULL AND TRADINGTIME<prior_time) "
                "AS time_reorder_count FROM ordered GROUP BY ALL"
            ).fetchall()
            reorder_by_scope = {
                (row[0], row[1], row[2], _date_text(row[3]) if row[3] is not None else None): int(row[4])
                for row in reorder_rows
            }
            base_rows = connection.execute(
                "SELECT r.raw_file_sha256,r.MARKET,r.SYMBOL,r.TRADINGDATE,count(*) AS row_count,"
                "count(*) FILTER(WHERE r.row_kind='quotes') AS accepted_rows,"
                "count(*) FILTER(WHERE r.row_kind='quarantine') AS quarantined_rows,"
                "count(*) FILTER(WHERE r.invalid_quote) AS invalid_quote_rows,"
                "count(*) FILTER(WHERE r.valid_price_pair) AS valid_price_pair_rows,"
                "count(*) FILTER(WHERE r.crossed_book) AS crossed_book_rows,"
                "coalesce(sum(r.null_field_values),0) AS null_field_values,"
                "count(*) FILTER(WHERE r.session_id IS NULL) AS session_unclassified_rows,"
                "count(*) FILTER(WHERE r.depth_invalid) AS depth_invalid_source_rows,"
                "count(*) FILTER(WHERE r.row_kind='quotes' AND r.depth_invalid) AS depth_invalid_rows "
                "FROM _consumer_batch_rows r GROUP BY ALL ORDER BY ALL"
            ).fetchall()
            column_names = [item[0] for item in connection.description]
            for values in base_rows:
                raw_hash, exchange, instrument, row_trade_day = values[:4]
                key = (raw_hash, exchange, instrument,
                       _date_text(row_trade_day) if row_trade_day is not None else None)
                row = dict(zip(column_names, values, strict=True))
                normalized = {name: int(value or 0) for name, value in row.items()
                              if name not in {"raw_file_sha256", "MARKET", "SYMBOL", "TRADINGDATE"}}
                normalized.update(duplicate_by_scope.get(key, {
                    "duplicate_event_groups": 0, "duplicate_member_rows": 0,
                    "duplicate_event_extra_rows": 0,
                }))
                normalized["time_reorder_count"] = reorder_by_scope.get(key, 0)
                normalized["time_reorder_denominator"] = normalized["row_count"]
                normalized["null_field_value_denominator"] = normalized["row_count"] * len(fields)
                normalized["source_uri"] = raw_source_uri.get(str(raw_hash))
                actual_scopes[key] = normalized

            session_rows = connection.execute(
                "SELECT raw_file_sha256,MARKET,SYMBOL,TRADINGDATE,session_id,count(*) AS row_count,"
                "count(*) FILTER(WHERE row_kind='quotes') AS accepted_rows,"
                "count(*) FILTER(WHERE row_kind='quarantine') AS quarantined_rows "
                "FROM _consumer_batch_rows GROUP BY ALL ORDER BY ALL"
            ).fetchall()
            for row in session_rows:
                key = (row[0], row[1], row[2],
                       _date_text(row[3]) if row[3] is not None else None, row[4])
                actual_sessions[key] = {
                    "row_count": int(row[5]), "accepted_rows": int(row[6]),
                    "quarantined_rows": int(row[7]),
                }

            if trade_day is not None and trade_day in calendar_set:
                gap_rows = connection.execute(
                    "WITH valid_times AS (SELECT DISTINCT r.raw_file_sha256,r.MARKET,r.SYMBOL,"
                    "r.TRADINGDATE,r.session_id,r.TRADINGTIME FROM _consumer_batch_rows r "
                    "WHERE r.session_id IS NOT NULL AND r.MARKET IS NOT NULL AND r.SYMBOL IS NOT NULL "
                    "AND r.TRADINGDATE IS NOT NULL AND r.TRADINGTIME IS NOT NULL AND r.UNIX IS NOT NULL "
                    "AND CAST(r.TRADINGTIME AS DATE)=r.TRADINGDATE "
                    "AND CAST(epoch_ms(timezone(?,r.TRADINGTIME)) AS BIGINT)=CAST(r.UNIX AS BIGINT)), "
                    "group_counts AS (SELECT raw_file_sha256,MARKET,SYMBOL,TRADINGDATE,session_id,"
                    "count(*) AS unique_event_times FROM valid_times GROUP BY ALL), "
                    "ordered AS (SELECT *,epoch_us(TRADINGTIME)-epoch_us(lag(TRADINGTIME) OVER ("
                    "PARTITION BY raw_file_sha256,MARKET,SYMBOL,TRADINGDATE,session_id "
                    "ORDER BY TRADINGTIME)) AS gap_us FROM valid_times), "
                    "gap_stats AS (SELECT raw_file_sha256,MARKET,SYMBOL,TRADINGDATE,session_id,"
                    "count(*) AS interval_count,quantile_cont(gap_us/1000000.0,0.50) AS p50,"
                    "quantile_cont(gap_us/1000000.0,0.90) AS p90,"
                    "quantile_cont(gap_us/1000000.0,0.99) AS p99 FROM ordered "
                    "WHERE gap_us>0 GROUP BY ALL) "
                    "SELECT c.raw_file_sha256,c.MARKET,c.SYMBOL,c.TRADINGDATE,c.session_id,"
                    "c.unique_event_times,coalesce(g.interval_count,0),g.p50,g.p90,g.p99 "
                    "FROM group_counts c LEFT JOIN gap_stats g "
                    "USING(raw_file_sha256,MARKET,SYMBOL,TRADINGDATE,session_id) ORDER BY ALL",
                    [source_timezone],
                ).fetchall()
                for row in gap_rows:
                    key = (row[0], row[1], row[2], _date_text(row[3]), row[4])
                    interval_count = int(row[6])
                    actual_gaps[key] = {
                        "unique_valid_event_time_count": int(row[5]),
                        "positive_interval_count": interval_count,
                        "sample_count": interval_count,
                        "p50_seconds": float(row[7]) if row[7] is not None else None,
                        "p90_seconds": float(row[8]) if row[8] is not None else None,
                        "p99_seconds": float(row[9]) if row[9] is not None else None,
                    }

            row_metrics = connection.execute(
                "WITH flags AS (SELECT invalid_quote AS invalid_l1,"
                "has_invalid_l1_reason,"
                "list_contains(reason_codes,'SESSION_UNCLASSIFIED') AS out_of_session,"
                "(list_contains(reason_codes,'DUPLICATE_EVENT_KEY_IDENTICAL') OR "
                "list_contains(reason_codes,'DUPLICATE_EVENT_KEY_CONFLICTING')) AS duplicate_event,"
                "list_contains(reason_codes,'DUPLICATE_SOURCE_RECORD') AS source_duplicate "
                "FROM _consumer_batch_rows) SELECT count(*),"
                "count(*) FILTER(WHERE (invalid_l1 OR out_of_session OR duplicate_event)),"
                "count(*) FILTER(WHERE (invalid_l1::INTEGER+out_of_session::INTEGER+"
                "duplicate_event::INTEGER)>1),count(*) FILTER(WHERE invalid_l1),"
                "count(*) FILTER(WHERE out_of_session),count(*) FILTER(WHERE duplicate_event),"
                "count(*) FILTER(WHERE source_duplicate),"
                "count(*) FILTER(WHERE invalid_l1 IS DISTINCT FROM has_invalid_l1_reason) "
                "FROM flags"
            ).fetchone()
            if int(row_metrics[7]):
                raise VerificationError(
                    "source L1 validity differs from quarantine INVALID_L1_BOOK reason"
                )
            identity_audit_rows += int(row_metrics[0])
            three_rule_quarantine_union_rows += int(row_metrics[1])
            three_rule_overlap_rows += int(row_metrics[2])
            invalid_l1_reason_rows += int(row_metrics[3])
            session_unclassified_reason_rows += int(row_metrics[4])
            duplicate_event_reason_rows += int(row_metrics[5])
            source_duplicate_rows += int(row_metrics[6])
        finally:
            connection.execute("DROP TABLE IF EXISTS _consumer_batch_rows")
            connection.execute("DROP TABLE IF EXISTS _consumer_event_groups")
            connection.execute("DROP TABLE IF EXISTS _consumer_duplicate_members")

    if any(identity_seen.get(raw_hash, 0) != row_count
           for raw_hash, row_count in identity_expected.items()):
        identity_bad = True

    if duplicate_reason_category_mismatch_rows:
        raise VerificationError(
            "quarantine duplicate-event reason category differs from source payload variants: "
            f"mismatched_rows={duplicate_reason_category_mismatch_rows}"
        )

    duplicate_report = quality.get("duplicate_classification")
    if not isinstance(duplicate_report, dict):
        raise VerificationError("quality_report.duplicate_classification must be an object")
    if duplicate_report.get("event_key") != ["exchange", "instrument_id", "trade_date", "event_time"]:
        raise VerificationError("quality_report.duplicate_classification event_key differs from the POC key")
    if duplicate_report.get("policy") != "quarantine_entire_group_without_sequence":
        raise VerificationError("quality_report.duplicate_classification policy is unsupported")
    for field, value in duplicate_classification.items():
        if duplicate_report.get(field) != value:
            raise VerificationError(
                f"quality_report.duplicate_classification {field} differs from source payloads: "
                f"reported={duplicate_report.get(field)}, observed={value}"
            )
    if duplicate_report.get("source_record_duplicate_rows") != source_duplicate_rows:
        raise VerificationError("quality_report.duplicate_classification source_record_duplicate_rows differs")

    reported_scopes = quality.get("scopes")
    if not isinstance(reported_scopes, list):
        raise VerificationError("quality_report.scopes must be a list")
    reported_by_key: dict[tuple[Any, ...], Mapping[str, Any]] = {}
    for item in reported_scopes:
        if not isinstance(item, dict) or item.get("scope") != "raw_file/contract/trade_date":
            raise VerificationError("quality_report.scopes contains a malformed scope")
        key = _scope_key(item)
        if key in reported_by_key:
            raise VerificationError(f"quality_report.scopes contains duplicate key {key}")
        reported_by_key[key] = item
    if set(reported_by_key) != set(actual_scopes):
        missing, extra = set(actual_scopes) - set(reported_by_key), set(reported_by_key) - set(actual_scopes)
        raise VerificationError(
            f"quality_report.scopes keys differ from serialized rows: missing={list(missing)[:5]}, extra={list(extra)[:5]}"
        )
    count_fields = (
        "row_count", "accepted_rows", "quarantined_rows", "duplicate_event_groups",
        "duplicate_event_extra_rows", "duplicate_member_rows", "invalid_quote_rows",
        "null_field_values", "null_field_value_denominator", "crossed_book_rows",
        "valid_price_pair_rows", "time_reorder_count", "time_reorder_denominator",
        "session_unclassified_rows", "depth_invalid_rows", "depth_invalid_source_rows",
    )
    for key, actual in actual_scopes.items():
        item = reported_by_key[key]
        if item.get("source_uri") != actual["source_uri"]:
            raise VerificationError(f"quality_report scope source_uri differs for {key}")
        for field in count_fields:
            value = item.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value != actual[field]:
                raise VerificationError(
                    f"quality_report scope {field} differs for {key}: reported={value}, observed={actual[field]}"
                )
        rows = actual["row_count"]
        _rate_matches(item.get("quarantine_rate"), actual["quarantined_rows"], rows,
                      "quarantined_rows / input row_count", f"scope {key}.quarantine_rate")
        _rate_matches(item.get("duplicate_rate"), actual["duplicate_event_extra_rows"], rows,
                      "sum(event_key_group_size - 1) / input row_count", f"scope {key}.duplicate_rate")
        _rate_matches(item.get("invalid_quote_rate"), actual["invalid_quote_rows"], rows,
                      "invalid L1 rows / input row_count", f"scope {key}.invalid_quote_rate")
        _rate_matches(item.get("null_rate"), actual["null_field_values"],
                      actual["null_field_value_denominator"],
                      "null source-field values / (input row_count * physical source field count)",
                      f"scope {key}.null_rate")
        _rate_matches(item.get("crossed_book_rate"), actual["crossed_book_rows"],
                      actual["valid_price_pair_rows"],
                      "crossed positive-price pairs / positive non-null L1 price pairs",
                      f"scope {key}.crossed_book_rate")
        stale = item.get("stale_quote_segments")
        if (not isinstance(stale, dict) or stale.get("status") != "NOT_EVALUATED"
                or stale.get("value") is not None or stale.get("denominator") is not None
                or not isinstance(stale.get("reason"), str) or not stale["reason"].strip()):
            raise VerificationError(f"scope {key}.stale_quote_segments must be explicitly NOT_EVALUATED with a reason")

    reported_sessions = quality.get("session_scopes")
    if not isinstance(reported_sessions, list):
        raise VerificationError("quality_report.session_scopes must be a list")
    reported_session_map: dict[tuple[Any, ...], Mapping[str, Any]] = {}
    for item in reported_sessions:
        if not isinstance(item, dict) or item.get("scope") != "raw_file/contract/trade_date/session":
            raise VerificationError("quality_report.session_scopes contains a malformed scope")
        key = _session_scope_key(item)
        if key in reported_session_map:
            raise VerificationError(f"quality_report.session_scopes contains duplicate key {key}")
        reported_session_map[key] = item
    if set(reported_session_map) != set(actual_sessions):
        missing = set(actual_sessions) - set(reported_session_map)
        extra = set(reported_session_map) - set(actual_sessions)
        raise VerificationError(
            f"quality_report.session_scopes keys differ from serialized rows: "
            f"missing={list(missing)[:5]}, extra={list(extra)[:5]}"
        )
    for key, actual in actual_sessions.items():
        item = reported_session_map[key]
        expected_class = "unclassified" if key[-1] is None else "evidenced_session"
        if item.get("classification") != expected_class:
            raise VerificationError(f"quality_report session classification differs for {key}")
        for field, value in actual.items():
            if item.get(field) != value:
                raise VerificationError(f"quality_report session scope {field} differs for {key}")

    reported_gaps = quality.get("event_time_gap_quantiles")
    if not isinstance(reported_gaps, list):
        raise VerificationError("quality_report.event_time_gap_quantiles must be a list")
    reported_gap_map: dict[tuple[Any, ...], Mapping[str, Any]] = {}
    for item in reported_gaps:
        if not isinstance(item, dict) or item.get("scope") != "raw_file/contract/trade_date/session":
            raise VerificationError("quality_report.event_time_gap_quantiles contains a malformed scope")
        key = _gap_scope_key(item)
        if key in reported_gap_map:
            raise VerificationError(f"quality_report.event_time_gap_quantiles contains duplicate key {key}")
        reported_gap_map[key] = item
    if set(reported_gap_map) != set(actual_gaps):
        missing = set(actual_gaps) - set(reported_gap_map)
        extra = set(reported_gap_map) - set(actual_gaps)
        raise VerificationError(
            f"quality_report.event_time_gap_quantiles keys differ from serialized rows: "
            f"missing={list(missing)[:5]}, extra={list(extra)[:5]}"
        )
    expected_gap_formula = (
        "positive adjacent deltas after distinct valid event_time values, grouped within one raw file and evidenced session"
    )
    for key, actual in actual_gaps.items():
        item = reported_gap_map[key]
        for field, value in actual.items():
            reported = item.get(field)
            if value is None:
                if reported is not None:
                    raise VerificationError(f"quality report gap {field} differs for {key}")
            elif (isinstance(reported, bool) or not isinstance(reported, (int, float))
                  or not math.isclose(float(reported), value, rel_tol=1e-12, abs_tol=1e-9)):
                raise VerificationError(f"quality report gap {field} differs for {key}")
        evaluated = actual["positive_interval_count"] > 0
        if item.get("formula") != expected_gap_formula:
            raise VerificationError(f"quality report gap formula differs for {key}")
        if item.get("status") != ("EVALUATED" if evaluated else "NOT_EVALUATED"):
            raise VerificationError(f"quality report gap status differs for {key}")
        if evaluated and item.get("reason") is not None:
            raise VerificationError(f"quality report evaluated gap has an unexpected reason for {key}")
        if not evaluated and (not isinstance(item.get("reason"), str) or not item["reason"].strip()):
            raise VerificationError(f"quality report unevaluated gap lacks reason for {key}")

    observed_source_trade_dates = sorted({
        key[3] for key in actual_scopes if isinstance(key[3], str) and key[3]
    })
    declared_source_trade_dates = quality.get("source_trade_dates")
    if (declared_source_trade_dates is not None and (
            not isinstance(declared_source_trade_dates, list)
            or any(not isinstance(day, str) for day in declared_source_trade_dates)
            or sorted(declared_source_trade_dates) != observed_source_trade_dates)):
        raise VerificationError(
            "quality_report.source_trade_dates differs from serialized input rows"
        )

    return {
        "scope_count": len(actual_scopes),
        "session_scope_count": len(actual_sessions),
        "event_time_gap_scope_count": len(actual_gaps),
        "source_trade_dates": observed_source_trade_dates,
        "duplicate_event_key_groups": duplicate_event_key_groups,
        "duplicate_event_group_member_rows": duplicate_event_group_member_rows,
        "duplicate_event_extra_rows": duplicate_event_extra_rows,
        **{key: int(value) for key, value in duplicate_classification.items()},
        "three_rule_quarantine_union_rows": three_rule_quarantine_union_rows,
        "three_rule_overlap_rows": three_rule_overlap_rows,
        "invalid_l1_reason_rows": invalid_l1_reason_rows,
        "session_unclassified_reason_rows": session_unclassified_reason_rows,
        "duplicate_event_reason_rows": duplicate_event_reason_rows,
        "repeated_event_keys_with_accepted": repeated_event_keys_with_accepted,
        "repeated_event_keys_missing_duplicate_reason": repeated_event_keys_missing_duplicate_reason,
        "identity_audit_rows": identity_audit_rows,
        "duplicate_or_missing_record_ids": int(identity_bad),
    }


def _check_entry_manifest(entry_path: Path, entry: Mapping[str, Any], manifest_path: Path | None) -> dict[str, Any] | None:
    candidate = manifest_path
    if candidate is None:
        for parent in (entry_path.parent, entry_path.parent.parent, entry_path.parent.parent.parent):
            sibling = parent / "manifest.json"
            if sibling.is_file():
                try:
                    value = _safe_json(sibling, "outer onboarding manifest")
                except VerificationError:
                    continue
                if value.get("uri") == entry_path.as_uri():
                    candidate = sibling
                    break
    if candidate is None:
        return None
    manifest = _safe_json(candidate, "outer onboarding manifest")
    if manifest.get("uri") != entry_path.as_uri():
        raise VerificationError("outer onboarding manifest URI does not point to onboarding.json")
    _validate_manifest_structure(manifest, "outer onboarding manifest")
    actual_hash = sha256_file(entry_path)
    if manifest.get("content_hash") != actual_hash:
        raise VerificationError("outer onboarding manifest content hash mismatch")
    if manifest.get("artifact_id") != "art_" + actual_hash.removeprefix("sha256:"):
        raise VerificationError("outer onboarding manifest artifact_id mismatch")
    for key in ("run_id", "stage_run_id", "stage_name", "config_hash", "code_revision",
                "data_snapshot_id", "split_id"):
        if entry.get(key) is not None and manifest.get(key) != entry.get(key):
            raise VerificationError(f"outer onboarding manifest {key} mismatch")
    return manifest


def verify_entry(
    entry_path: str | Path,
    *,
    entry_manifest_path: str | Path | None = None,
    checkpoint_path: str | Path | None = None,
    duckdb_memory_limit: str = "1GB",
    duckdb_threads: int = 4,
) -> dict[str, Any]:
    """Validate a READY entry and return a JSON-safe verification report."""
    if not isinstance(duckdb_memory_limit, str) or not DUCKDB_MEMORY_LIMIT_RE.fullmatch(
        duckdb_memory_limit.strip()
    ):
        raise VerificationError("DuckDB memory limit must be a positive integer with a unit (e.g. 8GB)")
    duckdb_memory_limit = duckdb_memory_limit.strip().upper().replace(" ", "")
    if isinstance(duckdb_threads, bool) or not isinstance(duckdb_threads, int) or not 1 <= duckdb_threads <= 64:
        raise VerificationError("DuckDB threads must be an integer from 1 through 64")
    entry_file = Path(entry_path).expanduser().resolve()
    entry = _safe_json(entry_file, "onboarding.json")
    required = ("schema_version", "stage_name", "status", "run_id", "stage_run_id",
                "config_hash", "code_revision", "split_id", "data_snapshot_id", "inputs",
                "passport", "quality_report", "partitions", "artifacts", "artifact_manifests", "warnings")
    missing = [key for key in required if key not in entry]
    if missing:
        raise VerificationError(f"onboarding.json missing required fields: {missing}")
    if entry["stage_name"] != "onboard" or entry["status"] not in {"READY", "READY_WITH_WARNINGS"}:
        raise VerificationError("onboarding entry is not a successful onboard READY status")
    for key in ("run_id", "stage_run_id", "config_hash", "code_revision", "data_snapshot_id"):
        if not isinstance(entry[key], str) or not entry[key]:
            raise VerificationError(f"onboarding.{key} must be a non-empty string")
    if entry.get("split_id") is not None and not isinstance(entry["split_id"], str):
        raise VerificationError("onboarding.split_id must be a string or null")
    inputs = entry["inputs"]
    passport = entry["passport"]
    quality = entry["quality_report"]
    partitions = entry["partitions"]
    artifacts = entry["artifacts"]
    if not all(isinstance(value, dict) for value in (inputs, passport, quality, artifacts)):
        raise VerificationError("inputs, passport, quality_report and artifacts must be objects")
    if not isinstance(partitions, list) or not isinstance(entry["warnings"], list):
        raise VerificationError("partitions and warnings must be lists")
    if passport.get("exchange") != "CFFEX":
        raise VerificationError("passport.exchange must be CFFEX for this POC")
    availability = str(passport.get("availability_status", "")).casefold()
    if availability not in {"unknown", "unverified", "unknown_availability"} or passport.get("research_only") is not True:
        raise VerificationError("unknown availability must remain research_only")
    warning_codes = {w.get("code") for w in entry["warnings"] if isinstance(w, dict)}
    if "UNKNOWN_AVAILABILITY" not in warning_codes:
        raise VerificationError("onboarding warnings must include UNKNOWN_AVAILABILITY")

    source_info = _validate_input_evidence(entry_file, inputs)
    timezone_name = source_info["source_timezone"]
    source_types = source_info["quote_source_types"]
    quote_expected_types = {**source_types, **QUOTE_DERIVED_COLUMNS, **QUOTE_CORE_COLUMNS}
    quarantine_expected_types = {
        **{field["name"]: _arrow_type(field["arrow_type"])
           for field in source_info["source_schema"]["sample"]["fields"]},
        **QUARANTINE_DERIVED_COLUMNS,
    }
    refs_by_alias = _all_refs(artifacts)
    all_refs = [ref for refs in refs_by_alias.values() for ref in refs]
    sidecars: dict[tuple[str, str], dict[str, Any]] = {}
    paths_by_alias: dict[str, list[tuple[dict[str, Any], Path, list[Path], dict[str, Any]]]] = defaultdict(list)
    files_hashed = 0
    bytes_hashed = 0
    quote_paths: list[Path] = []
    quote_paths_by_date: dict[str, list[Path]] = defaultdict(list)
    quarantine_paths: list[Path] = []
    calendar_path: Path | None = None
    session_rules_path: Path | None = None
    for alias, refs in refs_by_alias.items():
        for ref in refs:
            path, sidecar = _verify_ref(ref, entry)
            key = (ref["artifact_id"], ref["uri"])
            if key in sidecars:
                raise VerificationError(f"duplicate output artifact ref {key}")
            sidecars[key] = sidecar
            bytes_hashed += payload_size(path)
            files = _parquet_paths(path) if alias in {"quotes", "quarantine", "calendar"} else []
            if alias == "session_rules":
                if path.is_dir():
                    raise VerificationError("session_rules artifact must be a single JSON file")
                session_rules_path = path
            elif alias == "calendar":
                if path.is_dir() or path.suffix.lower() != ".parquet":
                    raise VerificationError("calendar artifact must reference one calendar.parquet file")
                calendar_path = path
            elif alias == "quotes":
                if path.is_dir() or path.suffix.lower() != ".parquet":
                    raise VerificationError("each quotes ArtifactRef must reference one Parquet file")
                quote_paths.append(path)
                trade_day = ref.get("partition_keys", {}).get("trade_date")
                if not isinstance(trade_day, str) or not trade_day:
                    raise VerificationError("quote ArtifactRef requires a trade_date partition key")
                quote_paths_by_date[trade_day].append(path)
            elif alias == "quarantine":
                quarantine_paths.extend(files)
            parquet_rows = 0
            for parquet_path in files:
                if alias == "quotes":
                    parquet = _check_schema(parquet_path, quote_expected_types, "quotes")
                elif alias == "quarantine":
                    parquet = _check_schema(parquet_path, quarantine_expected_types, "quarantine")
                else:
                    parquet = _check_schema(parquet_path,
                                            {"calendar_id": pa.string(), "trade_date": pa.date32()},
                                            "calendar")
                parquet_rows += parquet.metadata.num_rows
                files_hashed += 1
            if alias in {"quotes", "quarantine", "calendar"}:
                _manifest_row_count(ref, parquet_rows, alias)
            paths_by_alias[alias].append((ref, path, files, sidecar))

    embedded = _flatten_embedded_manifests(entry["artifact_manifests"])
    if not embedded:
        raise VerificationError("artifact_manifests must contain a full manifest for each output ref")
    _verify_embedded_manifests(entry["artifact_manifests"], all_refs, sidecars)
    if any(manifest.get("uri") == entry_file.as_uri() for manifest in embedded):
        raise VerificationError("onboarding.json must not embed its own manifest")
    entry_manifest = _check_entry_manifest(
        entry_file, entry,
        Path(entry_manifest_path).expanduser().resolve() if entry_manifest_path else None,
    )
    if not quote_paths or not quarantine_paths or calendar_path is None or session_rules_path is None:
        raise VerificationError("published refs must include quotes, quarantine, calendar and session_rules")
    if len(quote_paths) != len(partitions):
        raise VerificationError("partitions count must match quotes ArtifactRef count")

    # Validate partition index refs and metadata before any aggregate scan.
    quote_refs_by_key = {
        (ref.get("partition_keys", {}).get("instrument_id"),
         ref.get("partition_keys", {}).get("trade_date")): ref
        for ref in refs_by_alias["quotes"]
    }
    seen_partitions: set[tuple[str, str]] = set()
    partition_rows = 0
    for part in partitions:
        if not isinstance(part, dict):
            raise VerificationError("partitions contains a non-object entry")
        key = (part.get("instrument_id"), str(part.get("trade_date")))
        if not isinstance(key[0], str) or not key[0] or not key[1]:
            raise VerificationError("partition requires instrument_id and trade_date")
        if key in seen_partitions:
            raise VerificationError(f"duplicate partition index entry: {key}")
        seen_partitions.add(key)
        ref = part.get("artifact_ref")
        if not isinstance(ref, dict) or (ref.get("artifact_id"), ref.get("uri")) not in sidecars:
            raise VerificationError(f"partition {key} has a broken artifact_ref")
        if quote_refs_by_key.get(key) != ref:
            raise VerificationError(f"partition {key} artifact_ref does not match artifacts.quotes")
        if ref.get("partition_keys", {}).get("exchange") != part.get("exchange"):
            raise VerificationError(f"partition {key} exchange does not match its ArtifactRef")
        if ref.get("partition_keys", {}).get("instrument_id") != part.get("instrument_id"):
            raise VerificationError(f"partition {key} instrument_id does not match its ArtifactRef")
        if str(ref.get("partition_keys", {}).get("trade_date")) != key[1]:
            raise VerificationError(f"partition {key} trade_date does not match its ArtifactRef")
        if not isinstance(part.get("row_count"), int) or isinstance(part.get("row_count"), bool):
            raise VerificationError(f"partition {key} row_count must be an integer")
        _manifest_row_count(ref, part["row_count"], f"partition {key}")
        partition_rows += part["row_count"]
    if seen_partitions != set(quote_refs_by_key):
        raise VerificationError("partition index does not cover all quotes refs")

    session_rules = _validate_session_rules(session_rules_path, timezone_name)
    _validate_partition_session_coverage(session_rules, partitions)
    if session_rules["evidence_sha256"] != inputs["session_evidence_ref"]["sha256"]:
        raise VerificationError("session_rules evidence hash differs from inputs.session_evidence_ref")
    temporary_parent = REPO_ROOT / "artifacts" / "verification"
    temporary_parent.mkdir(parents=True, exist_ok=True)
    temporary_directory = tempfile.TemporaryDirectory(
        prefix="duckdb-consumer-", dir=temporary_parent
    )
    connection = duckdb.connect(database=":memory:", config={
        "memory_limit": duckdb_memory_limit,
        "threads": str(duckdb_threads),
        "temp_directory": temporary_directory.name,
    })
    _register_input_file_policy(connection, source_info["raw_files"])
    checkpoint_records, checkpoint_stream, checkpoint_status = _open_verification_checkpoint(
        checkpoint_path,
        _checkpoint_binding(entry_file, entry_manifest, refs_by_alias, sidecars),
    )
    quality_check_counts: Counter[str] = Counter()
    observed_quarantine_reasons: dict[str, int] = {}
    observed_depth_invalid_rows = 0
    observed_quality_flags: dict[str, int] = {}
    quote_rows = 0
    quarantine_rows = 0
    quote_dates: set[str] = set()
    per_partition: list[dict[str, Any]] = []
    scoped_quality_summary: dict[str, int] = {}
    try:
        calendar_ids, calendar_dates = _calendar_dates(connection, calendar_path)
        if calendar_ids != [source_info["calendar_id"]]:
            raise VerificationError("calendar.parquet ID differs from frozen provider provenance")
        if not set(source_info["expected_calendar_dates"]).issubset(calendar_dates):
            raise VerificationError("calendar.parquet does not cover raw dates in frozen CFFEX evidence")
        if source_info["frozen_calendar_dates"] is not None:
            if calendar_dates != source_info["frozen_calendar_dates"]:
                raise VerificationError("calendar.parquet differs from the verified frozen snapshot")
        elif calendar_dates != _evidenced_cffex_dates(
            date.fromisoformat(calendar_dates[0]), date.fromisoformat(calendar_dates[-1]),
            source_info["calendar_evidence"],
        ):
            raise VerificationError("calendar.parquet contains dates outside frozen CFFEX evidence")
        session_partitions = _quarantine_session_partitions(
            connection, quarantine_paths, inputs["mapping"], partitions
        )
        _register_session_intervals(connection, session_rules, session_partitions,
                                    calendar_dates, calendar_ids[0])
        for ref, _, files, _ in paths_by_alias["quotes"]:
            parquet_path = files[0]
            partition_keys = ref["partition_keys"]
            part_key = (partition_keys["instrument_id"], str(partition_keys["trade_date"]))
            expected_exchange = partition_keys.get("exchange")
            if expected_exchange != "CFFEX":
                raise VerificationError(f"quote artifact partition exchange is not CFFEX: {part_key}")
            cached = _checkpoint_scan(checkpoint_records, "quote", ref)
            if cached is not None and cached.get("sorted") is True and isinstance(cached.get("scan"), dict):
                scan = cached["scan"]
            else:
                scan = _scan_quote_file(
                    connection, parquet_path, timezone_name, expected_exchange,
                    part_key[0], part_key[1], calendar_ids[0],
                )
                _assert_sorted_quote_file(parquet_path)
                _append_checkpoint_record(
                    checkpoint_stream, checkpoint_records, "quote", ref,
                    {"scan": scan, "sorted": True},
                )
            rows = int(scan["rows"])
            quote_rows += rows
            observed_depth_invalid_rows += int(scan["depth_invalid_rows"])
            if int(scan["available_rows"]) != 0:
                raise VerificationError(f"available_time is non-null in {parquet_path}")
            if int(scan["missing_identity_rows"]) or int(scan["missing_l1_rows"]):
                raise VerificationError(f"quote file has required identity/L1 nulls: {parquet_path}")
            if int(scan["invalid_l1_rows"]):
                raise VerificationError(f"accepted quote file contains invalid one-level books: {parquet_path}")
            if int(scan["missing_quality_flags"]):
                raise VerificationError(f"accepted quote file contains null quality_flags: {parquet_path}")
            if int(scan["bad_depth_quality_flags"]):
                raise VerificationError(f"DEPTH_BOOK_INVALID flags disagree with level 2-5 values: {parquet_path}")
            if int(scan["bad_record_ids"]):
                raise VerificationError(f"record_id/source provenance mismatch in {parquet_path}")
            if int(scan["bad_event_unix_rows"]):
                raise VerificationError(f"event_time and source_event_unix_ms disagree in {parquet_path}")
            if int(scan["bad_trade_date_rows"]):
                raise VerificationError(f"event_time local date and trade_date disagree in {parquet_path}")
            if int(scan["bad_session_ids"]):
                raise VerificationError(f"session_id is stale or mismatched with trade_date in {parquet_path}")
            if int(scan["bad_calendar_session_ids"]):
                raise VerificationError(f"session_id calendar prefix disagrees in {parquet_path}")
            if int(scan["bad_session_classification_rows"]):
                raise VerificationError(f"event_time does not match session_rules/session_id in {parquet_path}")
            if int(scan["bad_partition_rows"]):
                raise VerificationError(f"quote file has missing partition keys: {parquet_path}")
            if int(scan["bad_partition_values"]):
                raise VerificationError(f"quote values do not match artifact partition keys {part_key}")
            _manifest_row_count(ref, rows, f"quotes {parquet_path}")
            _compare_time_bounds(ref, scan["time_min"], scan["time_max"], f"quotes {parquet_path}")
            matching_part = next(part for part in partitions
                                 if (part["instrument_id"], str(part["trade_date"])) == part_key)
            _compare_partition_time_bounds(
                matching_part, scan["time_min"], scan["time_max"], f"partition {part_key}"
            )
            quote_dates.add(part_key[1])
            per_partition.append({"instrument_id": part_key[0], "trade_date": part_key[1],
                                  "row_count": rows, "event_time_min": str(scan["time_min"]),
                                  "event_time_max": str(scan["time_max"])})

        for ref, _, files, _ in paths_by_alias["quarantine"]:
            file_rows = 0
            for parquet_path in files:
                relative = parquet_path.relative_to(file_uri_path(ref["uri"])).as_posix() \
                    if file_uri_path(ref["uri"]).is_dir() else parquet_path.name
                scan_ref = {
                    "artifact_id": f"{ref['artifact_id']}::{relative}",
                    "uri": f"{ref['uri'].rstrip('/')}/{relative}",
                    "content_hash": ref["content_hash"],
                }
                scan = _checkpoint_scan(checkpoint_records, "quarantine", scan_ref)
                if scan is None:
                    scan = _scan_quarantine_file(connection, parquet_path)
                    _append_checkpoint_record(
                        checkpoint_stream, checkpoint_records, "quarantine", scan_ref, scan,
                    )
                rows = int(scan["rows"])
                file_rows += rows
                if int(scan["bad_record_ids"]):
                    raise VerificationError(f"quarantine record_id/source provenance mismatch: {parquet_path}")
                if int(scan["missing_reason_rows"]) or int(scan["missing_source_key_rows"]):
                    raise VerificationError(f"quarantine row lacks reason/source identity: {parquet_path}")
            _manifest_row_count(ref, file_rows, "quarantine")
            quarantine_rows += file_rows

        scoped_quality_summary = _validate_scoped_quality_report_batched(
            connection, quote_paths_by_date, quarantine_paths,
            source_info["source_schema"], inputs["mapping"], timezone_name,
            source_info["raw_files"], quality, calendar_dates,
        )
        identity_audit = {
            "rows": scoped_quality_summary["identity_audit_rows"],
            "repeated_event_keys_with_accepted": scoped_quality_summary["repeated_event_keys_with_accepted"],
            "repeated_event_keys_missing_duplicate_reason":
                scoped_quality_summary["repeated_event_keys_missing_duplicate_reason"],
            "duplicate_or_missing_record_ids": scoped_quality_summary["duplicate_or_missing_record_ids"],
        }
        if int(identity_audit["rows"]) != quote_rows + quarantine_rows:
            raise VerificationError("global accepted/quarantine audit row count disagrees with file scans")
        if int(identity_audit["repeated_event_keys_with_accepted"]):
            raise VerificationError("repeated event-key groups must be fully quarantined; accepted rows remain")
        if int(identity_audit["repeated_event_keys_missing_duplicate_reason"]):
            raise VerificationError("quarantined repeated event-key groups lack duplicate reason codes")
        if int(identity_audit["duplicate_or_missing_record_ids"]):
            raise VerificationError("record_id values are missing or duplicated across outputs")
        observed_quarantine_reasons = _quarantine_reason_counts(connection, quarantine_paths)
        observed_quality_flags = _accepted_quality_flag_counts(connection, quote_paths)
        quote_trade_dates = sorted(quote_dates)
    finally:
        connection.close()
        temporary_directory.cleanup()
        if checkpoint_stream is not None:
            checkpoint_stream.close()

    input_rows = quality.get("input_rows")
    accepted_rows = quality.get("accepted_rows")
    declared_quarantine = quality.get("quarantined_rows")
    for name, value in (("input_rows", input_rows), ("accepted_rows", accepted_rows),
                        ("quarantined_rows", declared_quarantine)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise VerificationError(f"quality_report.{name} must be a non-negative integer")
    if (input_rows, accepted_rows, declared_quarantine) != (
        quote_rows + quarantine_rows, quote_rows, quarantine_rows
    ):
        raise VerificationError("QualityReport counts do not match accepted/quarantine artifact rows")
    if source_info["raw_input_rows"] != input_rows:
        raise VerificationError("sum(inputs.raw_files.row_count) does not equal quality_report.input_rows")
    reported_reasons = quality.get("quarantine_reason_counts")
    if not isinstance(reported_reasons, dict):
        raise VerificationError("quality_report.quarantine_reason_counts must be an object")
    allowed_reported_reasons = set(QUARANTINE_REASON_CODES)
    if set(reported_reasons) - allowed_reported_reasons:
        raise VerificationError("quality_report.quarantine_reason_counts has unknown reason codes")
    for reason, count in reported_reasons.items():
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise VerificationError(f"quarantine reason count {reason} must be non-negative integer")
    expected_reasons = {
        reason: int(reported_reasons.get(reason, 0))
        for reason in QUARANTINE_REASON_CODES
        if reported_reasons.get(reason, 0) != 0
    }
    if observed_quarantine_reasons != expected_reasons:
        raise VerificationError(
            "quality_report.quarantine_reason_counts differs from observed quarantine rows: "
            f"reported={expected_reasons}, observed={observed_quarantine_reasons}"
        )
    reported_quality_flags = quality.get("quality_flag_counts")
    if not isinstance(reported_quality_flags, dict):
        raise VerificationError("quality_report.quality_flag_counts must be an object")
    for flag, count in reported_quality_flags.items():
        if not isinstance(flag, str) or not flag:
            raise VerificationError("quality flag count keys must be non-empty strings")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise VerificationError(f"quality flag count {flag} must be a non-negative integer")
    reported_nonzero_flags = {flag: count for flag, count in reported_quality_flags.items() if count}
    if reported_nonzero_flags != observed_quality_flags:
        raise VerificationError(
            "quality_report.quality_flag_counts differs from accepted quote quality_flags: "
            f"reported={reported_nonzero_flags}, observed={observed_quality_flags}"
        )
    if reported_quality_flags.get("DEPTH_BOOK_INVALID", 0) != observed_depth_invalid_rows:
        raise VerificationError(
            "quality_report.quality_flag_counts DEPTH_BOOK_INVALID differs from accepted quote rows"
        )
    if partition_rows != quote_rows:
        raise VerificationError("partition index row counts do not equal accepted quotes row count")
    if quote_rows <= 0:
        raise VerificationError("zero accepted rows cannot be published as a downstream READY dataset")
    if calendar_ids[0] != passport.get("calendar_id", inputs.get("calendar", {}).get("calendar_id", calendar_ids[0])):
        raise VerificationError("passport/inputs calendar_id does not match calendar.parquet")
    if not quote_dates.issubset(set(calendar_dates)):
        missing_calendar = sorted(quote_dates - set(calendar_dates))
        raise VerificationError(f"accepted quote dates absent from frozen calendar: {missing_calendar[:20]}")
    source_trade_dates = scoped_quality_summary["source_trade_dates"]
    expected_missing_dates = sorted(set(calendar_dates) - set(source_trade_dates))
    declared_missing = quality.get("expected_missing_dates")
    if declared_missing is not None and sorted(map(str, declared_missing)) != expected_missing_dates:
        raise VerificationError(
            "quality_report.expected_missing_dates differs from observed source-date coverage"
        )
    declared_calendar_missing = quality.get("calendar_dates_without_source_rows")
    if (declared_calendar_missing is not None
            and sorted(map(str, declared_calendar_missing)) != expected_missing_dates):
        raise VerificationError(
            "quality_report.calendar_dates_without_source_rows differs from source-date coverage"
        )

    rules = quality.get("rules", [])
    if not isinstance(rules, list):
        raise VerificationError("quality_report.rules must be a list")
    rules_by_id: dict[str, dict[str, Any]] = {}
    for rule in rules:
        if not isinstance(rule, dict) or rule.get("outcome") not in {
            "PASS", "WARN", "FAIL", "NOT_EVALUATED", "NOT_APPLICABLE"
        }:
            raise VerificationError("quality_report.rules contains an invalid rule/outcome")
        rule_id = rule.get("rule_id")
        if not isinstance(rule_id, str) or not rule_id or rule_id in rules_by_id:
            raise VerificationError("quality_report.rules has a missing or duplicate rule_id")
        rules_by_id[rule_id] = rule
        if rule["outcome"] == "FAIL":
            raise VerificationError(f"quality_report contains FAIL outcome for {rule_id}")
        quality_check_counts[rule.get("outcome", "UNKNOWN")] += 1
    expected_rule_hits = {
        **{reason: (observed_quarantine_reasons.get(reason, 0), input_rows)
           for reason in QUARANTINE_REASON_CODES},
        "DEPTH_BOOK_INVALID": (observed_depth_invalid_rows, accepted_rows),
    }
    for rule_id, (hit_count, denominator) in expected_rule_hits.items():
        rule = rules_by_id.get(rule_id)
        if rule is None:
            raise VerificationError(f"quality_report.rules is missing required row rule {rule_id}")
        if rule.get("hit_count") != hit_count or rule.get("denominator") != denominator:
            raise VerificationError(
                f"quality_report rule {rule_id} counts differ from serialized rows: "
                f"reported={rule.get('hit_count')}/{rule.get('denominator')}, "
                f"observed={hit_count}/{denominator}"
            )
        expected_scope = "accepted_quote_row" if rule_id == "DEPTH_BOOK_INVALID" else "input_row"
        if rule.get("scope") != expected_scope:
            raise VerificationError(
                f"quality_report rule {rule_id} scope must be {expected_scope!r}"
            )
        expected_outcome = "WARN" if hit_count else "PASS"
        if rule.get("outcome") != expected_outcome:
            raise VerificationError(f"quality_report rule {rule_id} outcome disagrees with observed hit count")

    # Use a tiny on-demand scan for consumer semantics; never materialize a complete table.
    sample_connection = duckdb.connect(database=":memory:", config={
        "memory_limit": duckdb_memory_limit,
        "threads": str(duckdb_threads),
    })
    try:
        sample = _sample_rows(sample_connection, quote_paths)
    finally:
        sample_connection.close()
    consumer_sample = []
    for row in sample:
        metadata = _metadata_for_instrument(passport, str(row["instrument_id"]))
        metrics = calculate_l1(row["bid_price_1"], row["ask_price_1"], row["bid_size_1"],
                               row["ask_size_1"], metadata.get("tick_size"))
        consumer_sample.append({"instrument_id": row["instrument_id"],
                                "trade_date": _date_text(row["trade_date"]),
                                "session_id": row["session_id"],
                                "event_time": row["event_time"].isoformat(), **metrics})
    tick_metadata = passport.get("instrument_metadata", [])
    if isinstance(tick_metadata, dict):
        tick_metadata = list(tick_metadata.values())
    unknown_tick_sizes = sorted(
        str(item.get("instrument_id")) for item in tick_metadata
        if isinstance(item, dict) and item.get("tick_size") is None
    )
    metadata_ids = {
        str(item.get("instrument_id")) for item in tick_metadata if isinstance(item, dict)
    }
    unknown_tick_sizes = sorted(set(unknown_tick_sizes) | {
        str(part["instrument_id"]) for part in partitions
        if str(part["instrument_id"]) not in metadata_ids
    })
    return {
        "schema_version": "1.0.0",
        "status": "PASS",
        "entry_uri": entry_file.as_uri(),
        "run_id": entry["run_id"],
        "data_snapshot_id": entry["data_snapshot_id"],
        "source": {
            "schema_ref": inputs["schema_ref"],
            "schema_sha256": inputs["schema_sha256"],
            "schema_field_count": len(source_info["source_schema"]["sample"]["fields"]),
            "raw_files": [
                {key: item.get(key) for key in (
                    "uri", "sha256", "size_bytes", "row_count", "source_format",
                    "row_group_count", "trade_date_min", "trade_date_max", "trade_date_count",
                )}
                for item in source_info["raw_files"]
            ],
            "contracts": sorted({str(part["instrument_id"]) for part in partitions}),
            "quote_trade_dates": quote_trade_dates,
            "declared_source_trade_dates": quality.get("source_trade_dates"),
            "observed_source_trade_dates": source_trade_dates,
        },
        "quality_summary": {
            "input_rows": input_rows,
            "accepted_rows": accepted_rows,
            "quarantined_rows": declared_quarantine,
            "partition_count": len(partitions),
            "contract_count": len({part["instrument_id"] for part in partitions}),
            "calendar_id": calendar_ids[0],
            "calendar_dates": len(calendar_dates),
            "quote_dates": len(quote_trade_dates),
            "quality_rule_outcomes": dict(quality_check_counts),
            "cross_output_duplicate_event_keys": 0,
            "bad_record_ids": 0,
            "quarantine_reason_counts": observed_quarantine_reasons,
            "quality_flag_counts": observed_quality_flags,
            "depth_invalid_rows": observed_depth_invalid_rows,
            **scoped_quality_summary,
        },
        "verified_quality_rule_counts": {
            rule_id: {"hit_count": hit_count, "denominator": denominator}
            for rule_id, (hit_count, denominator) in expected_rule_hits.items()
        },
        "expected_missing_dates": expected_missing_dates,
        "availability": {
            "status": passport["availability_status"],
            "research_only": True,
            "non_null_available_time_rows": 0,
            "verified_point_in_time": False,
        },
        "capabilities": {
            "spread_price_units": True,
            "spread_ticks_disabled_instruments": unknown_tick_sizes,
            "cross_source_point_in_time_causality": False,
        },
        "session_rules": {"version": session_rules["version"],
                          "timezone": session_rules["timezone"],
                          "rule_count": len(session_rules["rules"])},
        "consumer_sample": consumer_sample,
        "session_local_intervals": session_local_intervals(sample),
        "verified_outer_entry_manifest": entry_manifest is not None,
        "verification_limits": {
            "duckdb_memory_limit": duckdb_memory_limit,
            "duckdb_threads": duckdb_threads,
            "duckdb_temp_storage": "ephemeral directory under artifacts/verification",
            "payload_bytes_stream_hashed": bytes_hashed,
            "parquet_files": files_hashed,
            "row_materialization": "bounded sample only; full data scanned with DuckDB",
            "checkpoint_status": checkpoint_status,
            "checkpoint_scan_count": len(checkpoint_records),
        },
        "verified_artifact_count": len(all_refs),
        "verified_artifact_ids": sorted(ref["artifact_id"] for ref in all_refs),
        "partition_summaries": sorted(per_partition,
                                       key=lambda item: (item["instrument_id"], item["trade_date"])),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--entry", required=True, help="published onboarding.json path")
    parser.add_argument("--report", help="write the JSON verification report to this path")
    parser.add_argument("--entry-manifest", help="optional workflow manifest sidecar for onboarding.json")
    parser.add_argument(
        "--checkpoint",
        help="optional append-only scan checkpoint; reuse requires unchanged entry, manifests, payload hashes and verifier source",
    )
    parser.add_argument(
        "--duckdb-memory-limit", default="1GB",
        help="DuckDB memory cap (default: 1GB; for example: 8GB)",
    )
    parser.add_argument(
        "--duckdb-threads", default=4, type=int,
        help="DuckDB worker threads, 1 through 64 (default: 4)",
    )
    args = parser.parse_args(argv)
    try:
        report = verify_entry(
            args.entry,
            entry_manifest_path=args.entry_manifest,
            checkpoint_path=args.checkpoint,
            duckdb_memory_limit=args.duckdb_memory_limit,
            duckdb_threads=args.duckdb_threads,
        )
        code = 0
    except (VerificationError, OSError, duckdb.Error, pa.ArrowException, ValueError, TypeError) as exc:
        report = {"schema_version": "1.0.0", "status": "FAIL", "entry": str(args.entry),
                  "errors": [{"type": type(exc).__name__, "message": str(exc)}]}
        code = 1
    encoded = json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False)
    if args.report:
        report_path = Path(args.report).expanduser().resolve()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
