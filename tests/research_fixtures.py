"""Small, serialized Study/DataSource fixtures for the research management tests."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False)


def _sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _write_json(path: Path, value: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_canonical_json(value) + "\n", encoding="utf-8")
    return path


def _file_ref(path: Path) -> dict[str, str]:
    resolved = path.resolve()
    return {"uri": resolved.as_uri(), "content_hash": _sha256(resolved.read_bytes())}


def refresh_source_fixture_refs(fixture: SourceFixture, *, refresh_split: bool = False) -> None:
    """Refresh artifact refs and the enclosing source manifest after semantic edits.

    Tamper tests intentionally do not call this helper. Contract tests use it so
    validation reaches the artifact contents instead of stopping at stale hashes.
    """
    document = fixture.document
    for key, field in (("schema", "schema_ref"), ("calendar", "calendar_ref"),
                       ("session_policy", "session_policy_ref"),
                       ("market_metadata", "market_metadata_ref")):
        path = fixture.paths.get(key)
        if path is not None:
            document[field] = _file_ref(path)

    if refresh_split:
        access_path = fixture.paths["access_policy"]
        access_policy = json.loads(access_path.read_text(encoding="utf-8"))
        access_policy["split_refs"] = [_file_ref(fixture.paths["split"])]
        _write_json(access_path, access_policy)
    document["access_policy_ref"] = _file_ref(fixture.paths["access_policy"])

    manifest = json.loads(fixture.paths["manifest"].read_text(encoding="utf-8"))
    binding_fields = ("schema_ref", "calendar_ref", "session_policy_ref", "access_policy_ref",
                      "market_metadata_ref")
    manifest["bindings"] = {field: document[field] for field in binding_fields
                             if field in document}
    manifest_path = _write_json(fixture.paths["manifest"], manifest)
    document["manifest_ref"] = _file_ref(manifest_path)


@dataclass(frozen=True)
class SourceFixture:
    """A source document plus the real local artifacts it references."""

    document: dict[str, Any]
    data_ref: dict[str, str]
    data_path: Path
    paths: dict[str, Path]


def market_metadata_document(
    *,
    metadata_id: str = "synthetic_cffex_metadata",
    version: int = 1,
    product: str = "T",
    valid_from: str = "2024-01-01",
    valid_to: str | None = None,
    tick_size: str = "0.005",
) -> dict[str, Any]:
    """Build a valid metadata document with a single, explicit tick rule."""
    return {
        "metadata_id": metadata_id,
        "version": version,
        "provided_by": {"kind": "human", "id": "fixture-user"},
        "source": "Synthetic test fixture; no market measurements are represented.",
        "tick_rules": [{
            "product": product,
            "valid_from": valid_from,
            "valid_to": valid_to,
            "tick_size": tick_size,
            "unit": "quoted_price_unit",
        }],
    }


def make_source_fixture(
    directory: Path,
    *,
    source_id: str = "synthetic_l2",
    version: int = 1,
    instrument_id: str = "T2406",
    trade_dates: tuple[str, ...] = ("2024-06-03", "2024-06-04"),
    include_metadata: bool = False,
    metadata_document: dict[str, Any] | None = None,
    data_bytes: bytes = b"synthetic serialized market data\n",
    shared_data_ref: dict[str, str] | None = None,
    shared_data_path: Path | None = None,
) -> SourceFixture:
    """Create a complete source tree with hashed JSON artifacts and data bytes.

    The fixture deliberately uses a local ``file://`` URI for every artifact and
    leaves ``available_time`` absent, so source identity and unknown availability
    are exercised without inventing access-time information.
    """
    root = directory / f"{source_id}-v{version}"
    root.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    if shared_data_ref is None:
        data_path = root / "market-data.bin"
        data_path.write_bytes(data_bytes)
        data_ref = _file_ref(data_path)
    else:
        if shared_data_path is None:
            raise ValueError("shared_data_path is required with shared_data_ref")
        data_path = shared_data_path
        data_ref = dict(shared_data_ref)

    paths["data"] = data_path
    calendar_path = _write_json(root / "calendar.json", {
        "schema_version": "calendar_v1",
        "calendar_id": "synthetic_cffex_calendar",
        "timezone": "Asia/Shanghai",
        "trading_dates": list(trade_dates),
    })
    schema_path = _write_json(root / "schema.json", {
        "schema_version": "source_schema_v1",
        "fields": {
            "event_time": "timestamp[us, tz=UTC]",
            "instrument_id": "string",
            "last_price": "float64",
        },
    })
    sessions_path = _write_json(root / "session-policy.json", {
        "schema_version": "session_policy_v1",
        "session_policy_id": "synthetic_cffex_sessions",
        "sessions": ["day", "night"],
    })
    split_path = _write_json(root / "split-train.json", {
        "schema_version": "split_manifest_v1",
        "split_id": "synthetic_train_v1",
        "role": "train",
    })
    access_path = _write_json(root / "access-policy.json", {
        "schema_version": "access_policy_v1",
        "access_policy_id": "synthetic_research_access",
        "allowed_uses": ["research"],
        "split_refs": [_file_ref(split_path)],
    })
    paths.update({
        "calendar": calendar_path,
        "schema": schema_path,
        "session_policy": sessions_path,
        "split": split_path,
        "access_policy": access_path,
    })

    refs = {
        "schema_ref": _file_ref(schema_path),
        "calendar_ref": _file_ref(calendar_path),
        "session_policy_ref": _file_ref(sessions_path),
        "access_policy_ref": _file_ref(access_path),
    }
    if metadata_document is not None or include_metadata:
        metadata = metadata_document or market_metadata_document()
        metadata_path = _write_json(root / "market-metadata.json", metadata)
        refs["market_metadata_ref"] = _file_ref(metadata_path)
        paths["market_metadata"] = metadata_path

    data_artifacts = [data_ref]
    content_hash = _sha256(_canonical_json(data_artifacts).encode("utf-8"))
    manifest = {
        "schema_version": "data_source_manifest_v1",
        "source_id": source_id,
        "version": version,
        "content_hash": content_hash,
        "data_artifacts": data_artifacts,
        "bindings": refs,
    }
    manifest_path = _write_json(root / "manifest.json", manifest)
    paths["manifest"] = manifest_path

    document = {
        "schema_version": "data_source_v1",
        "source_id": source_id,
        "version": version,
        "name": "Synthetic Treasury Futures L2",
        "manifest_ref": _file_ref(manifest_path),
        "content_hash": content_hash,
        "schema_ref": refs["schema_ref"],
        "format": "binary_fixture",
        "instruments": [instrument_id],
        "coverage": {
            "partitioning": "trade_date_v1",
            "partitions": [
                {"instrument_id": instrument_id, "trade_date": day,
                 "session_ids": ["day", "night"]}
                for day in trade_dates
            ],
        },
        "time_rules": {"timezone": "Asia/Shanghai", "event_time_field": "event_time"},
        "calendar_ref": refs["calendar_ref"],
        "session_policy_ref": refs["session_policy_ref"],
        "availability_mode": "research_only",
        "access_policy_ref": refs["access_policy_ref"],
        "created_by": {"kind": "agent", "id": "fixture-builder"},
    }
    if "market_metadata_ref" in refs:
        document["market_metadata_ref"] = refs["market_metadata_ref"]

    return SourceFixture(document=document, data_ref=data_ref, data_path=data_path, paths=paths)


def make_study_document(
    *,
    study_id: str = "synthetic_study",
    source_refs: tuple[str, ...] = ("synthetic_l2@1",),
    status: str = "active",
    instrument_id: str = "T2406",
) -> dict[str, Any]:
    """Build a valid minimal Study using an empty finite feature backlog."""
    return {
        "schema_version": "study_v1",
        "study_id": study_id,
        "name": "Synthetic order-book study",
        "objective": "Exercise revisioned research management with synthetic artifacts.",
        "status": status,
        "source_refs": list(source_refs),
        "feature_set_refs": [],
        "backlog": [],
        "limits": {
            "allowed_steps": [3, 4, 5],
            "allowed_scope": [{
                "instruments": [instrument_id],
                "start": "2024-06-03T00:00:00+08:00",
                "end": "2024-06-05T00:00:00+08:00",
                "session_filter": ["day", "night"],
            }],
            "budget": {"max_partitions": 20, "max_cpu_seconds": 60},
        },
        "created_by": {"kind": "human", "id": "fixture-user"},
        "change_reason": "Create serialized test fixture.",
    }
