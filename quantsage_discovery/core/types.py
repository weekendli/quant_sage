"""Stable public types used by workflow stages and the local executor."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable


class StageStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CACHED = "CACHED"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class ArtifactRef:
    artifact_id: str
    artifact_type: str
    schema_version: str
    uri: str
    content_hash: str
    manifest_uri: str | None = None
    partition_keys: Mapping[str, str] = field(default_factory=dict)
    row_count: int | None = None
    time_min: str | None = None
    time_max: str | None = None
    run_id: str | None = None
    stage_run_id: str | None = None
    stage_name: str | None = None
    stage_version: str | None = None
    cache_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "artifact_type": self.artifact_type,
            "schema_version": self.schema_version,
            "uri": self.uri,
            "content_hash": self.content_hash,
            "manifest_uri": self.manifest_uri,
            "partition_keys": dict(self.partition_keys),
            "row_count": self.row_count,
            "time_min": self.time_min,
            "time_max": self.time_max,
            "run_id": self.run_id,
            "stage_run_id": self.stage_run_id,
            "stage_name": self.stage_name,
            "stage_version": self.stage_version,
            "cache_key": self.cache_key,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ArtifactRef:
        required = ("artifact_id", "artifact_type", "schema_version", "uri", "content_hash")
        missing = [key for key in required if not isinstance(value.get(key), str) or not value[key]]
        if missing:
            raise ValueError(f"ArtifactRef missing required string fields: {', '.join(missing)}")
        partition_keys = value.get("partition_keys", {})
        if not isinstance(partition_keys, Mapping) or any(
            not isinstance(k, str) or not isinstance(v, (str, int, float, bool))
            for k, v in partition_keys.items()
        ):
            raise ValueError("ArtifactRef.partition_keys must be a scalar mapping")
        row_count = value.get("row_count")
        if row_count is not None and (
            isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0
        ):
            raise ValueError("ArtifactRef.row_count must be a non-negative integer or null")
        return cls(
            artifact_id=str(value["artifact_id"]),
            artifact_type=str(value["artifact_type"]),
            schema_version=str(value["schema_version"]),
            uri=str(value["uri"]),
            content_hash=str(value["content_hash"]),
            manifest_uri=value.get("manifest_uri"),
            partition_keys={str(k): str(v) for k, v in partition_keys.items()},
            row_count=row_count,
            time_min=value.get("time_min"),
            time_max=value.get("time_max"),
            run_id=value.get("run_id"),
            stage_run_id=value.get("stage_run_id"),
            stage_name=value.get("stage_name"),
            stage_version=value.get("stage_version"),
            cache_key=value.get("cache_key"),
        )


@dataclass(frozen=True)
class ArtifactManifest:
    artifact_id: str
    artifact_type: str
    schema_version: str
    uri: str
    content_hash: str
    manifest_uri: str
    run_id: str
    stage_run_id: str
    stage_name: str
    stage_version: str
    cache_key: str
    config_hash: str
    code_revision: str
    input_artifact_ids: tuple[str, ...]
    data_snapshot_id: str | None
    split_id: str | None
    partition_keys: Mapping[str, str] = field(default_factory=dict)
    time_min: str | None = None
    time_max: str | None = None
    row_count: int | None = None
    quality_flags: tuple[str, ...] = ()
    created_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "artifact_type": self.artifact_type,
            "schema_version": self.schema_version,
            "uri": self.uri,
            "content_hash": self.content_hash,
            "manifest_uri": self.manifest_uri,
            "run_id": self.run_id,
            "stage_run_id": self.stage_run_id,
            "stage_name": self.stage_name,
            "stage_version": self.stage_version,
            "cache_key": self.cache_key,
            "config_hash": self.config_hash,
            "code_revision": self.code_revision,
            "input_artifact_ids": list(self.input_artifact_ids),
            "data_snapshot_id": self.data_snapshot_id,
            "split_id": self.split_id,
            "partition_keys": dict(self.partition_keys),
            "time_min": self.time_min,
            "time_max": self.time_max,
            "row_count": self.row_count,
            "quality_flags": list(self.quality_flags),
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ArtifactManifest:
        required_strings = (
            "artifact_id",
            "artifact_type",
            "schema_version",
            "uri",
            "content_hash",
            "manifest_uri",
            "run_id",
            "stage_run_id",
            "stage_name",
            "stage_version",
            "cache_key",
            "config_hash",
            "code_revision",
        )
        missing = [key for key in required_strings if not isinstance(value.get(key), str) or not value[key]]
        if missing:
            raise ValueError(f"ArtifactManifest missing required string fields: {', '.join(missing)}")
        input_ids = value.get("input_artifact_ids")
        quality_flags = value.get("quality_flags", [])
        partition_keys = value.get("partition_keys", {})
        if not isinstance(input_ids, list) or any(not isinstance(item, str) for item in input_ids):
            raise ValueError("ArtifactManifest.input_artifact_ids must be a list of strings")
        if not isinstance(quality_flags, list) or any(not isinstance(item, str) for item in quality_flags):
            raise ValueError("ArtifactManifest.quality_flags must be a list of strings")
        if not isinstance(partition_keys, Mapping) or any(
            not isinstance(k, str) or not isinstance(v, (str, int, float, bool))
            for k, v in partition_keys.items()
        ):
            raise ValueError("ArtifactManifest.partition_keys must be a scalar mapping")
        row_count = value.get("row_count")
        if row_count is not None and (
            isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0
        ):
            raise ValueError("ArtifactManifest.row_count must be a non-negative integer or null")
        return cls(
            artifact_id=str(value["artifact_id"]),
            artifact_type=str(value["artifact_type"]),
            schema_version=str(value["schema_version"]),
            uri=str(value["uri"]),
            content_hash=str(value["content_hash"]),
            manifest_uri=str(value["manifest_uri"]),
            run_id=str(value["run_id"]),
            stage_run_id=str(value["stage_run_id"]),
            stage_name=str(value["stage_name"]),
            stage_version=str(value["stage_version"]),
            cache_key=str(value["cache_key"]),
            config_hash=str(value["config_hash"]),
            code_revision=str(value["code_revision"]),
            input_artifact_ids=tuple(input_ids),
            data_snapshot_id=value.get("data_snapshot_id"),
            split_id=value.get("split_id"),
            partition_keys={str(k): str(v) for k, v in partition_keys.items()},
            time_min=value.get("time_min"),
            time_max=value.get("time_max"),
            row_count=row_count,
            quality_flags=tuple(quality_flags),
            created_at=str(value.get("created_at", "")),
        )

    def to_ref(self) -> ArtifactRef:
        return ArtifactRef(
            artifact_id=self.artifact_id,
            artifact_type=self.artifact_type,
            schema_version=self.schema_version,
            uri=self.uri,
            content_hash=self.content_hash,
            manifest_uri=self.manifest_uri,
            partition_keys=self.partition_keys,
            row_count=self.row_count,
            time_min=self.time_min,
            time_max=self.time_max,
            run_id=self.run_id,
            stage_run_id=self.stage_run_id,
            stage_name=self.stage_name,
            stage_version=self.stage_version,
            cache_key=self.cache_key,
        )


@dataclass(frozen=True)
class StageResult:
    artifacts: Mapping[str, ArtifactRef] = field(default_factory=dict)
    metrics: Mapping[str, float | int | str] = field(default_factory=dict)
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class ResourceBudget:
    memory_limit: str = "8GB"
    threads: int = 4
    max_concurrent_stages: int = 1
    temporary_uri: str | None = None
    max_feature_candidates: int = 40
    max_cube_cells_per_hypothesis: int = 24
    max_retries: int = 2
    retry_delay_seconds: float = 0.0

    def __post_init__(self) -> None:
        for name in (
            "threads",
            "max_concurrent_stages",
            "max_feature_candidates",
            "max_cube_cells_per_hypothesis",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"resource budget {name} must be >= 1")
        if isinstance(self.max_retries, bool) or not isinstance(self.max_retries, int) or self.max_retries < 0:
            raise ValueError("retry limits and delay must be non-negative")
        if (isinstance(self.retry_delay_seconds, bool)
                or not isinstance(self.retry_delay_seconds, (int, float))
                or not math.isfinite(float(self.retry_delay_seconds))
                or self.retry_delay_seconds < 0):
            raise ValueError("retry limits and delay must be non-negative")
        if not isinstance(self.memory_limit, str) or not re.fullmatch(
            r"\s*\d+(?:\.\d+)?\s*(?:B|KB|MB|GB|TB)\s*", self.memory_limit, re.IGNORECASE
        ):
            raise ValueError("memory_limit must be a size such as '8GB'")


@dataclass(frozen=True)
class RunContext:
    run_id: str
    config_hash: str
    split_id: str | None
    data_snapshot_id: str | None
    as_of_cutoff: str | None
    artifact_store: Any
    artifact_transaction: Any
    rng_seed: int
    logger: Any
    resource_budget: ResourceBudget
    config: Mapping[str, Any]
    stage_config: Mapping[str, Any] = field(default_factory=dict)
    stage_run_id: str | None = None
    stage_name: str | None = None
    partition_keys: Mapping[str, str] = field(default_factory=dict)
    code_revision: str = ""


@runtime_checkable
class Stage(Protocol):
    name: str
    version: str
    dependencies: tuple[str, ...]

    def validate(self, ctx: RunContext, inputs: dict[str, ArtifactRef]) -> None: ...

    def execute(self, ctx: RunContext, inputs: dict[str, ArtifactRef]) -> StageResult: ...


def stage_is_partitioned(stage: Any) -> bool:
    """Partitioning is opt-in for reducers; existing plugins default to per-partition."""
    return bool(getattr(stage, "partitioned", True))


def stage_config_keys(stage: Any) -> tuple[str, ...] | None:
    keys = getattr(stage, "config_keys", None)
    if keys is None:
        return None
    if not isinstance(keys, (tuple, list)) or any(not isinstance(key, str) for key in keys):
        raise TypeError(f"stage {stage.name!r} config_keys must be a tuple/list of dotted paths")
    return tuple(keys)
