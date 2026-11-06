"""Immutable artifact staging, schema validation, hashing, and verification."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from .errors import ArtifactError, ArtifactValidationError
from .types import ArtifactManifest, ArtifactRef, RunContext, StageResult
from .utils import canonical_json

Validator = Callable[[Path], None]


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def hash_path(path: Path) -> str:
    """Hash a file or directory without loading large artifacts into memory."""
    digest = hashlib.sha256()
    if path.is_file():
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    elif path.is_dir():
        for child in sorted(p for p in path.rglob("*") if p.is_file()):
            if child.is_symlink():
                raise ArtifactError(f"artifact directory contains symlink: {child}")
            relative = child.relative_to(path).as_posix().encode("utf-8")
            digest.update(len(relative).to_bytes(8, "big"))
            digest.update(relative)
            file_hash = hashlib.sha256()
            with child.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    file_hash.update(chunk)
            digest.update(file_hash.digest())
    else:
        raise ArtifactError(f"artifact payload does not exist: {path}")
    return "sha256:" + digest.hexdigest()


def artifact_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ArtifactError(f"only file:// artifact URIs are supported by LocalExecutor: {uri}")
    path = Path(unquote(parsed.path))
    if not path.is_absolute():
        raise ArtifactError(f"artifact URI must resolve to an absolute file path: {uri}")
    return path


def _validate_time_bounds(time_min: str | None, time_max: str | None) -> None:
    if (time_min is None) != (time_max is None):
        raise ArtifactValidationError("time_min and time_max must both be set or both be null")
    if time_min is not None and time_max is not None and time_min > time_max:
        raise ArtifactValidationError("time_min must not be greater than time_max")


def _validate_json_value(value: Any, schema: Mapping[str, Any], path: str) -> None:
    expected = schema.get("type")
    type_checks = {
        "object": lambda item: isinstance(item, dict),
        "array": lambda item: isinstance(item, list),
        "string": lambda item: isinstance(item, str),
        "integer": lambda item: isinstance(item, int) and not isinstance(item, bool),
        "number": lambda item: isinstance(item, (int, float)) and not isinstance(item, bool),
        "boolean": lambda item: isinstance(item, bool),
        "null": lambda item: item is None,
    }
    if expected in type_checks and not type_checks[expected](value):
        raise ArtifactValidationError(f"{path} must be JSON {expected}")
    if "enum" in schema and value not in schema["enum"]:
        raise ArtifactValidationError(f"{path} must be one of {schema['enum']!r}")
    if expected == "object" and isinstance(value, dict):
        for name in schema.get("required", []):
            if name not in value:
                raise ArtifactValidationError(f"{path}.{name} is required")
        properties = schema.get("properties", {})
        for name, child in value.items():
            if name in properties:
                _validate_json_value(child, properties[name], f"{path}.{name}")
            elif schema.get("additionalProperties") is False:
                raise ArtifactValidationError(f"{path}.{name} is not an allowed property")
    if expected == "array" and isinstance(value, list) and isinstance(schema.get("items"), Mapping):
        for index, child in enumerate(value):
            _validate_json_value(child, schema["items"], f"{path}[{index}]")
    if "minimum" in schema and isinstance(value, (int, float)) and value < schema["minimum"]:
        raise ArtifactValidationError(f"{path} must be >= {schema['minimum']}")
    if "minLength" in schema and isinstance(value, str) and len(value) < schema["minLength"]:
        raise ArtifactValidationError(f"{path} must have length >= {schema['minLength']}")


@dataclass
class _Pending:
    alias: str
    payload_path: Path
    artifact_type: str
    schema_version: str
    metadata: dict[str, Any]
    ref: ArtifactRef | None = None


class ArtifactStore:
    """Artifact store with mandatory typed validators and stage-bundle atomicity."""

    def __init__(self, root: Path | str):
        self.root = Path(root).expanduser().resolve()
        self._validators: dict[tuple[str, str], Validator] = {}

    def register_validator(self, artifact_type: str, schema_version: str, validator: Validator) -> None:
        if not artifact_type or not schema_version or not callable(validator):
            raise ValueError("validator registration requires artifact type, schema version, and callable")
        key = (artifact_type, schema_version)
        existing = self._validators.get(key)
        if existing is not None and existing is not validator:
            raise ValueError(f"a validator is already registered for {artifact_type}@{schema_version}")
        self._validators[key] = validator

    def register_json_schema(self, artifact_type: str, schema_version: str,
                             schema: Mapping[str, Any]) -> None:
        """Register the common structural JSON Schema subset for a typed artifact."""
        schema_copy = json.loads(canonical_json(schema))

        def validate(path: Path) -> None:
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ArtifactValidationError(f"invalid JSON payload: {exc}") from exc
            _validate_json_value(value, schema_copy, "$" )

        self.register_validator(artifact_type, schema_version, validate)

    def validate_payload(self, path: Path, artifact_type: str, schema_version: str) -> None:
        validator = self._validators.get((artifact_type, schema_version))
        if validator is None:
            raise ArtifactValidationError(
                f"no schema validator registered for {artifact_type}@{schema_version}"
            )
        try:
            validator(path)
        except ArtifactValidationError:
            raise
        except Exception as exc:
            raise ArtifactValidationError(
                f"schema validation failed for {artifact_type}@{schema_version}: {exc}"
            ) from exc

    def begin(self, context: RunContext, stage_name: str, stage_version: str, cache_key: str,
              inputs: Mapping[str, ArtifactRef]) -> ArtifactTransaction:
        return ArtifactTransaction(self, context, stage_name, stage_version, cache_key, inputs)

    def verify(self, ref: ArtifactRef, expected: Mapping[str, Any] | None = None) -> ArtifactManifest:
        payload = artifact_path(ref.uri)
        if not ref.manifest_uri:
            raise ArtifactValidationError(f"artifact ref has no manifest URI: {ref.artifact_id}")
        manifest_file = artifact_path(ref.manifest_uri)
        if not payload.exists() or not manifest_file.is_file():
            raise ArtifactError(f"artifact payload or manifest missing: {ref.artifact_id}")
        try:
            raw = json.loads(manifest_file.read_text(encoding="utf-8"))
            manifest = ArtifactManifest.from_dict(raw)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            raise ArtifactValidationError(f"invalid artifact manifest {manifest_file}: {exc}") from exc
        if (manifest.uri != ref.uri or manifest.manifest_uri != ref.manifest_uri
                or manifest.artifact_id != ref.artifact_id):
            raise ArtifactValidationError(f"artifact ref does not match manifest for {ref.artifact_id}")
        if manifest.content_hash != ref.content_hash or hash_path(payload) != manifest.content_hash:
            raise ArtifactValidationError(f"artifact content hash mismatch for {ref.artifact_id}")
        if manifest.artifact_id != "art_" + manifest.content_hash.removeprefix("sha256:"):
            raise ArtifactValidationError(f"artifact id is not content-addressed for {ref.artifact_id}")
        if manifest.schema_version != ref.schema_version or manifest.artifact_type != ref.artifact_type:
            raise ArtifactValidationError(f"artifact schema ref mismatch for {ref.artifact_id}")
        if dict(manifest.partition_keys) != dict(ref.partition_keys):
            raise ArtifactValidationError(f"artifact partition keys mismatch for {ref.artifact_id}")
        if manifest.row_count != ref.row_count:
            raise ArtifactValidationError(f"artifact row_count mismatch for {ref.artifact_id}")
        if manifest.time_min != ref.time_min or manifest.time_max != ref.time_max:
            raise ArtifactValidationError(f"artifact time bounds mismatch for {ref.artifact_id}")
        for key in ("stage_name", "stage_version", "cache_key"):
            ref_value = getattr(ref, key)
            if ref_value is not None and getattr(manifest, key) != ref_value:
                raise ArtifactValidationError(f"artifact {key} provenance mismatch for {ref.artifact_id}")
        for key in ("run_id", "stage_run_id"):
            ref_value = getattr(ref, key)
            if ref_value is not None and getattr(manifest, key) != ref_value:
                raise ArtifactValidationError(f"artifact {key} provenance mismatch for {ref.artifact_id}")
        if expected:
            for key, value in expected.items():
                if manifest.to_dict().get(key) != value:
                    raise ArtifactValidationError(
                        f"artifact provenance mismatch for {ref.artifact_id}: {key} "
                        f"expected {value!r}, found {manifest.to_dict().get(key)!r}"
                    )
        self.validate_payload(payload, manifest.artifact_type, manifest.schema_version)
        return manifest


class ArtifactTransaction:
    """Private per-stage bundle; it becomes visible only after full output validation."""

    def __init__(self, store: ArtifactStore, context: RunContext, stage_name: str,
                 stage_version: str, cache_key: str, inputs: Mapping[str, ArtifactRef]):
        self.store = store
        self.context = context
        self.stage_name = stage_name
        self.stage_version = stage_version
        self.cache_key = cache_key
        self.inputs = dict(inputs)
        self.stage_run_id = context.stage_run_id or f"sr_{uuid.uuid4().hex}"
        self.temp_root = store.root / ".staging" / f"{self.stage_run_id}-{uuid.uuid4().hex}"
        self.final_root = store.root / "runs" / context.run_id / "stages" / self.stage_run_id
        self.temp_root.mkdir(parents=True, exist_ok=False)
        self._pending: dict[str, _Pending] = {}
        self._committed = False

    def reserve_output(self, alias: str, *, artifact_type: str, schema_version: str,
                       relative_path: str = "payload",
                       partition_keys: Mapping[str, Any] | None = None,
                       row_count: int | None = None, time_min: str | None = None,
                       time_max: str | None = None,
                       quality_flags: tuple[str, ...] | list[str] = ()) -> Path:
        """Reserve a path within the private bundle for a streaming file writer."""
        if not alias or "/" in alias or "\\" in alias or alias in {".", ".."}:
            raise ArtifactError(f"artifact alias must be a simple non-empty name: {alias!r}")
        if alias in self._pending:
            raise ArtifactError(f"duplicate output artifact alias: {alias}")
        if not artifact_type or not schema_version:
            raise ArtifactValidationError("artifact_type and schema_version are required")
        if row_count is not None and (
            isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0
        ):
            raise ArtifactValidationError("row_count must be a non-negative integer or null")
        if any(not isinstance(flag, str) for flag in quality_flags):
            raise ArtifactValidationError("quality_flags must contain strings")
        _validate_time_bounds(time_min, time_max)
        relative = Path(relative_path)
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            raise ArtifactError("output relative_path must stay inside its artifact bundle")
        alias_dir = self.temp_root / "artifacts" / alias
        payload = alias_dir.joinpath(*relative.parts)
        payload.parent.mkdir(parents=True, exist_ok=True)
        self._pending[alias] = _Pending(
            alias=alias,
            payload_path=payload,
            artifact_type=artifact_type,
            schema_version=schema_version,
            metadata={
                "partition_keys": {str(k): str(v) for k, v in
                                   (partition_keys or self.context.partition_keys).items()},
                "row_count": row_count,
                "time_min": time_min,
                "time_max": time_max,
                "quality_flags": tuple(quality_flags),
                "relative_path": relative.as_posix(),
            },
        )
        return payload

    def register_output(self, alias: str) -> ArtifactRef:
        item = self._pending.get(alias)
        if item is None:
            raise ArtifactError(f"output path was not reserved: {alias}")
        if item.ref is not None:
            return item.ref
        if not item.payload_path.exists():
            raise ArtifactError(f"reserved output was not written: {alias}")
        hash_value = hash_path(item.payload_path)
        item.ref = ArtifactRef(
            artifact_id="art_" + hash_value.removeprefix("sha256:"),
            artifact_type=item.artifact_type,
            schema_version=item.schema_version,
            uri="pending://" + self.stage_run_id + "/" + alias,
            content_hash=hash_value,
            partition_keys=item.metadata["partition_keys"],
            row_count=item.metadata["row_count"],
            time_min=item.metadata["time_min"],
            time_max=item.metadata["time_max"],
            run_id=self.context.run_id,
            stage_run_id=self.stage_run_id,
            stage_name=self.stage_name,
            stage_version=self.stage_version,
            cache_key=self.cache_key,
        )
        return item.ref

    def final_ref(self, ref: ArtifactRef) -> ArtifactRef:
        """Compute a stable output URI before commit, for references embedded in sibling outputs."""
        if not ref.uri.startswith("pending://"):
            return ref
        alias = ref.uri.rsplit("/", 1)[-1]
        item = self._pending.get(alias)
        if item is None or item.ref != ref:
            raise ArtifactError(f"unknown pending output ref: {ref.uri}")
        alias_dir = self.final_root / "artifacts" / alias
        return replace(
            ref,
            uri=(alias_dir / item.metadata["relative_path"]).as_uri(),
            manifest_uri=(alias_dir / "manifest.json").as_uri(),
        )

    def write_bytes(self, alias: str, data: bytes, *, artifact_type: str,
                    schema_version: str, partition_keys: Mapping[str, Any] | None = None,
                    row_count: int | None = None, time_min: str | None = None,
                    time_max: str | None = None, quality_flags: tuple[str, ...] | list[str] = ()) -> ArtifactRef:
        if not isinstance(data, bytes):
            raise TypeError("write_bytes expects bytes")
        path = self.reserve_output(alias, artifact_type=artifact_type, schema_version=schema_version,
                                   partition_keys=partition_keys, row_count=row_count,
                                   time_min=time_min, time_max=time_max, quality_flags=quality_flags)
        path.write_bytes(data)
        return self.register_output(alias)

    def write_json(self, alias: str, value: Any, *, artifact_type: str,
                   schema_version: str, partition_keys: Mapping[str, Any] | None = None,
                   row_count: int | None = None, time_min: str | None = None,
                   time_max: str | None = None, quality_flags: tuple[str, ...] | list[str] = ()) -> ArtifactRef:
        data = (canonical_json(value) + "\n").encode("utf-8")
        return self.write_bytes(alias, data, artifact_type=artifact_type,
                                schema_version=schema_version, partition_keys=partition_keys,
                                row_count=row_count, time_min=time_min, time_max=time_max,
                                quality_flags=quality_flags)

    def write_file(self, alias: str, source: Path | str, *, artifact_type: str,
                   schema_version: str, partition_keys: Mapping[str, Any] | None = None,
                   row_count: int | None = None, time_min: str | None = None,
                   time_max: str | None = None, quality_flags: tuple[str, ...] | list[str] = ()) -> ArtifactRef:
        source_path = Path(source).expanduser().resolve()
        if not source_path.exists():
            raise ArtifactError(f"artifact source does not exist: {source_path}")
        path = self.reserve_output(alias, artifact_type=artifact_type, schema_version=schema_version,
                                   partition_keys=partition_keys, row_count=row_count,
                                   time_min=time_min, time_max=time_max, quality_flags=quality_flags)
        if source_path.is_dir():
            if path.exists():
                path.rmdir()
            shutil.copytree(source_path, path)
        elif source_path.is_file():
            shutil.copy2(source_path, path)
        else:
            raise ArtifactError(f"unsupported artifact source type: {source_path}")
        return self.register_output(alias)

    def commit(self, result: StageResult, code_revision: str) -> dict[str, ArtifactRef]:
        if self._committed:
            raise ArtifactError("artifact transaction already committed")
        if not isinstance(result, StageResult):
            raise ArtifactValidationError("stage execute() must return StageResult")
        returned_pending: set[str] = set()
        finalized: dict[str, ArtifactRef] = {}
        for alias, ref in result.artifacts.items():
            if not isinstance(alias, str) or not alias or not isinstance(ref, ArtifactRef):
                raise ArtifactValidationError("StageResult artifacts must map aliases to ArtifactRef values")
            if ref.uri.startswith("pending://"):
                pending_alias = ref.uri.rsplit("/", 1)[-1]
                item = self._pending.get(pending_alias)
                if item is None or item.ref != ref or pending_alias != alias:
                    raise ArtifactValidationError(f"StageResult references unknown output {alias!r}")
                returned_pending.add(alias)
                if item.ref is None:
                    raise ArtifactValidationError(f"StageResult output {alias!r} was not registered")
                before_validation = hash_path(item.payload_path)
                if before_validation != ref.content_hash:
                    raise ArtifactValidationError(f"output changed after registration: {alias}")
                self.store.validate_payload(item.payload_path, ref.artifact_type, ref.schema_version)
                after_validation = hash_path(item.payload_path)
                if after_validation != before_validation:
                    raise ArtifactValidationError(f"schema validator modified output: {alias}")
                final_ref = self.final_ref(ref)
                manifest = ArtifactManifest(
                    artifact_id=ref.artifact_id,
                    artifact_type=ref.artifact_type,
                    schema_version=ref.schema_version,
                    uri=final_ref.uri,
                    content_hash=ref.content_hash,
                    manifest_uri=final_ref.manifest_uri or "",
                    run_id=self.context.run_id,
                    stage_run_id=self.stage_run_id,
                    stage_name=self.stage_name,
                    stage_version=self.stage_version,
                    cache_key=self.cache_key,
                    config_hash=self.context.config_hash,
                    code_revision=code_revision,
                    input_artifact_ids=tuple(sorted({ref.artifact_id for ref in self.inputs.values()})),
                    data_snapshot_id=self.context.data_snapshot_id,
                    split_id=self.context.split_id,
                    partition_keys=item.metadata["partition_keys"],
                    time_min=item.metadata["time_min"],
                    time_max=item.metadata["time_max"],
                    row_count=item.metadata["row_count"],
                    quality_flags=item.metadata["quality_flags"],
                    created_at=now_iso(),
                )
                alias_dir = item.payload_path.parents[len(Path(item.metadata["relative_path"]).parts) - 1]
                (alias_dir / "manifest.json").write_text(
                    canonical_json(manifest.to_dict()) + "\n", encoding="utf-8"
                )
                finalized[alias] = final_ref
            else:
                raise ArtifactValidationError(
                    f"stage output {alias!r} must be created through artifact_transaction"
                )
        if returned_pending != set(self._pending):
            missing = sorted(set(self._pending) - returned_pending)
            raise ArtifactValidationError("outputs not returned in StageResult: " + ", ".join(missing))
        if self.final_root.exists():
            raise ArtifactError(f"refusing to overwrite prior stage output: {self.final_root}")
        self.final_root.parent.mkdir(parents=True, exist_ok=True)
        os.replace(self.temp_root, self.final_root)
        try:
            for ref in finalized.values():
                if ref.uri.startswith("file://"):
                    self.store.verify(ref)
        except Exception:
            shutil.rmtree(self.final_root, ignore_errors=True)
            raise
        self._committed = True
        return finalized

    def abort(self) -> None:
        if not self._committed:
            shutil.rmtree(self.temp_root, ignore_errors=True)
