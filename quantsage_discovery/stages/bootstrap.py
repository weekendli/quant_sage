"""Raw source snapshot and unassigned research-only split bootstrap stage."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from quantsage_discovery.core.artifacts import ArtifactStore
from quantsage_discovery.core.errors import ArtifactValidationError
from quantsage_discovery.core.types import ArtifactRef, RunContext, StageResult
from quantsage_discovery.core.utils import canonical_json
from quantsage_discovery.ingest.calendar import sha256_file
from quantsage_discovery.ingest.readers import verify_source_inventory


def _json_validator(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ArtifactValidationError(f"invalid JSON artifact {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ArtifactValidationError("bootstrap JSON artifact must be an object")
    return value


def validate_source_snapshot_artifact(path: Path) -> None:
    value = _json_validator(path)
    try:
        verify_source_inventory(value, verify_hashes=True)
    except (KeyError, OSError, ValueError) as exc:
        raise ArtifactValidationError(f"frozen source snapshot validation failed: {exc}") from exc


def validate_split_manifest_artifact(path: Path) -> None:
    value = _json_validator(path)
    split_id = value.get("split_id")
    content = {key: item for key, item in value.items() if key != "split_id"}
    expected = "split_" + hashlib.sha256(canonical_json(content).encode("utf-8")).hexdigest()
    if split_id != expected:
        raise ArtifactValidationError("SplitManifest ID does not match serialized manifest content")
    if value.get("method") != "unassigned_research_only":
        raise ArtifactValidationError("Step02 requires an unassigned research-only SplitManifest")
    if value.get("fitting_allowed") is not False or value.get("hypothesis_stages_allowed") is not False:
        raise ArtifactValidationError("unassigned SplitManifest must disable fitting and hypotheses")
    if value.get("train") is not None or value.get("development") is not None or value.get("final_holdout") is not None:
        raise ArtifactValidationError("unassigned SplitManifest cannot define synthetic date splits")


class BootstrapStage:
    name = "bootstrap"
    version = "1.0.0"
    dependencies: tuple[str, ...] = ()
    partitioned = False
    config_keys = (
        "data_snapshot_id",
        "splits.split_id",
        "input.adapter",
        "input.paths",
        "input.source_schema_path",
        "bootstrap.source_inventory_path",
        "bootstrap.split_manifest_path",
    )

    def register_validators(self, store: ArtifactStore) -> None:
        store.register_validator("source_snapshot", self.version, validate_source_snapshot_artifact)
        store.register_validator("split_manifest", self.version, validate_split_manifest_artifact)

    def validate(self, ctx: RunContext, inputs: dict[str, ArtifactRef]) -> None:
        if inputs:
            raise ValueError("bootstrap does not accept upstream artifact refs")
        bootstrap = ctx.config.get("bootstrap", {})
        inventory_path = Path(bootstrap.get("source_inventory_path", "")).expanduser()
        split_path = Path(bootstrap.get("split_manifest_path", "")).expanduser()
        if not inventory_path.is_absolute() or not split_path.is_absolute():
            raise ValueError("run prepare_onboarding_config before starting bootstrap")
        inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
        split_manifest = json.loads(split_path.read_text(encoding="utf-8"))
        if sha256_file(inventory_path) != bootstrap.get("source_inventory_sha256"):
            raise ValueError("preflight source inventory hash differs from resolved config")
        if sha256_file(split_path) != bootstrap.get("split_manifest_sha256"):
            raise ValueError("preflight SplitManifest hash differs from resolved config")
        verify_source_inventory(inventory, verify_hashes=False)
        validate_split_manifest_artifact(split_path)
        if inventory.get("data_snapshot_id") != ctx.data_snapshot_id:
            raise ValueError("resolved config data_snapshot_id differs from frozen source inventory")
        if split_manifest.get("split_id") != ctx.split_id:
            raise ValueError("resolved config split_id differs from frozen SplitManifest")
        if split_manifest.get("source_data_snapshot_id") != ctx.data_snapshot_id:
            raise ValueError("SplitManifest refers to a different source snapshot")
        if "onboard" not in split_manifest.get("allowed_stages", []):
            raise ValueError("unassigned SplitManifest does not permit the onboarding stage")

    def execute(self, ctx: RunContext, inputs: dict[str, ArtifactRef]) -> StageResult:
        del inputs
        bootstrap = ctx.config["bootstrap"]
        inventory = json.loads(
            Path(bootstrap["source_inventory_path"]).read_text(encoding="utf-8")
        )
        split_manifest = json.loads(
            Path(bootstrap["split_manifest_path"]).read_text(encoding="utf-8")
        )
        refs = {
            "source_snapshot": ctx.artifact_transaction.write_json(
                "source_snapshot",
                inventory,
                artifact_type="source_snapshot",
                schema_version=self.version,
                row_count=inventory["input_row_count"],
            ),
            "split_manifest": ctx.artifact_transaction.write_json(
                "split_manifest",
                split_manifest,
                artifact_type="split_manifest",
                schema_version=self.version,
            ),
        }
        return StageResult(
            artifacts=refs,
            metrics={
                "input_files": len(inventory["raw_files"]),
                "input_rows": inventory["input_row_count"],
                "source_dates": len(inventory["source_dates"]),
            },
        )
