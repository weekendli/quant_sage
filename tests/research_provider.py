"""Deterministic arithmetic catalog for research-engine tests; no market inference."""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import ClassVar

from quantsage_discovery.core.artifacts import artifact_path
from quantsage_discovery.core.errors import ConfigError
from quantsage_discovery.core.research import WorkDefinition, digest, instant
from quantsage_discovery.core.types import StageResult


def validate_toy(path):
    result = json.loads(path.read_text())
    if set(result) != {"values", "definition", "scope", "verdict"}:
        raise ValueError("invalid toy payload")
    if not isinstance(result["values"], list):
        raise TypeError("values must be a list")


class ArithmeticCatalog:
    fail_start = None
    empty_start = None
    metric_sets: ClassVar = {"toy_pair": ["toy.mean", "toy.max"]}

    def __init__(self):
        self.definitions = {"base": WorkDefinition("base", "primitive", "primitives", "1", {})}

    def get(self, key):
        if key not in self.definitions:
            raise ConfigError(f"unknown definition {key}")
        return self.definitions[key]

    def resolve(self, kind, parameters):
        if kind == "business":
            if parameters["family"] != "scale" or "size" in parameters or "space" in parameters:
                raise ConfigError("scale has no space/size dimension")
            deps = ("base",)
            stage = "features"
        elif kind == "statistical":
            if parameters["metric"] not in ("toy.mean", "toy.max"):
                raise ConfigError("unknown toy metric")
            deps = (parameters["source"],)
            stage = "statistical_atlas"
        elif kind == "validation":
            if parameters["mode"] != "quality" or parameters["protocol_ref"] != "toy_quality_v1":
                raise ConfigError("toy provider supports quality only; no hypothesis inference")
            deps = tuple(parameters["targets"])
            stage = "quality"
        else:
            raise ConfigError("unsupported kind")
        key = digest({"kind": kind, "parameters": parameters})
        definition = WorkDefinition(
            key, kind, stage, "1", parameters, deps, partitioned=kind != "validation"
        )
        self.definitions[key] = definition
        return definition

    def authorize(self, scope, validation, sources):
        values = {
            k: json.loads(artifact_path(v.uri).read_text())["value"] for k, v in sources.items()
        }
        if values["snapshot_ref"] != "toy_snapshot":
            raise ConfigError("unrecognized toy snapshot")
        split = values["split_ref"]
        if split["role"] != "exploration" or validation["mode"] != "quality":
            raise ConfigError("toy scope only authorizes exploration quality checks")
        if instant(scope["output_interval"]["start"]) < instant(split["start"]) or instant(
            scope["output_interval"]["end"]
        ) > instant(split["end"]):
            raise ConfigError("requested scope is outside authorized split")
        if scope["instruments"] != ["TOY"]:
            raise ConfigError("unsupported toy instrument")
        return {
            "authorized": True,
            "role": split["role"],
            "read_interval": scope["output_interval"],
        }

    def register_validators(self, store):
        from tests.synthetic_stages import register_record_validator

        register_record_validator(store)

        store.register_validator("toy_batch", "1", validate_toy)

    def execute(self, definition, ctx, inputs):
        if definition.kind == "business" and ctx.partition_keys.get("start") == self.fail_start:
            raise ValueError("injected arithmetic worker failure")
        dependencies = [
            json.loads(artifact_path(ref.uri).read_text())
            for key, ref in inputs.items()
            if key.startswith("item_") and "." in key
        ]
        if definition.kind == "primitive":
            values = [1, 2, 3]
        elif definition.kind == "business":
            factor = int(definition.parameters["lookback"][:-1])
            values = [v * factor for v in dependencies[0]["values"]]
            if ctx.partition_keys.get("start") == self.empty_start:
                values = []
        elif definition.kind == "statistical":
            series = dependencies[0]["values"]
            if not series:
                values = []
            elif definition.parameters["metric"] == "toy.mean":
                values = [sum(series) / len(series)]
            else:
                values = [max(series)]
        else:
            values = [v for item in dependencies for v in item["values"]]
        verdict = "PASS" if definition.kind == "validation" else None
        ref = ctx.artifact_transaction.write_json(
            "result",
            {
                "values": values,
                "definition": asdict(definition),
                "scope": dict(ctx.partition_keys),
                "verdict": verdict,
            },
            artifact_type="toy_batch",
            schema_version="1",
            row_count=len(values),
        )
        return StageResult(
            artifacts={"result": ref},
            metrics={
                "n_total": len(values),
                "n_valid": len(values),
                "processed_scope": "complete",
                "verdict": verdict or "NOT_APPLICABLE",
            },
        )


def make_environment(tmp_path):
    from quantsage_discovery.core.artifacts import ArtifactStore
    from quantsage_discovery.core.registry import Registry
    from quantsage_discovery.core.workflow import WorkflowEngine
    from tests.synthetic_stages import JsonStage

    registry = Registry(tmp_path / "registry.sqlite")
    store = ArtifactStore(tmp_path / "artifacts")
    config = {
        "project": "toy",
        "seed": 1,
        "storage": {"artifacts_uri": str(store.root), "registry_uri": str(registry.path)},
        "resources": {"max_retries": 0},
    }
    stages = [
        JsonStage(name="snapshot", value="toy_snapshot"),
        JsonStage(
            name="split",
            value={
                "role": "exploration",
                "start": "2026-01-01T00:00:00+00:00",
                "end": "2026-02-01T00:00:00+00:00",
            },
        ),
    ]
    WorkflowEngine(stages=stages, config=config, registry=registry, artifact_store=store).run(
        run_id="sources"
    )
    refs = {
        r["stage_name"]: r["artifacts"]["result"]["artifact_id"]
        for r in registry.get_run_stages("sources")
    }
    config.update(
        workflow={"item_provider": "tests.research_provider:ArithmeticCatalog"},
        feature_selection={
            "schema_version": "explicit_items_v1",
            "mode": "explicit",
            "unknown_fields": "error",
            "catalog_inventory": "selected_and_registered",
        },
        business_features={
            "pack": "toy",
            "items": [
                {"id": "short", "family": "scale", "lookback": "3s"},
                {"id": "long", "family": "scale", "lookback": "5s"},
            ],
        },
        statistical_features={
            "feature_set_version": "toy_v1",
            "items": [
                {
                    "id": "structure",
                    "source": {"kind": "business_feature", "ref": "short"},
                    "mode": "trailing",
                    "profile_window": "1m",
                    "sampling_interval": "1s",
                    "evaluation_stride": "1s",
                    "max_feature_age": "1s",
                    "min_samples": 1,
                    "metrics": ["toy.mean", "toy.max"],
                }
            ],
        },
        research_batch={
            "schema_version": "research_batch_v1",
            "id": "A",
            "parent_batch_ref": None,
            "data_scope": {
                "snapshot_ref": refs["snapshot"],
                "split_ref": refs["split"],
                "instruments": ["TOY"],
                "output_interval": {
                    "start": "2026-01-05T00:00:00+00:00",
                    "end": "2026-01-06T00:00:00+00:00",
                },
            },
            "targets": {"business_features": ["short"], "statistical_features": ["structure"]},
            "execution": {
                "allowed_stages": ["primitives", "features", "statistical_atlas", "quality"],
                "missing_dependencies": "compute_within_allowed_stages",
                "reuse_verified_artifacts": True,
            },
            "validation": {"mode": "quality", "protocol_ref": "toy_quality_v1"},
        },
    )
    ArithmeticCatalog.fail_start = ArithmeticCatalog.empty_start = None
    return config, registry, store
