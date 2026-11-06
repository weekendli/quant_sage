"""Explicit, catalog-backed research batches over the existing artifact engine.

Catalog plugins implement resolve(kind, parameters), get(key), authorize(scope,
validation, sources), register_validators(store), and execute(definition, ctx, inputs).
They own domain legality, input history/session permissions, and numerical work.
"""

from __future__ import annotations

import hashlib
import importlib
import re
from collections import Counter
from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from ..config import config_hash
from .errors import ConfigError, WorkflowError
from .registry import Registry
from .types import StageResult
from .utils import canonical_json
from .workflow import WorkflowEngine, source_code_revision


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def fields(value: Any, allowed: set[str], label: str) -> dict:
    if not isinstance(value, dict) or set(value) - allowed:
        raise ConfigError(f"{label}: expected mapping with fields {sorted(allowed)}")
    return value


def text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value.startswith("<"):
        raise ConfigError(f"{label} must be a non-empty resolved string")
    return value


def unique_strings(value: Any, label: str) -> list[str]:
    if not isinstance(value, list):
        raise ConfigError(f"{label} must be a list")
    result = [text(v, label) for v in value]
    if len(result) != len(set(result)):
        raise ConfigError(f"{label} contains duplicates")
    return result


def instant(value: Any) -> datetime:
    try:
        result = datetime.fromisoformat(value)
    except (ValueError, TypeError) as exc:
        raise ConfigError("scope timestamps must be timezone-aware ISO strings") from exc
    if result.tzinfo is None:
        raise ConfigError("scope timestamps must include a timezone")
    return result.astimezone(UTC)


def duration(value: Any, label: str) -> str:
    value = text(value, label)
    if not re.fullmatch(r"[1-9][0-9]*(?:ms|s|m|h|d)", value):
        raise ConfigError(f"{label} must be a positive duration such as 1s or 30m")
    return value


@dataclass(frozen=True)
class WorkDefinition:
    """Catalog key is a lookup name; semantic identity also includes dependencies."""

    key: str
    kind: str
    stage: str
    version: str
    parameters: Mapping[str, Any]
    dependencies: tuple[str, ...] = ()
    partitioned: bool = True
    pending_reason: str | None = None


def summarize(states: list[str]) -> str:
    if not states:
        return "NO_WORK"
    if "RUNNING" in states:
        return "IN_PROGRESS"
    complete = sum(s in ("SUCCEEDED", "CACHED") for s in states)
    if complete == len(states):
        return "COMPLETE"
    if complete:
        return "PARTIAL"
    for state in ("FAILED", "BLOCKED", "SKIPPED"):
        if state in states:
            return state
    return "NOT_STARTED"


class ItemStage:
    config_keys: tuple = ()

    def __init__(self, definition, identity, dependencies, provider, sources):
        self.definition = definition
        self.name = "item_" + identity
        self.version = definition.version
        self.dependencies = tuple(dependencies)
        self.partitioned = definition.partitioned
        self.provider = provider
        self.external_inputs = sources

    def register_validators(self, store):
        self.provider.register_validators(store)

    def validate(self, ctx, inputs):
        if self.definition.pending_reason:
            raise WorkflowError(self.definition.pending_reason)

    def execute(self, ctx, inputs):
        # Workers cannot accidentally make results depend on batch aliases/list order.
        scope = dict(ctx.partition_keys)
        config = {
            "work_definition": asdict(self.definition),
            "scope": scope,
            "seed": ctx.rng_seed,
            "data_snapshot_id": ctx.data_snapshot_id,
            "split_id": ctx.split_id,
        }
        result = self.provider.execute(
            self.definition, replace(ctx, config=config, stage_config=config), inputs
        )
        if not isinstance(result, StageResult) or not result.artifacts:
            raise WorkflowError(
                "item worker must publish a validated artifact, even for empty results"
            )
        # These are processed/valid row counts, not proof of a scientific effect.
        for key in ("n_total", "n_valid"):
            value = result.metrics.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise WorkflowError(f"item worker must report non-negative integer {key}")
        if result.metrics["n_valid"] > result.metrics["n_total"]:
            raise WorkflowError("n_valid cannot exceed n_total")
        if result.metrics.get("processed_scope") != "complete":
            raise WorkflowError("worker must explicitly account for the complete requested scope")
        if self.definition.kind == "validation":
            verdicts = (
                ("PASS", "FAIL")
                if self.definition.parameters.get("mode") == "quality"
                else ("SUPPORTED_IN_SCOPE", "INCONCLUSIVE", "FALSIFIED")
            )
            if result.metrics.get("verdict") not in verdicts:
                raise WorkflowError("validation worker must report a separate domain verdict")
        return result


class BatchEngine(WorkflowEngine):
    def __init__(self, config, *, registry, artifact_store):
        self.original_config = config
        batch = fields(
            config.get("research_batch"),
            {
                "schema_version",
                "id",
                "parent_batch_ref",
                "data_scope",
                "targets",
                "execution",
                "validation",
            },
            "research_batch",
        )
        if batch.get("schema_version") != "research_batch_v1":
            raise ConfigError("unsupported research_batch.schema_version")
        text(batch.get("id"), "batch id")
        workflow = config.get("workflow", {})
        spec = text(workflow.get("item_provider"), "workflow.item_provider")
        if workflow.get("stages"):
            raise ConfigError("research batches use item_provider, not workflow.stages")
        try:
            module, name = spec.split(":")
            self.provider = getattr(importlib.import_module(module), name)()
        except (ValueError, AttributeError, ImportError, TypeError) as exc:
            raise ConfigError(f"cannot load item_provider {spec}: {exc}") from exc
        scope = fields(
            batch.get("data_scope"),
            {"snapshot_ref", "split_ref", "instruments", "output_interval"},
            "data_scope",
        )
        instruments = unique_strings(scope.get("instruments"), "instruments")
        if not instruments:
            raise ConfigError("data scope requires at least one instrument")
        interval = fields(scope.get("output_interval"), {"start", "end"}, "output_interval")
        start, end = instant(interval.get("start")), instant(interval.get("end"))
        if start >= end:
            raise ConfigError("output_interval must have start < end")
        # Stable UTC-day partitions; partial boundaries remain distinct cache scopes.
        self.partitions = []
        current = start
        max_partitions = config.get("resources", {}).get("max_work_partitions", 10000)
        if type(max_partitions) is not int or max_partitions < 1:
            raise ConfigError("max_work_partitions must be a positive integer")
        while current < end:
            stop = min(
                end, current.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            )
            for instrument in sorted(instruments):
                self.partitions.append(
                    {
                        "instrument_id": instrument,
                        "start": current.isoformat(),
                        "end": stop.isoformat(),
                    }
                )
                if len(self.partitions) > max_partitions:
                    raise ConfigError("data partition budget exceeded")
            current = stop
        self.sources = {}
        self.provider.register_validators(artifact_store)
        for key in ("snapshot_ref", "split_ref"):
            source = registry.find_artifact_manifest(text(scope.get(key), key))
            if source is None:
                raise ConfigError(f"{key} not found in the artifact registry")
            ref, _ = source
            artifact_store.verify(ref)
            self.sources[key] = ref
        validation = fields(batch.get("validation"), {"mode", "protocol_ref"}, "validation")
        if validation.get("mode") not in ("quality", "exploratory", "confirmatory"):
            raise ConfigError("validation.mode must be quality, exploratory or confirmatory")
        text(validation.get("protocol_ref"), "validation.protocol_ref")
        # A trusted domain plugin must verify inventory, sessions, split permissions,
        # read support and confirmation authorization; the engine never guesses them.
        self.authorization = self.provider.authorize(scope, validation, self.sources)
        if not isinstance(self.authorization, dict) or not self.authorization.get("authorized"):
            raise ConfigError("provider did not authorize the requested data/validation scope")
        execution = fields(
            batch.get("execution"),
            {"allowed_stages", "missing_dependencies", "reuse_verified_artifacts"},
            "execution",
        )
        self.allowed = set(unique_strings(execution.get("allowed_stages"), "allowed_stages"))
        policy = execution.get("missing_dependencies")
        if policy not in ("block", "compute_within_allowed_stages"):
            raise ConfigError("invalid missing_dependencies policy")
        if execution.get("reuse_verified_artifacts") is not True:
            raise ConfigError("this implementation requires reuse_verified_artifacts: true")
        selection = fields(
            config.get("feature_selection"),
            {"schema_version", "mode", "unknown_fields", "catalog_inventory"},
            "feature_selection",
        )
        if selection != {
            "schema_version": "explicit_items_v1",
            "mode": "explicit",
            "unknown_fields": "error",
            "catalog_inventory": "selected_and_registered",
        }:
            raise ConfigError("feature_selection must declare the explicit_items_v1 contract")
        business = fields(
            config.get("business_features", {}), {"pack", "items"}, "business_features"
        )
        statistical = fields(
            config.get("statistical_features", {}),
            {"feature_set_version", "items"},
            "statistical_features",
        )
        self.definitions = {}
        self.aliases = {}
        aliases = {}

        def register(alias, definitions):
            text(alias, "item id")
            if alias in aliases:
                raise ConfigError(f"duplicate item id: {alias}")
            aliases[alias] = definitions
            for definition in definitions:
                self._register(definition)

        items = business.get("items", [])
        if not isinstance(items, list):
            raise ConfigError("business_features.items must be a list")
        for item in items:
            item = fields(item, {"id", "family", "space", "size", "lookback"}, "business item")
            params = {k: text(v, k) for k, v in item.items() if k != "id"}
            text(params.get("family"), "family")
            duration(params.get("lookback"), "lookback")
            params["pack"] = text(business.get("pack"), "pack")
            register(item.get("id"), [self.provider.resolve("business", params)])
        items = statistical.get("items", [])
        if not isinstance(items, list):
            raise ConfigError("statistical_features.items must be a list")
        for item in items:
            item = fields(
                item,
                {
                    "id",
                    "source",
                    "mode",
                    "profile_window",
                    "sampling_interval",
                    "evaluation_stride",
                    "max_feature_age",
                    "min_samples",
                    "metrics",
                    "metric_set",
                },
                "statistical item",
            )
            source = fields(item.get("source"), {"kind", "ref"}, "statistical source")
            ref = text(source.get("ref"), "source.ref")
            if source.get("kind") == "business_feature":
                if ref not in aliases or len(aliases[ref]) != 1:
                    raise ConfigError(f"unknown/ambiguous business source: {ref}")
                source_definition = aliases[ref][0]
                if source_definition.kind != "business":
                    raise ConfigError("statistical source must be a business definition")
            elif source.get("kind") == "primitive":
                source_definition = self.provider.get(ref)
                if source_definition.kind != "primitive":
                    raise ConfigError("primitive source must resolve to a primitive definition")
                self._register(source_definition)
            else:
                raise ConfigError("source.kind must be business_feature or primitive")
            if ("metrics" in item) == ("metric_set" in item):
                raise ConfigError("specify exactly one of metrics or metric_set")
            if "metrics" in item:
                metrics = unique_strings(item["metrics"], "metrics")
            else:
                metric_set = text(item["metric_set"], "metric_set")
                if metric_set not in self.provider.metric_sets:
                    raise ConfigError(f"unknown metric_set: {metric_set}")
                metrics = unique_strings(
                    self.provider.metric_sets[metric_set], "metric_set members"
                )
            if not metrics:
                raise ConfigError("a statistical item requires at least one metric")
            params = {
                k: v for k, v in item.items() if k not in ("id", "source", "metrics", "metric_set")
            }
            if params.get("mode") not in ("trailing", "segment_summary"):
                raise ConfigError("statistical mode must be trailing or segment_summary")
            for key in (
                "profile_window",
                "sampling_interval",
                "evaluation_stride",
                "max_feature_age",
            ):
                duration(params.get(key), key)
            if "min_samples" not in params:
                raise ConfigError("min_samples is required")
            for k, v in params.items():
                if k == "min_samples":
                    if type(v) is not int or v < 1:
                        raise ConfigError("min_samples must be a positive integer")
                else:
                    text(v, k)
            params.update(
                source=source_definition.key,
                feature_set_version=text(
                    statistical.get("feature_set_version"), "feature_set_version"
                ),
            )
            register(
                item.get("id"),
                [self.provider.resolve("statistical", {**params, "metric": m}) for m in metrics],
            )
        targets = fields(
            batch.get("targets"),
            {"business_features", "statistical_features", "primitives"},
            "targets",
        )
        requested = set()
        for category, kind in (
            ("business_features", "business"),
            ("statistical_features", "statistical"),
            ("primitives", "primitive"),
        ):
            for alias in unique_strings(targets.get(category, []), category):
                if kind == "primitive":
                    definition = self.provider.get(alias)
                    self._register(definition)
                    aliases.setdefault(alias, [definition])
                if alias not in aliases or any(d.kind != kind for d in aliases[alias]):
                    raise ConfigError(f"unknown target {kind}: {alias}")
                self.aliases[alias] = [d.key for d in aliases[alias]]
                requested.update(self.aliases[alias])
        self.requested = requested
        needed = set()
        active = set()

        def visit(key):
            if key in active:
                raise ConfigError("item dependency cycle")
            if key in needed:
                return
            active.add(key)
            d = self.definitions.get(key) or self.provider.get(key)
            self._register(d)
            for dep in d.dependencies:
                visit(dep)
            active.remove(key)
            needed.add(key)

        for key in sorted(requested):
            visit(key)
        if needed:
            protocol = self.provider.resolve(
                "validation", {**validation, "targets": sorted(requested), "scope": scope}
            )
            self._register(protocol)
            visit(protocol.key)
            self.validation_key = protocol.key
        else:
            self.validation_key = None
        max_items = config.get("resources", {}).get("max_feature_candidates", 40)
        if len(requested) > max_items:
            raise ConfigError("requested output budget exceeded")
        max_dependencies = config.get("resources", {}).get("max_dependency_items", 10000)
        if (
            type(max_dependencies) is not int
            or max_dependencies < 1
            or len(needed) > max_dependencies
        ):
            raise ConfigError("dependency budget exceeded")
        max_units = config.get("resources", {}).get("max_work_units", 100000)
        units = sum(len(self.partitions) if self.definitions[k].partitioned else 1 for k in needed)
        if type(max_units) is not int or max_units < 1 or units > max_units:
            raise ConfigError("partitioned work-unit budget exceeded")
        self.identities = {}

        def identity(key):
            if key not in self.identities:
                d = self.definitions[key]
                self.identities[key] = digest(
                    {
                        "provider": spec,
                        "kind": d.kind,
                        "stage": d.stage,
                        "version": d.version,
                        "parameters": d.parameters,
                        "pending_reason": d.pending_reason,
                        "partitioned": d.partitioned,
                        "dependencies": sorted(identity(x) for x in d.dependencies),
                    }
                )
            return self.identities[key]

        stages = [
            ItemStage(
                self.definitions[k],
                identity(k),
                ["item_" + identity(dep) for dep in self.definitions[k].dependencies],
                self.provider,
                self.sources,
            )
            for k in sorted(needed)
        ]
        self.executable = [
            s.name
            for s in stages
            if s.definition.stage in self.allowed
            and not s.definition.pending_reason
            and (
                policy != "block"
                or s.definition.key in requested
                or s.definition.key == self.validation_key
            )
        ]
        self.work = {
            s.name: {
                **asdict(s.definition),
                "semantic_hash": self.identities[s.definition.key],
                "selection": "REQUESTED" if s.definition.key in requested else "DEPENDENCY_ONLY",
            }
            for s in stages
        }
        self.inventory = {alias: [d.key for d in defs] for alias, defs in aliases.items()}
        normalized = dict(config)
        normalized["data_snapshot_id"] = self.sources["snapshot_ref"].artifact_id
        normalized["splits"] = {"split_id": self.sources["split_ref"].artifact_id}
        # Include provider sources in revisions, not just the generic adapter source.
        super().__init__(
            stages=stages,
            config=normalized,
            registry=registry,
            artifact_store=artifact_store,
            code_revision=source_code_revision([self.provider]),
        )

    def _register(self, definition):
        if not isinstance(definition, WorkDefinition):
            raise ConfigError("catalog must return WorkDefinition")
        for name in ("key", "kind", "stage", "version"):
            text(getattr(definition, name), f"WorkDefinition.{name}")
        if definition.kind not in ("primitive", "business", "statistical", "validation"):
            raise ConfigError("unknown WorkDefinition.kind")
        if (
            not isinstance(definition.parameters, Mapping)
            or not isinstance(definition.dependencies, tuple)
            or any(not isinstance(x, str) or not x for x in definition.dependencies)
            or len(set(definition.dependencies)) != len(definition.dependencies)
        ):
            raise ConfigError("definition parameters/dependencies must be typed and unique")
        if type(definition.partitioned) is not bool:
            raise ConfigError("definition.partitioned must be boolean")
        canonical_json(definition.parameters)
        prior = self.definitions.get(definition.key)
        if prior is not None and prior != definition:
            raise ConfigError(f"conflicting definition: {definition.key}")
        self.definitions[definition.key] = definition

    def graph(self):
        result = super().graph()
        result["research_plan"] = {
            "schema_version": "research_plan_v1",
            "work": self.work,
            "aliases": self.inventory,
            "requested": sorted(self.requested),
            "partitions": self.partitions,
            "authorization": self.authorization,
            "validation_key": self.validation_key,
        }
        result["research_plan"]["plan_hash"] = digest(result["research_plan"])
        return json_normalized(result)

    def batch_plan(self):
        result = self.graph()
        result["selected"] = self.executable
        result["cache_only"] = sorted(set(self.stages) - set(self.executable))
        return json_normalized(result)

    def select_stages(self, **selectors):
        # Empty authorization must mean cache-only, never the base engine's all-stages default.
        return set(self.executable), set(self.stages)

    def run_batch(self, *, run_id=None, resume=None):
        if resume:
            prior = self.registry.get_run(resume)
            if prior is None or config_hash(prior["config"]) != config_hash(self.config):
                raise ConfigError("resume requires the original frozen batch configuration")
        parent = self.original_config["research_batch"].get("parent_batch_ref")
        if parent:
            prior = self.registry.get_run(parent)
            if prior is None or "research_plan" not in prior["graph"]:
                raise ConfigError(f"unknown parent research batch run: {parent}")
        result = self.run(
            run_id=run_id, resume=resume, only=self.executable, partitions=self.partitions
        )
        return batch_status(self.registry, result.run_id)


def json_normalized(value):
    from .utils import normalize_json_value

    return normalize_json_value(value)


def batch_status(registry: Registry, run_id: str) -> dict:
    run = registry.get_run(run_id)
    if run is None:
        raise WorkflowError(f"unknown run_id: {run_id}")
    plan = run["graph"].get("research_plan")
    if plan is None:
        raise WorkflowError("run is not a research batch")
    records = registry.get_run_stages(run_id)
    work_rows = []
    for name, definition in sorted(plan["work"].items()):
        scopes = plan["partitions"] if definition["partitioned"] else [{}]
        latest = {canonical_json(r["partition"]): r for r in records if r["stage_name"] == name}
        partitions = []
        for scope in scopes:
            row = latest.get(canonical_json(scope))
            evidence = row
            if row and row["status"] == "CACHED" and row["source_stage_run_id"]:
                evidence = registry.get_stage_run_by_id(row["source_stage_run_id"]) or row
            partitions.append(
                {
                    "scope": scope,
                    "state": row["status"] if row else "PENDING",
                    "attempts": row["attempt"] if row else 0,
                    "error": row["error"] if row else None,
                    "metrics": evidence["metrics"] if evidence else {},
                    "history": registry.get_stage_events(row["stage_run_id"]) if row else [],
                    "artifacts": row["artifacts"] if row else {},
                    "cache_source": row["source_stage_run_id"] if row else None,
                }
            )
        work_rows.append(
            {
                **definition,
                "partitions": partitions,
                "aliases": sorted(
                    a for a, ids in plan["aliases"].items() if definition["key"] in ids
                ),
                "summary": summarize([r["state"] for r in partitions]),
            }
        )
    parent_ref = run["config"]["research_batch"].get("parent_batch_ref")
    parent = registry.get_run(parent_ref) if parent_ref else None
    prior_plan = parent["graph"].get("research_plan", {}) if parent else {}
    prior_requested = set(prior_plan.get("requested", []))
    prior_work = {d["key"]: d for d in prior_plan.get("work", {}).values()}
    return {
        "run_id": run_id,
        "batch_id": run["config"]["research_batch"]["id"],
        "status": run["status"],
        "summary": summarize([p["state"] for row in work_rows for p in row["partitions"]]),
        "compute_summary": summarize(
            [
                p["state"]
                for row in work_rows
                if row["key"] in plan["requested"]
                for p in row["partitions"]
            ]
        ),
        "plan_hash": plan["plan_hash"],
        "items": work_rows,
        "inventory": [
            {
                "alias": a,
                "definitions": ids,
                "selection": "REQUESTED"
                if set(ids) & set(plan["requested"])
                else "DEPENDENCY_ONLY"
                if set(ids) & {r["key"] for r in work_rows}
                else "NOT_SELECTED",
            }
            for a, ids in sorted(plan["aliases"].items())
        ],
        "state_counts": dict(Counter(p["state"] for row in work_rows for p in row["partitions"])),
        "parent_batch_ref": parent_ref,
        "delta": {
            "added_definitions": sorted(set(plan["requested"]) - prior_requested),
            "removed_definitions": sorted(prior_requested - set(plan["requested"])),
            "changed_definitions": sorted(
                r["key"]
                for r in work_rows
                if r["key"] in prior_work
                and r["semantic_hash"] != prior_work[r["key"]]["semantic_hash"]
            ),
            "common_partitions": [
                p for p in plan["partitions"] if p in prior_plan.get("partitions", [])
            ],
            "added_partitions": [
                p for p in plan["partitions"] if p not in prior_plan.get("partitions", [])
            ],
        },
        "validation": [row for row in work_rows if row["kind"] == "validation"],
    }
