"""YAML loading, basic contract validation, and stage plugin discovery."""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from .core.errors import ConfigError
from .core.types import ResourceBudget
from .core.utils import canonical_json


class ConfiguredStage:
    """Attach optional workflow YAML cache/partition metadata to a stage plugin."""

    def __init__(self, delegate: Any, *, config_keys: tuple[str, ...] | None = None,
                 partitioned: bool | None = None):
        self._delegate = delegate
        self.name = delegate.name
        self.version = delegate.version
        self.dependencies = tuple(delegate.dependencies)
        self.config_keys = config_keys if config_keys is not None else getattr(delegate, "config_keys", None)
        if partitioned is not None:
            self.partitioned = partitioned
        elif hasattr(delegate, "partitioned"):
            self.partitioned = delegate.partitioned

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)

    def validate(self, ctx: Any, inputs: dict[str, Any]) -> None:
        return self._delegate.validate(ctx, inputs)

    def execute(self, ctx: Any, inputs: dict[str, Any]) -> Any:
        return self._delegate.execute(ctx, inputs)


def sha256_text(value: str) -> str:
    import hashlib

    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def validate_config(config: Any) -> dict[str, Any]:
    if not isinstance(config, dict):
        raise ConfigError("configuration root must be a YAML mapping")
    if "project" in config and (not isinstance(config["project"], str) or not config["project"].strip()):
        raise ConfigError("project must be a non-empty string")
    seed = config.get("seed", 0)
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ConfigError("seed must be an integer")
    storage = config.get("storage", {})
    if not isinstance(storage, dict):
        raise ConfigError("storage must be a mapping")
    for key in ("artifacts_uri", "registry_uri", "temporary_uri"):
        if key in storage and storage[key] is not None and not isinstance(storage[key], str):
            raise ConfigError(f"storage.{key} must be a path string")
    resources = config.get("resources", {})
    if not isinstance(resources, dict):
        raise ConfigError("resources must be a mapping")
    try:
        ResourceBudget(
            memory_limit=resources.get("memory_limit", "8GB"),
            threads=resources.get("threads", 4),
            max_concurrent_stages=resources.get("max_concurrent_stages", 1),
            temporary_uri=resources.get("temporary_uri", storage.get("temporary_uri")),
            max_feature_candidates=resources.get("max_feature_candidates", 40),
            max_cube_cells_per_hypothesis=resources.get("max_cube_cells_per_hypothesis", 24),
            max_retries=resources.get("max_retries", 2),
            retry_delay_seconds=resources.get("retry_delay_seconds", 0.0),
        )
    except (TypeError, ValueError) as exc:
        raise ConfigError(str(exc)) from exc
    workflow = config.get("workflow", {})
    if not isinstance(workflow, dict):
        raise ConfigError("workflow must be a mapping")
    plugins = workflow.get("stages", [])
    if plugins is None:
        plugins = []
    if not isinstance(plugins, list):
        raise ConfigError("workflow.stages must be a list of import specs")
    for index, plugin in enumerate(plugins):
        if isinstance(plugin, str):
            spec = plugin
        elif isinstance(plugin, dict):
            spec = plugin.get("import")
            if not isinstance(spec, str):
                raise ConfigError(f"workflow.stages[{index}].import must be a module:attribute string")
            keys = plugin.get("config_keys")
            if keys is not None and (not isinstance(keys, list) or any(not isinstance(key, str) for key in keys)):
                raise ConfigError(f"workflow.stages[{index}].config_keys must be a list of dotted paths")
            if "partitioned" in plugin and not isinstance(plugin["partitioned"], bool):
                raise ConfigError(f"workflow.stages[{index}].partitioned must be a boolean")
        else:
            raise ConfigError(f"workflow.stages[{index}] must be a string or mapping")
        if ":" not in spec:
            raise ConfigError(f"workflow.stages[{index}] import spec must use module:attribute syntax")
    # PyYAML parses unquoted ISO dates as date objects; normalize those to ISO strings
    # so plugins, hashes, stored run configs, and subsequent CLI invocations agree.
    from .core.utils import normalize_json_value

    try:
        return normalize_json_value(config)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"configuration contains unsupported values: {exc}") from exc


def load_config(path: str | Path) -> dict[str, Any]:
    config_path = Path(path).expanduser().resolve()
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigError(f"cannot read config {config_path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in {config_path}: {exc}") from exc
    return validate_config(raw)


def config_hash(config: Mapping[str, Any]) -> str:
    return sha256_text(canonical_json(config))


def dotted_get(mapping: Mapping[str, Any], dotted_path: str) -> Any:
    current: Any = mapping
    for part in dotted_path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def stage_config(config: Mapping[str, Any], stage_name: str, config_keys: tuple[str, ...] | None) -> dict[str, Any]:
    if config_keys is None:
        # Safe default: an unscoped plugin may read any config field.
        return dict(config)
    selected: dict[str, Any] = {}
    for key in config_keys:
        selected[key] = dotted_get(config, key)
    return selected


def resource_budget(config: Mapping[str, Any]) -> ResourceBudget:
    resources = config.get("resources", {})
    storage = config.get("storage", {})
    return ResourceBudget(
        memory_limit=str(resources.get("memory_limit", "8GB")),
        threads=resources.get("threads", 4),
        max_concurrent_stages=resources.get("max_concurrent_stages", 1),
        temporary_uri=resources.get("temporary_uri", storage.get("temporary_uri")),
        max_feature_candidates=resources.get("max_feature_candidates", 40),
        max_cube_cells_per_hypothesis=resources.get("max_cube_cells_per_hypothesis", 24),
        max_retries=resources.get("max_retries", 2),
        retry_delay_seconds=resources.get("retry_delay_seconds", 0.0),
    )


def load_stage_plugins(config: Mapping[str, Any]) -> list[Any]:
    result = []
    entries = config.get("workflow", {}).get("stages", [])
    for entry in entries:
        spec = entry if isinstance(entry, str) else entry["import"]
        module_name, attribute_name = spec.split(":", 1)
        try:
            value = getattr(importlib.import_module(module_name), attribute_name)
        except (ImportError, AttributeError) as exc:
            raise ConfigError(f"cannot import stage plugin {spec!r}: {exc}") from exc
        if isinstance(value, type) or callable(value) and not hasattr(value, "execute"):
            stage = value()
        else:
            stage = value
        if isinstance(entry, dict):
            stage = ConfiguredStage(
                stage,
                config_keys=tuple(entry["config_keys"]) if "config_keys" in entry else None,
                partitioned=entry.get("partitioned"),
            )
        result.append(stage)
    return result


def configured_path(config: Mapping[str, Any], key: str, default: str) -> Path:
    storage = config.get("storage", {})
    value = storage.get(key, default)
    path = Path(value).expanduser()
    return path if path.is_absolute() else (Path.cwd() / path).resolve()
