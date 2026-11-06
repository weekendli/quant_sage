"""DAG validation, deterministic local scheduling, caching, resume, and stage execution."""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import shutil
import threading
import time
import traceback
import uuid
from collections.abc import Iterable, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..config import canonical_json, config_hash, resource_budget, stage_config
from .artifacts import ArtifactStore
from .errors import ArtifactError, GraphError, StageExecutionError, TransientError, WorkflowError
from .registry import Registry, StructuredLogger, new_stage_run_id
from .types import (
    ArtifactRef,
    RunContext,
    Stage,
    StageStatus,
    stage_config_keys,
    stage_is_partitioned,
)


@dataclass(frozen=True)
class _Job:
    stage_name: str
    partition_items: tuple[tuple[str, str], ...]

    @property
    def partition(self) -> dict[str, str]:
        return dict(self.partition_items)

    @property
    def key(self) -> tuple[str, str]:
        return self.stage_name, canonical_json(self.partition)


@dataclass
class _JobResult:
    status: StageStatus
    artifacts: dict[str, ArtifactRef]
    stage_run_id: str | None = None
    error: dict[str, Any] | None = None


@dataclass(frozen=True)
class RunSummary:
    run_id: str
    status: str
    stage_results: Mapping[str, Any]
    message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"run_id": self.run_id, "status": self.status,
                "stage_results": dict(self.stage_results), "message": self.message}


def source_code_revision(stages: Iterable[Any]) -> str:
    """Hash engine and plugin source so uncommitted code changes invalidate cache entries."""
    sources = {Path(__file__).resolve(), Path(__file__).parent / "artifacts.py"}
    package_dir = Path(__file__).resolve().parents[1]
    sources.update(p.resolve() for p in package_dir.rglob("*.py") if "__pycache__" not in p.parts)
    for stage in stages:
        stage = getattr(stage, "_delegate", stage)
        try:
            source = inspect.getsourcefile(type(getattr(stage, "_delegate", stage)))
        except (TypeError, OSError):
            source = None
        if source:
            sources.add(Path(source).resolve())
    digest = hashlib.sha256()
    for path in sorted(sources):
        if not path.is_file():
            continue
        relative = path.name if package_dir not in path.parents else path.relative_to(package_dir).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        digest.update(b"\0")
    return "sha256:" + digest.hexdigest()


class WorkflowEngine:
    """Local plugin engine; it knows the stage graph, never market-specific algorithms."""

    def __init__(self, *, stages: Iterable[Stage] = (), config: Mapping[str, Any] | None = None,
                 artifact_store: ArtifactStore | None = None,
                 registry: Registry | None = None,
                 logger: StructuredLogger | None = None,
                 code_revision: str | None = None):
        self.config = dict(config or {})
        self.config_digest = config_hash(self.config)
        budget = resource_budget(self.config)
        storage = self.config.get("storage", {})
        artifacts_root = Path(storage.get("artifacts_uri", "artifacts")).expanduser()
        if not artifacts_root.is_absolute():
            artifacts_root = (Path.cwd() / artifacts_root).resolve()
        registry_path = Path(storage.get("registry_uri", artifacts_root / "registry.duckdb")).expanduser()
        if not registry_path.is_absolute():
            registry_path = (Path.cwd() / registry_path).resolve()
        self.artifact_store = artifact_store or ArtifactStore(artifacts_root)
        self.registry = registry or Registry(registry_path)
        self.resource_budget = budget
        self.stages: dict[str, Stage] = {}
        self._stage_locks: dict[str, threading.Lock] = {}
        for stage in stages:
            self.register_stage(stage)
        self.code_revision = code_revision or source_code_revision(self.stages.values())
        for stage in self.stages.values():
            hook = getattr(stage, "register_validators", None)
            if hook is not None:
                hook(self.artifact_store)
        self.logger_factory = logger

    def register_stage(self, stage: Stage) -> None:
        name = getattr(stage, "name", None)
        version = getattr(stage, "version", None)
        dependencies = getattr(stage, "dependencies", None)
        if not isinstance(name, str) or not name.strip():
            raise GraphError("every stage must declare a non-empty name")
        if not isinstance(version, str) or not version.strip():
            raise GraphError(f"stage {name!r} must declare a non-empty version")
        if not isinstance(dependencies, (tuple, list)) or any(not isinstance(d, str) for d in dependencies):
            raise GraphError(f"stage {name!r} dependencies must be a tuple/list of names")
        if len(set(dependencies)) != len(dependencies):
            raise GraphError(f"stage {name!r} declares a dependency more than once")
        if not callable(getattr(stage, "validate", None)) or not callable(getattr(stage, "execute", None)):
            raise GraphError(f"stage {name!r} must implement validate() and execute()")
        if name in self.stages:
            raise GraphError(f"duplicate stage name: {name}")
        stage_config_keys(stage)
        self.stages[name] = stage
        self._stage_locks[name] = threading.Lock()

    def topological_order(self) -> list[str]:
        for stage in self.stages.values():
            missing = sorted(set(stage.dependencies) - self.stages.keys())
            if missing:
                raise GraphError(f"stage {stage.name!r} has missing dependency: {', '.join(missing)}")
        indegree = {name: len(stage.dependencies) for name, stage in self.stages.items()}
        children: dict[str, list[str]] = {name: [] for name in self.stages}
        for name, stage in self.stages.items():
            for dependency in stage.dependencies:
                children[dependency].append(name)
        ready = sorted(name for name, degree in indegree.items() if degree == 0)
        ordered: list[str] = []
        while ready:
            name = ready.pop(0)
            ordered.append(name)
            for child in sorted(children[name]):
                indegree[child] -= 1
                if indegree[child] == 0:
                    ready.append(child)
                    ready.sort()
        if len(ordered) != len(self.stages):
            cyclic = sorted(name for name, degree in indegree.items() if degree > 0)
            raise GraphError("workflow contains a dependency cycle involving: " + ", ".join(cyclic))
        return ordered

    def graph(self) -> dict[str, Any]:
        order = self.topological_order()
        return {"order": order,
                "stages": [{"name": name, "version": self.stages[name].version,
                            "dependencies": list(self.stages[name].dependencies),
                            "partitioned": stage_is_partitioned(self.stages[name])}
                           for name in order]}

    def select_stages(self, *, from_stage: str | None = None, until_stage: str | None = None,
                      only: Iterable[str] | None = None) -> tuple[set[str], set[str]]:
        order = self.topological_order()
        if not order:
            if from_stage or until_stage or list(only or ()):
                raise GraphError("stage selectors cannot be used when no stages are registered")
            return set(), set()
        only_names = set(only or ())
        unknown = (only_names - self.stages.keys()) | ({from_stage} - self.stages.keys() if from_stage else set()) \
                  | ({until_stage} - self.stages.keys() if until_stage else set())
        if unknown:
            raise GraphError("unknown stage selector: " + ", ".join(sorted(unknown)))
        if only_names and (from_stage or until_stage):
            raise GraphError("--only cannot be combined with --from or --until")
        if only_names:
            selected = set(only_names)
        else:
            first = order.index(from_stage) if from_stage else 0
            last = order.index(until_stage) if until_stage else len(order) - 1
            if first > last:
                raise GraphError("--from stage comes after --until stage in deterministic order")
            selected = set(order[first:last + 1])
        needed = set(selected)
        queue = list(selected)
        while queue:
            current = queue.pop()
            for dependency in self.stages[current].dependencies:
                if dependency not in needed:
                    needed.add(dependency)
                    queue.append(dependency)
        return selected, needed

    def plan(self, *, from_stage: str | None = None, until_stage: str | None = None,
             only: Iterable[str] | None = None) -> dict[str, Any]:
        graph = self.graph()
        selected, needed = self.select_stages(from_stage=from_stage, until_stage=until_stage, only=only)
        graph["selected"] = sorted(selected, key=self.topological_order().index)
        graph["cache_only_prerequisites"] = sorted(needed - selected, key=self.topological_order().index)
        graph["skipped"] = sorted(self.stages.keys() - needed)
        graph["message"] = None if graph["order"] else "No stages registered; no work will run."
        return graph

    def _run_context(self, *, run_id: str, stage: Stage, stage_run_id: str,
                     partition: Mapping[str, str], transaction: Any,
                     logger: StructuredLogger, snapshot_id: str | None,
                     split_id: str | None, as_of_cutoff: str | None,
                     stage_code_revision: str) -> RunContext:
        return RunContext(
            run_id=run_id,
            config_hash=self.config_digest,
            split_id=split_id,
            data_snapshot_id=snapshot_id,
            as_of_cutoff=as_of_cutoff,
            artifact_store=self.artifact_store,
            artifact_transaction=transaction,
            rng_seed=self.config.get("seed", 0),
            logger=logger,
            resource_budget=self.resource_budget,
            config=self.config,
            stage_config=stage_config(self.config, stage.name, stage_config_keys(stage)),
            stage_run_id=stage_run_id,
            stage_name=stage.name,
            partition_keys=partition,
            code_revision=stage_code_revision,
        )

    def _snapshot_id(self) -> str | None:
        value = self.config.get("data_snapshot_id")
        if value is None:
            input_config = self.config.get("input", {})
            if isinstance(input_config, Mapping):
                value = input_config.get("data_snapshot_id", input_config.get("snapshot_id"))
        return str(value) if value is not None else None

    def _split_id(self) -> str | None:
        splits = self.config.get("splits", {})
        value = splits.get("split_id") if isinstance(splits, Mapping) else None
        return str(value) if value is not None else None

    def _as_of_cutoff(self) -> str | None:
        value = self.config.get("as_of_cutoff")
        return str(value) if value is not None else None

    def _stage_code_revision(self, stage: Stage) -> str:
        digest = hashlib.sha256()
        digest.update(self.code_revision.encode("utf-8"))
        plugin_stage = getattr(stage, "_delegate", stage)
        try:
            source = inspect.getsourcefile(type(plugin_stage))
        except (TypeError, OSError):
            source = None
        if source and Path(source).is_file():
            with Path(source).open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
        return "sha256:" + digest.hexdigest()

    def _cache_key(self, stage: Stage, partition: Mapping[str, str],
                   inputs: Mapping[str, ArtifactRef], stage_code_revision: str) -> tuple[str, str]:
        subset = stage_config(self.config, stage.name, stage_config_keys(stage))
        subset_hash = hashlib.sha256(canonical_json(subset).encode("utf-8")).hexdigest()
        input_fingerprints = {
            key: {"artifact_id": ref.artifact_id, "content_hash": ref.content_hash,
                  "producer_cache_key": ref.cache_key}
            for key, ref in sorted(inputs.items())
        }
        descriptor = {
            "stage": stage.name,
            "version": stage.version,
            "stage_config_hash": subset_hash,
            "inputs": input_fingerprints,
            "partition": dict(partition),
            "data_snapshot_id": self._snapshot_id(),
            "split_id": self._split_id(),
            "as_of_cutoff": self._as_of_cutoff(),
            "rng_seed": self.config.get("seed", 0),
            "code_revision": stage_code_revision,
        }
        key = "sha256:" + hashlib.sha256(canonical_json(descriptor).encode("utf-8")).hexdigest()
        return key, "sha256:" + subset_hash

    @staticmethod
    def _refs_from_stage_record(record: Mapping[str, Any]) -> dict[str, ArtifactRef]:
        return {alias: ArtifactRef.from_dict(raw) for alias, raw in record.get("artifacts", {}).items()}

    def _verify_outputs(self, stage: Stage, record: Mapping[str, Any], cache_key: str,
                        inputs: Mapping[str, ArtifactRef], partition: Mapping[str, str],
                        code_revision: str) -> dict[str, ArtifactRef]:
        refs = self._refs_from_stage_record(record)
        if not refs and record.get("status") not in {StageStatus.SUCCEEDED.value, StageStatus.CACHED.value}:
            raise ArtifactError("completed stage has no artifact references")
        expected_inputs = sorted({ref.artifact_id for ref in inputs.values()})
        for ref in refs.values():
            self.artifact_store.verify(ref, expected={
                "stage_name": stage.name,
                "stage_version": stage.version,
                "cache_key": cache_key,
                "code_revision": code_revision,
                "input_artifact_ids": expected_inputs,
                "data_snapshot_id": self._snapshot_id(),
                "split_id": self._split_id(),
                "partition_keys": dict(partition),
            })
        return refs

    def _load_valid_cache(self, stage: Stage, cache_key: str, inputs: Mapping[str, ArtifactRef],
                          partition: Mapping[str, str], code_revision: str,
                          logger: StructuredLogger) -> tuple[dict[str, ArtifactRef], str] | None:
        while True:
            record = self.registry.find_cache(stage.name, stage.version, cache_key)
            if record is None:
                return None
            try:
                outputs = self._verify_outputs(stage, record, cache_key, inputs, partition, code_revision)
                logger.emit("cache_hit", stage=stage.name, stage_run_id=record["stage_run_id"],
                            partition=dict(partition), cache_key=cache_key,
                            output_hashes={key: ref.content_hash for key, ref in outputs.items()},
                            source_stage_run_id=record["stage_run_id"])
                return outputs, record["stage_run_id"]
            except (ArtifactError, WorkflowError, OSError, ValueError, TypeError, KeyError) as exc:
                self.registry.invalidate_stage_run(record["stage_run_id"])
                logger.emit("cache_invalid", stage=stage.name, stage_run_id=record["stage_run_id"],
                            partition=dict(partition), cache_key=cache_key, error=str(exc))

    def _execute_one(self, *, run_id: str, job: _Job, stage: Stage,
                     inputs: Mapping[str, ArtifactRef], logger: StructuredLogger,
                     force: bool, allow_execute: bool) -> _JobResult:
        partition = job.partition
        stage_code_revision = self._stage_code_revision(stage)
        cache_key, _ = self._cache_key(stage, partition, inputs, stage_code_revision)
        latest = self.registry.get_stage_run(run_id, stage.name, partition)
        invalid_current = False
        if latest and latest["status"] in {StageStatus.SUCCEEDED.value, StageStatus.CACHED.value} and not force:
            try:
                output_refs = self._verify_outputs(stage, latest, cache_key, inputs, partition,
                                                   stage_code_revision)
                return _JobResult(StageStatus(latest["status"]), output_refs, latest["stage_run_id"])
            except (ArtifactError, WorkflowError, OSError, ValueError, TypeError, KeyError) as exc:
                self.registry.invalidate_stage_run(latest["stage_run_id"])
                logger.emit("resume_output_invalid", stage=stage.name,
                            stage_run_id=latest["stage_run_id"], partition=partition, error=str(exc))
                invalid_current = True
        if not allow_execute or not force:
            cache_hit = self._load_valid_cache(stage, cache_key, inputs, partition,
                                               stage_code_revision, logger)
            if cache_hit:
                output_refs, source_id = cache_hit
                row = self.registry.ensure_stage_run(
                    run_id=run_id, stage_name=stage.name, stage_version=stage.version,
                    partition=partition, stage_run_id=new_stage_run_id(), cache_key=cache_key,
                    config_hash=self.config_digest, code_revision=stage_code_revision,
                    force_new=bool(latest and latest["status"] in {
                        StageStatus.SUCCEEDED.value, StageStatus.CACHED.value
                    }) or invalid_current,
                )
                if row["status"] not in {StageStatus.SUCCEEDED.value, StageStatus.CACHED.value}:
                    self.registry.set_stage_complete(row["stage_run_id"], StageStatus.CACHED,
                                                     artifacts=output_refs,
                                                     source_stage_run_id=source_id)
                return _JobResult(StageStatus.CACHED, output_refs, row["stage_run_id"])
        if not allow_execute:
            error = {"type": "MissingPrerequisiteArtifact",
                     "message": f"cache-only prerequisite {stage.name!r} has no valid cached output"}
            row = self.registry.ensure_stage_run(
                run_id=run_id, stage_name=stage.name, stage_version=stage.version,
                partition=partition, stage_run_id=new_stage_run_id(), cache_key=cache_key,
                config_hash=self.config_digest, code_revision=stage_code_revision,
                force_new=bool(latest and latest["status"] in {
                    StageStatus.SUCCEEDED.value, StageStatus.CACHED.value
                }) or invalid_current,
            )
            self.registry.set_stage_status(row["stage_run_id"], StageStatus.BLOCKED, error=error)
            return _JobResult(StageStatus.BLOCKED, {}, row["stage_run_id"], error)
        current = self.registry.ensure_stage_run(
            run_id=run_id, stage_name=stage.name, stage_version=stage.version,
            partition=partition, stage_run_id=new_stage_run_id(), cache_key=cache_key,
            config_hash=self.config_digest, code_revision=stage_code_revision,
            force_new=force or bool(latest and latest["status"] in {
                StageStatus.SUCCEEDED.value, StageStatus.CACHED.value
            }) or invalid_current,
        )
        stage_run_id = current["stage_run_id"]
        if current["status"] in {StageStatus.SUCCEEDED.value, StageStatus.CACHED.value} and not force:
            # This is the resumed current-run record. Its outputs were verified above.
            outputs = self._refs_from_stage_record(current)
            return _JobResult(StageStatus(current["status"]), outputs, stage_run_id)
        # A process may have died while writing. Only unregistered temporary/final paths are removed.
        staging_root = self.artifact_store.root / ".staging"
        if staging_root.exists():
            for path in staging_root.glob(stage_run_id + "-*"):
                shutil.rmtree(path, ignore_errors=True)
        shutil.rmtree(self.artifact_store.root / "runs" / run_id / "stages" / stage_run_id,
                      ignore_errors=True)
        attempt = self.registry.set_stage_running(
            stage_run_id, cache_key=cache_key, config_hash=self.config_digest,
            code_revision=stage_code_revision, inputs=inputs,
        )
        started = time.perf_counter()
        logger.emit("stage_started", stage=stage.name, stage_run_id=stage_run_id,
                    partition=partition, attempt=attempt, cache_key=cache_key,
                    input_hashes={key: ref.content_hash for key, ref in sorted(inputs.items())},
                    resource_budget=self.resource_budget.__dict__)
        transaction = None
        try:
            transaction = self.artifact_store.begin(
                self._run_context(run_id=run_id, stage=stage, stage_run_id=stage_run_id,
                                  partition=partition, transaction=None, logger=logger,
                                  snapshot_id=self._snapshot_id(), split_id=self._split_id(),
                                  as_of_cutoff=self._as_of_cutoff(),
                                  stage_code_revision=stage_code_revision),
                stage.name, stage.version, cache_key, inputs,
            )
            ctx = self._run_context(run_id=run_id, stage=stage, stage_run_id=stage_run_id,
                                    partition=partition, transaction=transaction, logger=logger,
                                    snapshot_id=self._snapshot_id(), split_id=self._split_id(),
                                    as_of_cutoff=self._as_of_cutoff(),
                                    stage_code_revision=stage_code_revision)
            for ref in inputs.values():
                self.artifact_store.verify(ref)
            try:
                stage.validate(ctx, dict(inputs))
            except Exception as exc:
                raise StageExecutionError(f"stage validation failed: {exc}") from exc
            result = None
            retries = 0
            while True:
                try:
                    result = stage.execute(ctx, dict(inputs))
                    break
                except TransientError as exc:
                    if retries >= self.resource_budget.max_retries:
                        raise StageExecutionError(
                            f"transient execution failure exhausted {retries} retries: {exc}"
                        ) from exc
                    retries += 1
                    transaction.abort()
                    logger.emit("stage_retry", stage=stage.name, stage_run_id=stage_run_id,
                                partition=partition, retry=retries, error=str(exc))
                    if self.resource_budget.retry_delay_seconds:
                        time.sleep(self.resource_budget.retry_delay_seconds)
                    transaction = self.artifact_store.begin(
                        self._run_context(run_id=run_id, stage=stage, stage_run_id=stage_run_id,
                                          partition=partition, transaction=None, logger=logger,
                                          snapshot_id=self._snapshot_id(), split_id=self._split_id(),
                                          as_of_cutoff=self._as_of_cutoff(),
                                          stage_code_revision=stage_code_revision),
                        stage.name, stage.version, cache_key, inputs,
                    )
                    ctx = self._run_context(run_id=run_id, stage=stage, stage_run_id=stage_run_id,
                                            partition=partition, transaction=transaction, logger=logger,
                                            snapshot_id=self._snapshot_id(), split_id=self._split_id(),
                                            as_of_cutoff=self._as_of_cutoff(),
                                            stage_code_revision=stage_code_revision)
            elapsed = (time.perf_counter() - started) * 1000
            outputs = transaction.commit(result, stage_code_revision)
            self.registry.set_stage_complete(stage_run_id, StageStatus.SUCCEEDED,
                                             artifacts=outputs, metrics=result.metrics,
                                             warnings=result.warnings, duration_ms=elapsed)
            logger.emit("stage_succeeded", stage=stage.name, stage_run_id=stage_run_id,
                        partition=partition, duration_ms=elapsed,
                        output_hashes={key: ref.content_hash for key, ref in sorted(outputs.items())},
                        metrics=dict(result.metrics), warnings=list(result.warnings),
                        peak_memory_bytes=self._peak_memory_bytes())
            return _JobResult(StageStatus.SUCCEEDED, outputs, stage_run_id)
        # A stage plugin may raise any ordinary exception; preserve its failure in the ledger.
        except Exception as exc:  # noqa: BLE001
            if transaction is not None:
                transaction.abort()
            elapsed = (time.perf_counter() - started) * 1000
            error = {"type": type(exc).__name__, "message": str(exc),
                     "traceback": traceback.format_exc()}
            cause = exc
            while cause is not None:
                code = getattr(cause, "code", None)
                if isinstance(code, str) and code:
                    error["code"] = code
                    break
                cause = cause.__cause__
            self.registry.set_stage_failed(stage_run_id, error=error, duration_ms=elapsed)
            logger.emit("stage_failed", stage=stage.name, stage_run_id=stage_run_id,
                        partition=partition, duration_ms=elapsed,
                        input_hashes={key: ref.content_hash for key, ref in sorted(inputs.items())},
                        error=error, peak_memory_bytes=self._peak_memory_bytes())
            return _JobResult(StageStatus.FAILED, {}, stage_run_id, error)

    @staticmethod
    def _peak_memory_bytes() -> int | None:
        try:
            import resource

            value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return int(value if os.uname().sysname == "Darwin" else value * 1024)
        except (ImportError, AttributeError, OSError):
            return None

    @staticmethod
    def _ancestors(stage_name: str, stages: Mapping[str, Stage]) -> set[str]:
        found: set[str] = set()
        pending = [stage_name]
        while pending:
            current = pending.pop()
            for dependency in stages[current].dependencies:
                if dependency not in found:
                    found.add(dependency)
                    pending.append(dependency)
        return found

    def _make_jobs(self, partitions: list[dict[str, str]]) -> tuple[list[_Job], dict[tuple[str, str], list[tuple[str, str]]]]:
        jobs: list[_Job] = []
        for stage_name in self.topological_order():
            stage = self.stages[stage_name]
            scopes = partitions if stage_is_partitioned(stage) else [{}]
            jobs.extend(_Job(stage_name, tuple(sorted(scope.items()))) for scope in scopes)
        jobs.sort(key=lambda job: (self.topological_order().index(job.stage_name), canonical_json(job.partition)))
        by_key = {job.key: job for job in jobs}
        dependencies: dict[tuple[str, str], list[tuple[str, str]]] = {}
        for job in jobs:
            dependency_jobs = []
            stage = self.stages[job.stage_name]
            for dep_name in sorted(stage.dependencies):
                dep = self.stages[dep_name]
                dep_scopes = partitions if stage_is_partitioned(dep) and not stage_is_partitioned(stage) else [
                    job.partition if stage_is_partitioned(dep) else {}
                ]
                for scope in dep_scopes:
                    dep_job = _Job(dep_name, tuple(sorted(scope.items())))
                    if dep_job.key not in by_key:
                        raise GraphError(f"no scheduled partition for dependency {dep_name!r}: {scope}")
                    dependency_jobs.append(dep_job.key)
            dependencies[job.key] = sorted(set(dependency_jobs))
        return jobs, dependencies

    def _input_map(self, job: _Job, dep_keys: list[tuple[str, str]],
                   results: Mapping[tuple[str, str], _JobResult]) -> dict[str, ArtifactRef]:
        grouped: dict[str, list[tuple[dict[str, str], str, ArtifactRef]]] = {}
        for dep_key in dep_keys:
            dep_name, _ = dep_key
            dep_job = next(j for j in self._active_jobs if j.key == dep_key)
            dep_result = results[dep_key]
            for alias, ref in sorted(dep_result.artifacts.items()):
                grouped.setdefault(dep_name, []).append((dep_job.partition, alias, ref))
        inputs: dict[str, ArtifactRef] = {}
        for dep_name, items in sorted(grouped.items()):
            partition_count = len({canonical_json(partition) for partition, _, _ in items})
            for partition, alias, ref in items:
                if partition_count == 1:
                    inputs[f"{dep_name}.{alias}"] = ref
                    if len(items) == 1:
                        inputs[dep_name] = ref
                else:
                    scope = canonical_json(partition)
                    inputs[f"{dep_name}.{alias}[{scope}]"] = ref
        external = getattr(self.stages[job.stage_name], "external_inputs", {})
        if not isinstance(external, Mapping) or any(
            not isinstance(key, str) or not isinstance(ref, ArtifactRef)
            for key, ref in external.items()
        ):
            raise GraphError("external_inputs must map aliases to ArtifactRef values")
        if set(external) & inputs.keys():
            raise GraphError("external input alias collides with a scheduled dependency")
        inputs.update(external)
        return inputs

    def run(self, *, config: Mapping[str, Any] | None = None, run_id: str | None = None,
            resume: str | None = None, from_stage: str | None = None,
            until_stage: str | None = None, only: Iterable[str] | None = None,
            force_stages: Iterable[str] | None = None,
            partitions: Iterable[Mapping[str, Any]] | None = None) -> RunSummary:
        if config is not None:
            self.config = dict(config)
            self.config_digest = config_hash(self.config)
            self.resource_budget = resource_budget(self.config)
        order = self.topological_order()
        if resume:
            prior = self.registry.get_run(resume)
            if prior is None:
                raise WorkflowError(f"cannot resume unknown run_id {resume!r}")
            stored_config = prior["config"]
            if config is not None and config_hash(stored_config) != self.config_digest:
                raise WorkflowError("resume config differs from the stored run config")
            self.config = stored_config
            self.config_digest = prior["config_hash"]
            self.resource_budget = resource_budget(self.config)
            if prior["graph"] != self.graph():
                raise WorkflowError("resume stage graph differs from the stored run graph")
            if prior["code_revision"] != self.code_revision:
                raise WorkflowError("resume code revision differs from the stored run code revision")
            execution = prior.get("execution", {})
            if from_stage is None and until_stage is None and only is None:
                from_stage = execution.get("from_stage")
                until_stage = execution.get("until_stage")
                only = execution.get("only")
            if partitions is None:
                partitions = execution.get("partitions", [{}])
            run_id = resume
        run_id = run_id or "run_" + uuid.uuid4().hex
        selected, needed = self.select_stages(from_stage=from_stage, until_stage=until_stage, only=only)
        scopes = []
        for scope in partitions or [{}]:
            normalized = {str(k): str(v) for k, v in scope.items()}
            if normalized not in scopes:
                scopes.append(normalized)
        if not scopes:
            scopes = [{}]
        execution = {"partitions": scopes, "from_stage": from_stage, "until_stage": until_stage,
                     "only": list(only or ())}
        snapshot_id = self._snapshot_id()
        split_id = self._split_id()
        as_of_cutoff = self._as_of_cutoff()
        if not resume:
            self.registry.create_run(run_id=run_id, config=self.config, config_hash=self.config_digest,
                                     data_snapshot_id=snapshot_id, split_id=split_id,
                                     as_of_cutoff=as_of_cutoff, code_revision=self.code_revision,
                                     graph=self.graph(), execution=execution)
        elif self.registry.get_run(run_id) is None:
            raise WorkflowError(f"cannot resume unknown run_id {run_id!r}")
        logger = self.logger_factory or StructuredLogger(
            self.artifact_store.root / "logs" / f"{run_id}.jsonl", run_id
        )
        if self.logger_factory is not None:
            logger = self.logger_factory
        jobs, dependencies = self._make_jobs(scopes)
        self._active_jobs = jobs
        # Publish the complete pending inventory before any worker is dispatched.
        # Reconciliation on resume preserves existing attempts and committed outputs.
        for job in jobs:
            stage = self.stages[job.stage_name]
            self.registry.ensure_stage_run(
                run_id=run_id, stage_name=stage.name, stage_version=stage.version,
                partition=job.partition, stage_run_id=new_stage_run_id(), cache_key=None,
                config_hash=self.config_digest, code_revision=self.code_revision,
            )
        forced = set(force_stages or ())
        unknown_force = forced - self.stages.keys()
        if unknown_force:
            raise GraphError("unknown --force-stage name: " + ", ".join(sorted(unknown_force)))
        force_closure = set(forced)
        for forced_name in forced:
            force_closure.update(name for name in self.stages
                                 if forced_name in self._ancestors(name, self.stages))
        if force_closure:
            self.registry.invalidate_cache(force_closure)
            logger.emit("cache_invalidated", stage_names=sorted(force_closure), run_id=run_id)
        results: dict[tuple[str, str], _JobResult] = {}
        pending = {job.key for job in jobs}
        in_flight: dict[Future[_JobResult], tuple[str, str]] = {}
        max_workers = self.resource_budget.max_concurrent_stages
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="quantsage-stage") as pool:
            while pending or in_flight:
                progressed = False
                ready = sorted(
                    (key for key in pending if all(dep in results for dep in dependencies[key])),
                    key=lambda key: (order.index(key[0]), key[1]),
                )
                for key in ready:
                    job = next(job for job in jobs if job.key == key)
                    stage = self.stages[job.stage_name]
                    dep_results = [results[dep] for dep in dependencies[key]]
                    if job.stage_name not in needed:
                        row = self.registry.ensure_stage_run(
                            run_id=run_id, stage_name=stage.name, stage_version=stage.version,
                            partition=job.partition, stage_run_id=new_stage_run_id(), cache_key=None,
                            config_hash=self.config_digest, code_revision=self.code_revision,
                        )
                        if row["status"] == StageStatus.PENDING.value:
                            self.registry.set_stage_status(row["stage_run_id"], StageStatus.SKIPPED,
                                                           error={"type": "Selection", "message": "outside requested stage range"})
                        results[key] = _JobResult(StageStatus.SKIPPED, {}, row["stage_run_id"])
                        pending.remove(key)
                        progressed = True
                        continue
                    if any(result.status in {StageStatus.FAILED, StageStatus.BLOCKED} for result in dep_results):
                        existing = self.registry.get_stage_run(run_id, stage.name, job.partition)
                        row = self.registry.ensure_stage_run(
                            run_id=run_id, stage_name=stage.name, stage_version=stage.version,
                            partition=job.partition, stage_run_id=new_stage_run_id(), cache_key=None,
                            config_hash=self.config_digest, code_revision=self.code_revision,
                            force_new=bool(existing and existing["status"] in {
                                StageStatus.SUCCEEDED.value, StageStatus.CACHED.value
                            }),
                        )
                        error = {"type": "DependencyFailed", "message": "one or more dependencies did not succeed"}
                        if row["status"] != StageStatus.BLOCKED.value:
                            self.registry.set_stage_status(row["stage_run_id"], StageStatus.BLOCKED, error=error)
                        results[key] = _JobResult(StageStatus.BLOCKED, {}, row["stage_run_id"], error)
                        pending.remove(key)
                        progressed = True
                        continue
                    if len(in_flight) >= max_workers:
                        break
                    inputs = self._input_map(job, dependencies[key], results)
                    allow_execute = job.stage_name in selected
                    future = pool.submit(
                        self._execute_one, run_id=run_id, job=job, stage=stage,
                        inputs=inputs, logger=logger, force=job.stage_name in force_closure,
                        allow_execute=allow_execute,
                    )
                    in_flight[future] = key
                    pending.remove(key)
                    progressed = True
                if in_flight and (len(in_flight) >= max_workers or not progressed or not pending):
                    done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                    for future in sorted(done, key=lambda item: in_flight[item]):
                        key = in_flight.pop(future)
                        results[key] = future.result()
                elif not progressed and pending:
                    unresolved = sorted(pending)
                    raise GraphError(f"scheduler deadlock: {unresolved}")
        failures = [result for result in results.values()
                    if result.status in {StageStatus.FAILED, StageStatus.BLOCKED}]
        status = "FAILED" if failures else "SUCCEEDED"
        self.registry.finish_run(run_id, status)
        stage_summary = {}
        for key in sorted(results, key=lambda k: (order.index(k[0]), k[1])):
            result = results[key]
            stage_summary.setdefault(key[0], []).append({
                "partition": json.loads(key[1]),
                "status": result.status.value,
                "stage_run_id": result.stage_run_id,
                "artifacts": {alias: ref.to_dict() for alias, ref in sorted(result.artifacts.items())},
                "error": result.error,
            })
        logger.emit("run_finished", status=status, stage_count=len(results))
        return RunSummary(run_id, status, stage_summary,
                          "No stages registered; no work was executed." if not self.stages else None)
