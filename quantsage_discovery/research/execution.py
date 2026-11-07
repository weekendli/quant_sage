"""Execution, publication receipts, cache verification, and recovery for research Runs."""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import uuid
from collections import Counter
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from quantsage_discovery.core.artifacts import (
    ArtifactStore,
    artifact_path,
    durable_write_json,
    hash_path,
)
from quantsage_discovery.core.errors import ArtifactError, ArtifactValidationError, WorkflowError
from quantsage_discovery.core.registry import Registry
from quantsage_discovery.core.types import (
    ArtifactRef,
    ResourceBudget,
    RunContext,
    StageResult,
    StageStatus,
)
from quantsage_discovery.core.utils import canonical_json

from .catalog import FeatureCatalog
from .errors import (
    PlanChangedError,
    ResearchValidationError,
    SourceScopeMissingError,
    StudyNotActiveError,
    UnknownReferenceError,
)
from .execution_view import active_linked_result
from .planner import RunPlanner
from .providers import (
    FEATURE_VALUES_ARTIFACT_TYPE,
    FEATURE_VALUES_SCHEMA_VERSION,
    normalize_feature_values_table,
    transform_values,
    validate_feature_result_payload,
    validate_feature_values_parquet,
)
from .repository import ResearchRepository
from .schemas import parse_versioned_ref

_SAFE_PATH_PART = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_DURATION_NS = re.compile(r"^(\d+)ns$")
_RECEIPT_SCHEMA = "publication_receipt_v1"


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


def _hash_json(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _safe_component(value: Any, name: str) -> str:
    if not isinstance(value, str) or not _SAFE_PATH_PART.fullmatch(value):
        raise ResearchValidationError(f"{name} must be a safe path component")
    return value


def _as_utc(value: Any, name: str) -> datetime:
    if not isinstance(value, str):
        raise ResearchValidationError(f"{name} must be a timezone-aware ISO timestamp")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ResearchValidationError(f"{name} must include a timezone")
    return parsed.astimezone(UTC)


def _duration(value: Any, name: str) -> timedelta:
    if not isinstance(value, str):
        raise ResearchValidationError(f"{name} must be a normalized duration")
    match = _DURATION_NS.fullmatch(value)
    if match is None:
        raise ResearchValidationError(f"{name} must be normalized as integer nanoseconds")
    return timedelta(microseconds=(int(match.group(1)) + 999) // 1000)


def _same_json(left: Any, right: Any) -> bool:
    return canonical_json(left) == canonical_json(right)


class ResearchExecutor:
    """Run a frozen research work graph through trusted synthetic providers."""

    def __init__(self, registry: Registry, artifact_store: ArtifactStore,
                 catalog: FeatureCatalog, *, fault_hook=None):
        self._registry = registry
        self._store = artifact_store
        self._catalog = catalog
        self._repository = ResearchRepository(registry)
        self._planner = RunPlanner(registry, self._repository, catalog)
        if fault_hook is not None and not callable(fault_hook):
            raise TypeError("fault_hook must be callable or None")
        self._fault_hook = fault_hook
        self._store.register_validator(
            FEATURE_VALUES_ARTIFACT_TYPE,
            FEATURE_VALUES_SCHEMA_VERSION,
            validate_feature_values_parquet,
        )

    def find_verified_result(self, run_context_dict: dict[str, Any], work: dict[str, Any],
                             *, record_health: bool = False) -> dict[str, Any] | None:
        """Return an exact cached result only after recursively verifying its lineage and bytes."""
        context = self._normalize_run_context(run_context_dict)
        candidates: list[dict[str, Any]] = []
        preferred = work.get("candidate_result_ref")
        if isinstance(preferred, str):
            result = self._registry.get_feature_result(preferred)
            if result is not None:
                candidates.append(result)
        try:
            candidates.extend(self._registry.find_feature_result_candidates(
                feature_ref=work["feature_ref"],
                definition_hash=work["definition_hash"],
                source_ref=work["source_ref"],
                split_id=context["plan"]["split_id"],
                scope=work["partition_scope"],
                required_history=work["required_history"],
            ))
        except (KeyError, TypeError) as exc:
            raise ResearchValidationError(f"malformed frozen work identity: {exc}") from exc

        seen_ids: set[str] = set()
        for candidate in candidates:
            if candidate is None or candidate.get("result_id") in seen_ids:
                continue
            seen_ids.add(candidate.get("result_id"))
            try:
                self._verify_stored_result(candidate, set())
                self._validate_result_for_work(candidate, context, work, set())
                if record_health:
                    self._registry.append_feature_result_health(
                        result_id=candidate["result_id"], status="available",
                        details={"source": "executor_cache_verification"},
                    )
                return candidate
            except (ArtifactError, ArtifactValidationError, WorkflowError,
                    ResearchValidationError, UnknownReferenceError, OSError,
                    ValueError, KeyError, TypeError) as exc:
                if record_health and candidate.get("result_id"):
                    self._record_result_health(candidate, str(exc))
        return None

    def _normalize_run_context(self, context: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(context, dict):
            raise ResearchValidationError("run context must be an object")
        plan = context.get("plan")
        request = context.get("request")
        if not isinstance(plan, dict) or not isinstance(request, dict):
            raise ResearchValidationError("run context requires frozen plan and request objects")
        if not isinstance(context.get("run_id"), str):
            raise ResearchValidationError("run context requires run_id")
        return context

    def _run_context(self, run_id: str) -> dict[str, Any]:
        stored = self._registry.get_run(run_id)
        if stored is None or not stored.get("research_study_id"):
            raise UnknownReferenceError(f"unknown research Run: {run_id}", details={"run_id": run_id})
        plan = stored.get("research_plan")
        request = stored.get("research_request")
        environment = stored.get("environment")
        if not isinstance(plan, dict) or not isinstance(request, dict):
            raise WorkflowError("registered research Run has no frozen plan/request")
        if "runtime_environment" not in plan and isinstance(environment, dict):
            plan = {**plan, "runtime_environment": environment}
        return {"run_id": run_id, "request": request, "plan": plan,
                "environment": environment, "code_revision": stored.get("code_revision")}

    def _computation_fingerprint(self, context: dict[str, Any], work: dict[str, Any],
                                 input_result_refs: list[str]) -> str:
        feature = self._catalog.get_feature(work["feature_ref"])
        plan = context["plan"]
        runtime = plan.get("runtime_environment") or context.get("environment") or {}
        dependency_identity = []
        for result_id in input_result_refs:
            result = self._registry.get_feature_result(result_id)
            if result is None:
                raise WorkflowError(f"input FeatureResult is unavailable: {result_id}")
            dependency_identity.append({
                "result_id": result_id,
                "artifact_hashes": sorted(
                    descriptor.get("hash") or descriptor.get("artifact_ref", {}).get("content_hash")
                    for descriptor in result.get("artifacts", [])
                ),
            })
        identity = {
            "schema_version": "feature_computation_identity_v1",
            "feature_ref": work["feature_ref"],
            "definition_hash": work["definition_hash"],
            "source_ref": work["source_ref"],
            "source_hash": plan.get("source_hashes", {}).get(work["source_ref"]),
            "split_ref": {
                **context["request"]["split_ref"],
                "split_id": plan["split_id"],
            },
            "partition_scope": work["partition_scope"],
            "required_history": work["required_history"],
            "input_result_refs": dependency_identity,
            "implementation": feature.get("implementation", {}),
            "runtime": {
                "source_tree_hash": runtime.get("source_tree_hash"),
                "python_version": runtime.get("python_version"),
                "python_implementation": runtime.get("python_implementation"),
                "dependencies": runtime.get("dependencies", {}),
            },
            "seed": context["request"].get("seed"),
        }
        return _hash_json(identity)

    def _validate_result_for_work(self, result: dict[str, Any], context: dict[str, Any],
                                  work: dict[str, Any], seen: set[str]) -> list[ArtifactRef]:
        result_id = result.get("result_id")
        if not isinstance(result_id, str) or result_id in seen:
            raise WorkflowError("FeatureResult lineage contains a cycle or invalid result id")
        seen = seen | {result_id}
        identity_fields = {
            "feature_ref": work["feature_ref"],
            "definition_hash": work["definition_hash"],
            "source_ref": work["source_ref"],
            "scope": work["partition_scope"],
            "required_history": work["required_history"],
        }
        for field, expected in identity_fields.items():
            if not _same_json(result.get(field), expected):
                raise WorkflowError(f"FeatureResult {result_id} has mismatched {field}")
        if not _same_json(result.get("processed_scope"), work["partition_scope"]):
            raise WorkflowError(f"FeatureResult {result_id} processed_scope does not match work")
        plan = context["plan"]
        expected_split_ref = {
            **context["request"]["split_ref"],
            "split_id": plan["split_id"],
        }
        if not _same_json(result.get("split_ref"), expected_split_ref):
            raise WorkflowError(f"FeatureResult {result_id} has mismatched split ref")
        if result.get("source_refs") != [work["source_ref"]]:
            raise WorkflowError(f"FeatureResult {result_id} has mismatched source refs")
        if result.get("producer_work_id") != result.get("producer_work_key"):
            raise WorkflowError(f"FeatureResult {result_id} has mismatched producer work id")
        quality = result.get("quality")
        if not isinstance(quality, dict):
            raise WorkflowError(f"FeatureResult {result_id} is missing quality counts")
        n_total, n_valid = quality.get("n_total"), quality.get("n_valid")
        if (type(n_total) is not int or type(n_valid) is not int or n_total < 0
                or n_valid < 0 or n_valid > n_total):
            raise WorkflowError(f"FeatureResult {result_id} has invalid row counts")
        expected_fingerprint = self._computation_fingerprint(
            context, work, list(result.get("input_result_refs", []))
        )
        if result.get("computation_fingerprint") != expected_fingerprint:
            raise WorkflowError(f"FeatureResult {result_id} has a different computation fingerprint")

        feature = self._catalog.get_feature(result["feature_ref"])
        input_ids = result.get("input_result_refs")
        if not isinstance(input_ids, list) or any(not isinstance(item, str) for item in input_ids):
            raise WorkflowError(f"FeatureResult {result_id} input_result_refs must be string IDs")
        input_results = []
        for input_id in input_ids:
            input_result = self._registry.get_feature_result(input_id)
            if input_result is None:
                raise WorkflowError(f"FeatureResult {result_id} input is missing: {input_id}")
            input_results.append(input_result)
        expected_inputs = work.get("input_result_refs")
        if isinstance(expected_inputs, list) and expected_inputs and input_ids != expected_inputs:
            raise WorkflowError(f"FeatureResult {result_id} input refs differ from frozen cache bindings")
        actual_features = [item.get("feature_ref") for item in input_results]
        if actual_features != feature.get("input_feature_refs", []):
            raise WorkflowError(f"FeatureResult {result_id} input feature lineage does not match")

        artifacts = self._verify_result_artifacts(result)
        if sum(ref.row_count or 0 for ref in artifacts) != n_total:
            raise WorkflowError(f"FeatureResult {result_id} row count does not match artifacts")
        for input_result in input_results:
            self._verify_stored_result(input_result, seen)
        return artifacts

    def _verify_stored_result(self, result: dict[str, Any], seen: set[str]) -> None:
        producer_run_id = result.get("producer_run_id")
        work_key = result.get("producer_work_key")
        if not isinstance(producer_run_id, str) or not isinstance(work_key, str):
            raise WorkflowError("stored FeatureResult has no producer identity")
        context = self._run_context(producer_run_id)
        plan_work = {item["work_key"]: item for item in context["plan"].get("work", [])}
        work = plan_work.get(work_key)
        if work is None:
            raise WorkflowError(f"FeatureResult producer work is absent from its frozen plan: {work_key}")
        stage = self._registry.get_run_stages(producer_run_id)
        stage_row = next((row for row in stage if row.get("work_key") == work_key), None)
        if stage_row is None or stage_row.get("stage_run_id") != result.get("producer_stage_run_id"):
            raise WorkflowError("FeatureResult producer stage identity does not match the ledger")
        self._validate_result_for_work(result, context, work, seen)

    def _verify_result_artifacts(self, result: dict[str, Any]) -> list[ArtifactRef]:
        descriptors = result.get("artifacts")
        if not isinstance(descriptors, list) or not descriptors:
            raise WorkflowError("FeatureResult artifacts must be a non-empty array")
        refs = []
        for descriptor in descriptors:
            if not isinstance(descriptor, dict) or not isinstance(descriptor.get("artifact_ref"), dict):
                raise WorkflowError("FeatureResult artifact descriptor is malformed")
            ref = ArtifactRef.from_dict(descriptor["artifact_ref"])
            if descriptor.get("hash") != ref.content_hash:
                raise WorkflowError("FeatureResult artifact descriptor hash does not match its ref")
            if (ref.artifact_type != FEATURE_VALUES_ARTIFACT_TYPE
                    or ref.schema_version != FEATURE_VALUES_SCHEMA_VERSION):
                raise WorkflowError("FeatureResult artifact has an unsupported schema")
            expected = {
                "run_id": result["producer_run_id"],
                "stage_run_id": result.get("producer_stage_run_id"),
                "stage_name": result.get("producer_stage_name"),
                "stage_version": result.get("producer_stage_version"),
                "cache_key": result["computation_fingerprint"],
                "split_id": result["split_ref"]["split_id"],
            }
            self._store.verify(ref, expected=expected)
            if ref.row_count != result["quality"]["n_total"]:
                raise WorkflowError("FeatureResult quality count does not match artifact row count")
            refs.append(ref)
        validate_feature_result_payload(refs, result)
        return refs

    def _record_result_health(self, result: dict[str, Any], reason: str) -> None:
        result_id = result.get("result_id")
        if not isinstance(result_id, str):
            return
        missing = False
        for descriptor in result.get("artifacts", []):
            artifact_ref = descriptor.get("artifact_ref", {}) if isinstance(descriptor, dict) else {}
            uri = artifact_ref.get("uri")
            if isinstance(uri, str):
                try:
                    if not artifact_path(uri).exists():
                        missing = True
                        break
                except ArtifactError:
                    pass
        try:
            self._registry.append_feature_result_health(
                result_id=result_id,
                status="missing" if missing else "invalid",
                reason=reason,
                details={"source": "executor_cache_verification"},
            )
        except WorkflowError:
            # A receipt not yet committed has no FeatureResult row to annotate.
            return

    @contextmanager
    def _run_lock(self, run_id: str):
        component = _safe_component(run_id, "run_id")
        lock_root = self._store.root / ".execution-locks"
        lock_root.mkdir(parents=True, exist_ok=True)
        lock_path = lock_root / f"{component}.lock"
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_CLOEXEC, 0o600)
        os.fchmod(descriptor, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def execute_run(self, run_id: str) -> dict[str, Any]:
        """Execute the registered plan; submitting/planning remain explicit separate calls."""
        with self._run_lock(run_id):
            context = self._run_context(run_id)
            self._authorize_run(context)
            self._recover_receipts(context)
            self._reconcile_running_work(run_id)
            context = self._run_context(run_id)
            self._execute_frozen_graph(context)
            return self._planner.get_run(run_id)

    def resume_run(self, run_id: str) -> dict[str, Any]:
        """Resume the original frozen work graph after recovering durable publications."""
        return self.execute_run(run_id)

    def recover(self, run_id: str | None = None) -> dict[str, Any]:
        """Repair receipt/index mismatches without dispatching unfinished numerical work."""
        run_ids = [run_id] if run_id is not None else [
            run["run_id"] for run in self._registry.list_research_runs()
        ]
        recovered = []
        for candidate_run_id in run_ids:
            with self._run_lock(candidate_run_id):
                context = self._run_context(candidate_run_id)
                self._recover_receipts(context)
                self._reconcile_running_work(candidate_run_id)
                self._update_run_summary(candidate_run_id)
                recovered.append(self._planner.get_run(candidate_run_id))
        if run_id is not None:
            return recovered[0]
        receipt_root = self._store.root / ".receipts" / "research"
        if receipt_root.exists():
            known_runs = set(run_ids)
            for path in sorted(receipt_root.rglob("*.json")):
                if path.name.endswith(".reason.json"):
                    continue
                relative = path.relative_to(receipt_root)
                if not relative.parts or relative.parts[0] not in known_runs:
                    self._quarantine_receipt(
                        path, "receipt belongs to an unregistered Run", None
                    )
        return {"schema_version": "run_recovery_v1", "runs": recovered}

    def _execute_frozen_graph(self, context: dict[str, Any]) -> None:
        run_id = context["run_id"]
        plan = context["plan"]
        work_items = self._planner.get_run(run_id)["item_executions"]
        if not work_items:
            status = "NO_WORK" if plan.get("status") == "NO_WORK" else "BLOCKED"
            self._registry.update_research_run_summary(
                run_id, status=status,
                summary={**plan.get("summary", {}), "status": status, "state_counts": {}},
            )
            return
        self._reject_unenforceable_budget(context, work_items)

        work_by_key = {work["work_key"]: work for work in work_items}
        completed: dict[str, dict[str, Any] | None] = {}
        visiting: set[str] = set()
        requested = sorted(
            work["work_key"] for work in work_items if work.get("role") == "requested"
        )
        for work_key in requested:
            self._execute_work(context, work_by_key[work_key], work_by_key, completed, visiting)

        # Dependency-only tasks made unnecessary by a verified parent cache are recorded
        # explicitly, with no attempt or numerical output.
        for work in sorted(work_items, key=lambda row: row["work_key"]):
            work_key = work["work_key"]
            if work_key in completed or work.get("role") == "requested":
                continue
            stage = self._stage_for_work(run_id, work)
            if stage["status"] in {StageStatus.PENDING.value, StageStatus.BLOCKED.value,
                                   StageStatus.FAILED.value}:
                self._registry.set_stage_status(
                    stage["stage_run_id"], StageStatus.SKIPPED,
                    error={"code": "CACHED_PARENT_SATISFIES_DEPENDENCY",
                           "message": "A verified requested result made this dependency unnecessary"},
                )
        self._update_run_summary(run_id)

    def _reject_unenforceable_budget(self, context: dict[str, Any],
                                     work_items: list[dict[str, Any]]) -> None:
        budget = context["plan"].get("budget", {})
        unsupported = sorted(set(budget) & {"max_cpu_seconds", "max_memory_bytes"})
        if unsupported and any(
            not work.get("cache_only") and not work.get("blocked_reason")
            for work in work_items
        ):
            raise ResearchValidationError(
                "the local synthetic executor cannot enforce these hard resource limits",
                details={"unforceable_limits": unsupported,
                         "executor": "single_owner_synchronous_v1"},
            )

    def _stage_for_work(self, run_id: str, work: dict[str, Any]) -> dict[str, Any]:
        stage_run_id = work.get("stage_run_id")
        stage = (self._registry.get_stage_run_by_id(stage_run_id)
                 if isinstance(stage_run_id, str) else None)
        if stage is None or stage.get("run_id") != run_id or stage.get("work_key") != work["work_key"]:
            raise WorkflowError(f"Run work row is missing from the Registry: {work['work_key']}")
        return stage

    def _execute_work(self, context: dict[str, Any], work: dict[str, Any],
                      work_by_key: dict[str, dict[str, Any]],
                      completed: dict[str, dict[str, Any] | None],
                      visiting: set[str]) -> dict[str, Any] | None:
        work_key = work["work_key"]
        if work_key in completed:
            return completed[work_key]
        if work_key in visiting:
            raise WorkflowError(f"frozen work graph contains a dependency cycle at {work_key}")
        visiting.add(work_key)
        run_id = context["run_id"]
        stage = self._stage_for_work(run_id, work)

        # A committed result in this Run is authoritative if it still verifies.
        active = self._latest_linked_result(run_id, work_key)
        if active is not None and stage["status"] in {
            StageStatus.SUCCEEDED.value, StageStatus.CACHED.value,
        }:
            try:
                self._validate_result_for_work(active, context, work, set())
                completed[work_key] = active
                visiting.remove(work_key)
                return active
            except (ArtifactError, ArtifactValidationError, WorkflowError,
                    ResearchValidationError, UnknownReferenceError, OSError,
                    ValueError, KeyError, TypeError) as exc:
                self._record_result_health(active, str(exc))
                self._registry.set_research_work_pending(
                    stage["stage_run_id"], reason=f"Committed result failed health verification: {exc}"
                )
                stage = self._stage_for_work(run_id, work)

        candidate = self.find_verified_result(context, work, record_health=True)
        if candidate is not None:
            refs = self._result_artifact_map(candidate)
            self._registry.link_cached_feature_result(
                run_id=run_id,
                work_key=work_key,
                stage_run_id=stage["stage_run_id"],
                result=candidate,
                artifacts=refs,
                metrics=self._quality_metrics(candidate["quality"]),
                cache_source_run_id=candidate["producer_run_id"],
            )
            completed[work_key] = candidate
            visiting.remove(work_key)
            return candidate

        blocked_reason = work.get("blocked_reason") or stage.get("blocked_reason")
        if blocked_reason or work.get("cache_only"):
            reason = blocked_reason or "CACHED_RESULT_INVALID_OR_MISSING"
            self._mark_blocked(stage, reason)
            completed[work_key] = None
            visiting.remove(work_key)
            return None

        dependency_results: dict[str, dict[str, Any]] = {}
        for dependency_key in sorted(work.get("dependency_work_keys", [])):
            dependency_work = work_by_key.get(dependency_key)
            if dependency_work is None:
                self._mark_blocked(stage, f"DEPENDENCY_NOT_IN_FROZEN_PLAN:{dependency_key}")
                completed[work_key] = None
                visiting.remove(work_key)
                return None
            result = self._execute_work(context, dependency_work, work_by_key, completed, visiting)
            if result is None:
                self._mark_blocked(stage, f"DEPENDENCY_BLOCKED:{dependency_key}")
                completed[work_key] = None
                visiting.remove(work_key)
                return None
            dependency_results[dependency_key] = result

        feature = self._catalog.get_feature(work["feature_ref"])
        expected_input_refs = feature.get("input_feature_refs", [])
        results_by_feature: dict[str, list[dict[str, Any]]] = {}
        for result in dependency_results.values():
            results_by_feature.setdefault(result["feature_ref"], []).append(result)
        if expected_input_refs and set(results_by_feature) != set(expected_input_refs):
            self._mark_blocked(stage, "DEPENDENCY_RESULT_SET_MISMATCH")
            completed[work_key] = None
            visiting.remove(work_key)
            return None
        ordered_inputs = []
        for feature_ref in expected_input_refs:
            candidates = results_by_feature.get(feature_ref, [])
            if len(candidates) != 1:
                self._mark_blocked(stage, f"DEPENDENCY_RESULT_AMBIGUOUS:{feature_ref}")
                completed[work_key] = None
                visiting.remove(work_key)
                return None
            ordered_inputs.append(candidates[0])

        input_result_refs = [result["result_id"] for result in ordered_inputs]
        fingerprint = self._computation_fingerprint(context, work, input_result_refs)
        input_artifacts: dict[str, ArtifactRef] = {}
        input_tables: list[pa.Table] = []
        for index, result in enumerate(ordered_inputs):
            refs = self._result_artifact_map(result)
            if len(refs) != 1:
                raise WorkflowError("synthetic provider requires one verified value artifact per input")
            ref = next(iter(refs.values()))
            input_artifacts[f"input_{index}"] = ref
            table = pq.read_table(artifact_path(ref.uri))
            dependency_scope = {
                **work["partition_scope"],
                **(work.get("read_scope") or {}),
            }
            input_tables.append(self._filter_feature_table(table, dependency_scope))

        source = self._source(work["source_ref"])
        if input_tables:
            if len(input_tables) > 1:
                tables = [table for table in input_tables if table.num_rows]
                raw_inputs = pa.concat_tables(tables) if tables else input_tables[0].slice(0, 0)
            else:
                raw_inputs = input_tables[0]
        else:
            raw_inputs = self._read_source_values(source, work)

        claimed = self._registry.claim_research_work(
            stage["stage_run_id"],
            computation_fingerprint=fingerprint,
            config_hash=context["plan"]["config_hash"],
            code_revision=context.get("code_revision") or context["plan"].get("code_revision", ""),
            inputs=input_artifacts,
        )
        if not claimed["claimed"]:
            # An owner-side concurrent claimant won. Re-read status/results instead of duplicating work.
            current = self._stage_for_work(run_id, work)
            result = self._latest_linked_result(run_id, work_key)
            if result is not None and current["status"] in {
                StageStatus.SUCCEEDED.value, StageStatus.CACHED.value,
            }:
                self._validate_result_for_work(result, context, work, set())
                completed[work_key] = result
                visiting.remove(work_key)
                return result
            completed[work_key] = None
            visiting.remove(work_key)
            return None

        attempt = claimed["attempt"]
        stage_run_id = stage["stage_run_id"]
        stage_name = f"research_step_{work['step']}"
        stage_version = work.get("implementation_version") or str(work["step"])
        publication_id = f"{stage_run_id}-attempt-{attempt}"
        partition = work["partition_scope"]
        source_hash = context["plan"].get("source_hashes", {}).get(work["source_ref"])
        seed = context["request"].get("seed")
        rng_seed = seed if type(seed) is int else 0
        run_context = RunContext(
            run_id=run_id,
            config_hash=context["plan"]["config_hash"],
            split_id=context["plan"]["split_id"],
            data_snapshot_id=source_hash,
            as_of_cutoff=None,
            artifact_store=self._store,
            artifact_transaction=None,
            rng_seed=rng_seed,
            logger=None,
            resource_budget=ResourceBudget(max_concurrent_stages=1),
            config=context["plan"].get("config_snapshot", {}),
            stage_config={"work_key": work_key, "feature_ref": work["feature_ref"]},
            stage_run_id=stage_run_id,
            stage_name=stage_name,
            partition_keys={key: str(value) for key, value in partition.items()
                            if key in {"exchange", "instrument_id", "trade_date", "session_id"}},
            code_revision=context.get("code_revision") or context["plan"].get("code_revision", ""),
        )
        transaction = self._store.begin(
            run_context, stage_name, stage_version, fingerprint, input_artifacts,
            publication_id=publication_id,
        )
        try:
            implementation = feature.get("implementation", {})
            provider_key = implementation.get("metric_provider_key") or implementation.get("provider_key")
            transformed, provider_reasons = transform_values(provider_key, feature, raw_inputs)
            output = self._filter_feature_table(transformed, partition)
            n_total = output.num_rows
            values = output["value"].to_pylist()
            n_valid = sum(value is not None and math.isfinite(value) for value in values)
            invalid_counts = {"NA_VALUE": n_total - n_valid} if n_total > n_valid else {}
            quality_flags = []
            if n_total == 0:
                quality_flags.append("ZERO_OBSERVATIONS")
            elif n_valid == 0:
                quality_flags.append("ALL_NA")
            metrics = {
                "n_total": n_total,
                "n_valid": n_valid,
                "processed_scope": "complete",
                "invalid_reason_counts": invalid_counts,
            }
            time_min, time_max = self._time_bounds(output)
            output_path = transaction.reserve_output(
                "result", artifact_type=FEATURE_VALUES_ARTIFACT_TYPE,
                schema_version=FEATURE_VALUES_SCHEMA_VERSION,
                partition_keys=partition, row_count=n_total,
                time_min=time_min, time_max=time_max,
                quality_flags=tuple(quality_flags),
            )
            pq.write_table(output, output_path, compression="zstd", version="2.6")
            pending_ref = transaction.register_output("result")
            stage_result = StageResult(
                artifacts={"result": pending_ref},
                metrics=metrics,
                warnings=tuple(sorted(provider_reasons)),
            )
            prepared = transaction.prepare(stage_result, run_context.code_revision)
            result_ref = prepared["result"]
            result_doc = self._make_feature_result(
                context=context,
                work=work,
                feature=feature,
                result_ref=result_ref,
                fingerprint=fingerprint,
                input_result_refs=input_result_refs,
                attempt=attempt,
                stage_run_id=stage_run_id,
                stage_name=stage_name,
                stage_version=stage_version,
                source=source,
                n_total=n_total,
                n_valid=n_valid,
                invalid_reason_counts=invalid_counts,
                quality_flags=quality_flags,
                output=output,
            )
            receipt = self._make_receipt(
                context, work, result_doc, result_ref, attempt, stage_run_id,
                transaction.final_root,
            )
            receipt_path = self._receipt_path(run_id, work_key, attempt)
            durable_write_json(receipt_path, receipt)
            fault_context = {
                "run_id": run_id,
                "work_key": work_key,
                "stage_run_id": stage_run_id,
                "attempt": attempt,
                "attempt_id": receipt["attempt_id"],
                "receipt_id": receipt["receipt_id"],
                "receipt_path": str(receipt_path),
                "final_bundle_uri": receipt["final_bundle_uri"],
            }
            self._inject_fault("before_publish", fault_context)
            published = transaction.publish(preserve_invalid=True)
            self._inject_fault("after_publish_before_db_commit", fault_context)
            self._inject_fault("before_db_commit", fault_context)
            result_doc["artifacts"][0]["artifact_ref"] = published["result"].to_dict()
            result_doc["artifacts"][0]["hash"] = published["result"].content_hash
            self._validate_result_for_work(result_doc, context, work, set())
            self._registry.commit_research_result(
                run_id=run_id, work_key=work_key, stage_run_id=stage_run_id,
                result=result_doc, artifacts=published, metrics=metrics,
                receipt_id=receipt["receipt_id"], attempt=attempt,
            )
            self._inject_fault("after_db_commit", fault_context)
            completed[work_key] = result_doc
            visiting.remove(work_key)
            return result_doc
        except Exception as exc:  # noqa: BLE001 - provider hooks may raise arbitrary ordinary failures
            current = self._stage_for_work(run_id, work)
            if current["status"] in {
                StageStatus.SUCCEEDED.value, StageStatus.CACHED.value,
            }:
                committed = self._latest_linked_result(run_id, work_key)
                if committed is not None:
                    completed[work_key] = committed
                    visiting.remove(work_key)
                    return committed
            if not transaction._committed:
                transaction.abort()
            if current["status"] == StageStatus.RUNNING.value:
                self._registry.set_stage_failed(
                    current["stage_run_id"],
                    error={"code": "WORK_FAILED", "type": type(exc).__name__,
                           "message": str(exc)},
                    duration_ms=None,
                )
            visiting.remove(work_key)
            completed[work_key] = None
            return None

    def _latest_linked_result(self, run_id: str, work_key: str) -> dict[str, Any] | None:
        links = [link for link in self._registry.list_run_result_links(run_id)
                 if link["work_key"] == work_key]
        stage = next((row for row in self._registry.get_run_stages(run_id)
                      if row.get("work_key") == work_key), None)
        events = self._registry.get_stage_events(stage["stage_run_id"]) if stage else []
        return active_linked_result(links, events)

    @staticmethod
    def _result_artifact_map(result: dict[str, Any]) -> dict[str, ArtifactRef]:
        descriptors = result.get("artifacts", [])
        if not isinstance(descriptors, list):
            raise WorkflowError("FeatureResult artifacts must be an array")
        refs = {}
        for index, descriptor in enumerate(descriptors):
            if not isinstance(descriptor, dict) or not isinstance(descriptor.get("artifact_ref"), dict):
                raise WorkflowError("FeatureResult artifact descriptor is malformed")
            alias = descriptor.get("artifact_role", "result" if index == 0 else f"result_{index}")
            refs[alias] = ArtifactRef.from_dict(descriptor["artifact_ref"])
        if not refs:
            raise WorkflowError("FeatureResult has no artifact refs")
        return refs

    @staticmethod
    def _quality_metrics(quality: dict[str, Any]) -> dict[str, Any]:
        return {
            "n_total": quality["n_total"],
            "n_valid": quality["n_valid"],
            "processed_scope": "complete",
            "invalid_reason_counts": quality.get("invalid_reason_counts", {}),
        }

    def _source(self, source_ref: str) -> dict[str, Any]:
        source_id, version = parse_versioned_ref(source_ref, "work.source_ref")
        source = self._repository.get_source(source_id, version)
        if source is None:
            raise UnknownReferenceError(f"unknown DataSource ref: {source_ref}")
        return source

    def _read_source_values(self, source: dict[str, Any], work: dict[str, Any]) -> pa.Table:
        if str(source.get("format", "")).lower() not in {"parquet", "parquet_v1"}:
            raise ArtifactValidationError("the synthetic executor accepts Parquet source artifacts only")
        manifest_ref = source["manifest_ref"]
        manifest_path = artifact_path(manifest_ref["uri"])
        if hash_path(manifest_path) != manifest_ref["content_hash"]:
            raise ArtifactValidationError("DataSource manifest content hash changed after registration")
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ArtifactValidationError(f"cannot read DataSource manifest: {exc}") from exc
        if (manifest.get("source_id") != source["source_id"]
                or manifest.get("version") != source["version"]
                or manifest.get("content_hash") != source["content_hash"]):
            raise ArtifactValidationError("DataSource manifest identity changed after registration")
        raw_scope = {
            **work["partition_scope"],
            **(work.get("raw_dependency_scope") or work.get("read_scope") or {}),
        }
        data_refs = manifest.get("data_artifacts")
        if not isinstance(data_refs, list):
            raise ArtifactValidationError("DataSource manifest data_artifacts must be an array")
        tables = []
        for data_ref in data_refs:
            path = artifact_path(data_ref["uri"])
            if hash_path(path) != data_ref["content_hash"]:
                raise ArtifactValidationError(f"DataSource artifact hash changed: {data_ref['uri']}")
            try:
                dataset = ds.dataset(path, format="parquet")
            except Exception as exc:
                raise ArtifactValidationError(f"cannot open authorized Parquet input: {exc}") from exc
            schema = dataset.schema
            if "event_time" not in schema.names or "value" not in schema.names:
                raise ArtifactValidationError("synthetic source Parquet needs event_time and value fields")
            event_type = schema.field("event_time").type
            if not pa.types.is_timestamp(event_type) or not event_type.tz:
                raise ArtifactValidationError("source event_time must be timezone-aware")
            if event_type.tz not in {"UTC", "Etc/UTC", "Z"}:
                raise ArtifactValidationError("synthetic source event_time must be stored in UTC")
            columns = [name for name in (
                "event_time", "available_time", "exchange", "instrument_id", "trade_date",
                "session_id", "value",
            ) if name in schema.names]
            expression = self._dataset_filter(raw_scope, schema)
            try:
                tables.append(dataset.to_table(columns=columns, filter=expression))
            except Exception as exc:
                raise ArtifactValidationError(f"cannot read scoped source values: {exc}") from exc
        if not tables:
            raise ArtifactValidationError("DataSource manifest contains no data artifacts")
        try:
            table = pa.concat_tables(tables, promote_options="default") if len(tables) > 1 else tables[0]
        except (TypeError, ValueError, pa.ArrowException) as exc:
            raise ArtifactValidationError(f"source Parquet partitions have incompatible schemas: {exc}") from exc
        normalized = normalize_feature_values_table(table, raw_scope)
        return self._filter_feature_table(normalized, raw_scope)

    @staticmethod
    def _dataset_filter(scope: dict[str, Any], schema: pa.Schema):
        if "event_time" not in schema.names:
            raise ArtifactValidationError("source Parquet has no event_time")
        field = schema.field("event_time")
        dtype = field.type
        start = _as_utc(scope["start"], "scope.start")
        end = _as_utc(scope["end"], "scope.end")
        expression = ((ds.field("event_time") >= pa.scalar(start, type=dtype))
                      & (ds.field("event_time") < pa.scalar(end, type=dtype)))
        for name in ("exchange", "instrument_id", "trade_date", "session_id"):
            value = scope.get(name)
            if value is None or name not in schema.names:
                continue
            column_type = schema.field(name).type
            if name == "trade_date" and pa.types.is_date(column_type):
                from datetime import date

                value = date.fromisoformat(str(value))
            expression = expression & (ds.field(name) == pa.scalar(value, type=column_type))
        return expression

    @staticmethod
    def _filter_feature_table(table: pa.Table, scope: dict[str, Any]) -> pa.Table:
        if table.num_rows == 0:
            return normalize_feature_values_table(table, scope)
        if "event_time" not in table.column_names:
            raise ArtifactValidationError("feature input has no event_time")
        timestamp_type = table.schema.field("event_time").type
        if not pa.types.is_timestamp(timestamp_type) or not timestamp_type.tz:
            raise ArtifactValidationError("feature input event_time must be timezone-aware")
        start = _as_utc(scope["start"], "scope.start")
        end = _as_utc(scope["end"], "scope.end")
        times = table["event_time"].cast(pa.timestamp("us", tz="UTC"))
        keep = pc.and_(pc.greater_equal(times, pa.scalar(start, type=times.type)),
                       pc.less(times, pa.scalar(end, type=times.type)))
        mask = keep
        for name in ("exchange", "instrument_id", "trade_date", "session_id"):
            value = scope.get(name)
            if value is not None and name in table.column_names:
                if name == "trade_date" and pa.types.is_date(table.schema.field(name).type):
                    from datetime import date

                    value = date.fromisoformat(str(value))
                mask = pc.and_(mask, pc.equal(table[name], pa.scalar(value)))
        filtered = table.filter(mask)
        if not all(name in filtered.column_names for name in ("event_time", "value")):
            return normalize_feature_values_table(filtered, scope)
        return normalize_feature_values_table(filtered, scope)

    @staticmethod
    def _time_bounds(table: pa.Table) -> tuple[str | None, str | None]:
        if table.num_rows == 0:
            return None, None
        values = table["event_time"].cast(pa.timestamp("us", tz="UTC")).to_pylist()
        minimum, maximum = min(values), max(values)
        return (minimum.astimezone(UTC).isoformat(timespec="microseconds"),
                maximum.astimezone(UTC).isoformat(timespec="microseconds"))

    def _make_feature_result(self, *, context: dict[str, Any], work: dict[str, Any],
                             feature: dict[str, Any], result_ref: ArtifactRef,
                             fingerprint: str, input_result_refs: list[str], attempt: int,
                             stage_run_id: str, stage_name: str, stage_version: str,
                             source: dict[str, Any], n_total: int, n_valid: int,
                             invalid_reason_counts: dict[str, int], quality_flags: list[str],
                             output: pa.Table) -> dict[str, Any]:
        scope = dict(work["partition_scope"])
        time_min, time_max = self._time_bounds(output)
        result_seed = {
            "computation_fingerprint": fingerprint,
            "artifact_hash": result_ref.content_hash,
            "producer_run_id": context["run_id"],
            "producer_work_key": work["work_key"],
            "attempt": attempt,
        }
        result_id = "result_" + hashlib.sha256(canonical_json(result_seed).encode()).hexdigest()[:32]
        descriptor = {
            "artifact_role": "feature_values",
            "artifact_ref": result_ref.to_dict(),
            "hash": result_ref.content_hash,
            "schema": result_ref.schema_version,
            "columns": [
                "event_time", "available_time", "instrument_id", "trade_date",
                "session_id", "value",
            ],
            "row_count": n_total,
        }
        split_ref = {**context["request"]["split_ref"],
                     "split_id": context["plan"]["split_id"]}
        availability_mode = source.get("availability_mode", "research_only")
        available_observed_count = (
            n_total - output["available_time"].null_count
            if "available_time" in output.column_names else 0
        )
        quality = {
            "n_total": n_total,
            "n_valid": n_valid,
            "invalid_reason_counts": invalid_reason_counts,
            "flags": quality_flags,
        }
        return {
            "schema_version": "feature_result_v1",
            "result_id": result_id,
            "feature_ref": work["feature_ref"],
            "definition_hash": work["definition_hash"],
            "source_ref": work["source_ref"],
            "source_refs": [work["source_ref"]],
            "split_ref": split_ref,
            "scope": scope,
            "processed_scope": dict(scope),
            "required_history": dict(work["required_history"]),
            "input_result_refs": list(input_result_refs),
            "producer_run_id": context["run_id"],
            "producer_work_id": work["work_key"],
            "producer_work_key": work["work_key"],
            "producer_stage_run_id": stage_run_id,
            "producer_stage_name": stage_name,
            "producer_stage_version": stage_version,
            "producer_attempt": attempt,
            "computation_fingerprint": fingerprint,
            "artifacts": [descriptor],
            "coverage": {
                "processed_complete": True,
                "processed_intervals": [dict(scope)],
                "observed_intervals": [],
                "valid_intervals": [],
                "missing_intervals": [],
                "observed_points": {
                    "artifact_ref": result_ref.to_dict(),
                    "time_column": "event_time",
                },
                "valid_points": {
                    "artifact_ref": result_ref.to_dict(),
                    "time_column": "event_time",
                    "predicate": {"column": "value", "operator": "not_null"},
                },
                "observed_count": n_total,
                "valid_count": n_valid,
            },
            "quality": quality,
            "time_metadata": {
                "event_time_column": "event_time",
                "available_time_column": ("available_time" if available_observed_count else None),
                "available_time_observed_count": available_observed_count,
                "availability_mode": availability_mode,
                "timezone": "UTC",
                "session_id": scope["session_id"],
                "time_policy": feature.get("time_policy", {}),
                "sampling": feature.get("sampling", {}),
                "time_min": time_min,
                "time_max": time_max,
            },
            "created_at": _now(),
        }

    def _make_receipt(self, context: dict[str, Any], work: dict[str, Any],
                      result: dict[str, Any], result_ref: ArtifactRef, attempt: int,
                      stage_run_id: str, final_root: Path) -> dict[str, Any]:
        attempt_id = f"{stage_run_id}:attempt:{attempt}"
        receipt_id = "receipt_" + hashlib.sha256(
            f"{context['run_id']}\0{work['work_key']}\0{attempt}\0"
            f"{result['computation_fingerprint']}".encode()
        ).hexdigest()[:32]
        return {
            "schema_version": _RECEIPT_SCHEMA,
            "receipt_id": receipt_id,
            "run_id": context["run_id"],
            "plan_hash": context["plan"]["plan_hash"],
            "work_key": work["work_key"],
            "stage_run_id": stage_run_id,
            "attempt": attempt,
            "attempt_id": attempt_id,
            "computation_fingerprint": result["computation_fingerprint"],
            "result": result,
            "artifact_refs": [result_ref.to_dict()],
            "final_bundle_uri": final_root.as_uri(),
            "created_at": _now(),
        }

    def _receipt_path(self, run_id: str, work_key: str, attempt: int) -> Path:
        run_component = _safe_component(run_id, "run_id")
        work_component = _safe_component(work_key, "work_key")
        if type(attempt) is not int or attempt < 1:
            raise ValueError("attempt must be a positive integer")
        return (self._store.root / ".receipts" / "research" / run_component /
                f"{work_component}.attempt-{attempt}.json")

    def _inject_fault(self, phase: str, context: dict[str, Any]) -> None:
        if self._fault_hook is not None:
            self._fault_hook(phase, dict(context))

    def _mark_blocked(self, stage: dict[str, Any], reason: str) -> None:
        if stage["status"] == StageStatus.BLOCKED.value:
            return
        if stage["status"] in {StageStatus.SUCCEEDED.value, StageStatus.CACHED.value}:
            self._registry.set_research_work_pending(
                stage["stage_run_id"], reason=f"Previously linked result is no longer usable: {reason}"
            )
            stage = self._registry.get_stage_run_by_id(stage["stage_run_id"])
        if stage["status"] in {StageStatus.PENDING.value, StageStatus.FAILED.value,
                               StageStatus.SKIPPED.value}:
            self._registry.set_stage_status(
                stage["stage_run_id"], StageStatus.BLOCKED,
                error={"code": reason.split(":", 1)[0], "message": reason},
            )

    def _reconcile_running_work(self, run_id: str) -> None:
        for stage in self._registry.get_run_stages(run_id):
            if stage.get("work_key") and stage["status"] == StageStatus.RUNNING.value:
                self._registry.set_research_work_pending(
                    stage["stage_run_id"],
                    reason="Executor stopped while this attempt was marked RUNNING",
                )

    def _update_run_summary(self, run_id: str) -> None:
        view = self._planner.get_run(run_id)
        work = view["item_executions"]
        counts = Counter(item["state"] for item in work)
        requested = [item for item in work if item.get("role") == "requested"]
        requested_counts = Counter(item["state"] for item in requested)
        if not work:
            status = "NO_WORK" if view["plan"].get("status") == "NO_WORK" else "BLOCKED"
        elif requested and all(item["state"] in {
            StageStatus.SUCCEEDED.value, StageStatus.CACHED.value,
        } for item in requested):
            status = "COMPLETE"
        elif counts[StageStatus.RUNNING.value]:
            status = "IN_PROGRESS"
        elif (requested_counts[StageStatus.SUCCEEDED.value]
              + requested_counts[StageStatus.CACHED.value]):
            status = "PARTIAL"
        elif requested_counts[StageStatus.FAILED.value]:
            status = "FAILED"
        elif requested_counts[StageStatus.BLOCKED.value]:
            status = "BLOCKED"
        elif requested_counts[StageStatus.SKIPPED.value]:
            status = "SKIPPED"
        elif not requested:
            status = "BLOCKED"
        else:
            status = "NOT_STARTED"
        summary = {
            **view.get("summary", {}),
            "status": status,
            "state_counts": dict(counts),
            "requested_state_counts": dict(requested_counts),
            "result_count": len(view.get("result_refs", [])),
        }
        self._registry.update_research_run_summary(run_id, status=status, summary=summary)

    def _authorize_run(self, context: dict[str, Any]) -> None:
        """Recheck current Study/source/split read boundaries before any resumed computation."""
        request, plan = context["request"], context["plan"]
        frozen_runtime = plan.get("runtime_environment", {})
        from .planner import _runtime_snapshot

        _, _, current_runtime = _runtime_snapshot()
        runtime_fields = (
            "source_tree_hash", "python_version", "python_implementation", "dependencies",
        )
        runtime_changes = {
            field: {"frozen": frozen_runtime.get(field), "current": current_runtime.get(field)}
            for field in runtime_fields
            if frozen_runtime.get(field) != current_runtime.get(field)
        }
        if runtime_changes:
            raise PlanChangedError(
                "executor source or runtime changed after this Run was frozen",
                details={"runtime_changes": runtime_changes},
            )
        current_study = self._repository.get_study(request["study_id"])
        if current_study is None or current_study.get("status") != "active":
            status = None if current_study is None else current_study.get("status")
            raise StudyNotActiveError(
                f"Study is {status or 'missing'} and cannot resume execution",
                details={"study_id": request["study_id"], "status": status},
            )
        if not set(plan.get("source_refs", [])).issubset(current_study.get("source_refs", [])):
            raise SourceScopeMissingError("current Study no longer authorizes all frozen Run sources")
        current_steps = set(current_study.get("limits", {}).get("allowed_steps", []))
        required_steps = {work["step"] for work in plan.get("work", [])}
        if not required_steps.issubset(current_steps):
            raise SourceScopeMissingError("current Study no longer authorizes all frozen Run steps")

        split_ref = request["split_ref"]
        split_path = artifact_path(split_ref["uri"])
        if hash_path(split_path) != split_ref["content_hash"]:
            raise ResearchValidationError("frozen SplitManifest content hash changed")
        try:
            split = json.loads(split_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ResearchValidationError(f"cannot read frozen SplitManifest: {exc}") from exc
        if split.get("split_id") != plan.get("split_id"):
            raise ResearchValidationError("frozen SplitManifest identity changed")

        sources = [self._source(ref) for ref in plan.get("source_refs", [])]
        for source_ref, source in zip(plan.get("source_refs", []), sources, strict=True):
            policy = self._source_access_policy(source)
            if "research" not in policy.get("allowed_uses", []):
                raise SourceScopeMissingError(f"DataSource {source_ref} no longer permits research")
            if split_ref not in policy.get("split_refs", []):
                raise SourceScopeMissingError(
                    f"DataSource {source_ref} does not authorize the frozen split"
                )

        allowed_scope = current_study.get("limits", {}).get("allowed_scope", [])
        for work in plan.get("work", []):
            raw_scope = (work.get("raw_dependency_scope") or work.get("read_scope")
                         or work["partition_scope"])
            required_scope = {
                "instruments": [work["partition_scope"]["instrument_id"]],
                "start": raw_scope["start"],
                "end": raw_scope["end"],
                "session_filter": [work["partition_scope"]["session_id"]],
            }
            if not any(self._scope_is_within(required_scope, allowed) for allowed in allowed_scope):
                raise SourceScopeMissingError(
                    f"current Study no longer authorizes frozen work history: {work['work_key']}"
                )
            if not self._planner_scope_authorized(
                split, required_scope["instruments"][0], raw_scope["start"], raw_scope["end"],
                required_scope["session_filter"],
            ):
                raise SourceScopeMissingError(
                    f"current SplitManifest no longer authorizes frozen work history: {work['work_key']}"
                )

    @staticmethod
    def _source_access_policy(source: dict[str, Any]) -> dict[str, Any]:
        ref = source["access_policy_ref"]
        path = artifact_path(ref["uri"])
        if hash_path(path) != ref["content_hash"]:
            raise ResearchValidationError("DataSource access policy content hash changed")
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ResearchValidationError(f"cannot read DataSource access policy: {exc}") from exc

    @staticmethod
    def _scope_is_within(requested: dict[str, Any], allowed: dict[str, Any]) -> bool:
        from .schemas import scope_is_within

        return scope_is_within(requested, allowed)

    @staticmethod
    def _planner_scope_authorized(split: dict[str, Any], instrument: str,
                                  start: str, end: str, sessions: list[str]) -> bool:
        from .planner import _scope_authorized

        return _scope_authorized(
            split.get("read_ranges", []), instrument,
            _as_utc(start, "scope.start"), _as_utc(end, "scope.end"), set(sessions),
        )

    def _recover_receipts(self, context: dict[str, Any]) -> None:
        receipt_root = self._store.root / ".receipts" / "research"
        if not receipt_root.exists():
            return
        run_id = context["run_id"]
        for path in sorted(receipt_root.rglob("*.json")):
            if path.name.endswith(".reason.json"):
                continue
            if path.parent != receipt_root / run_id:
                if path.is_relative_to(receipt_root / run_id):
                    self._quarantine_receipt(
                        path, "receipt is not at the registered Run receipt path", run_id
                    )
                continue
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                receipt = self._validate_receipt_document(raw, path)
                if receipt["run_id"] != run_id:
                    raise WorkflowError("receipt is stored under a different Run directory")
                self._recover_receipt(context, receipt, path)
            except (ArtifactError, ArtifactValidationError, WorkflowError,
                    ResearchValidationError, UnknownReferenceError, OSError,
                    json.JSONDecodeError, ValueError, KeyError, TypeError) as exc:
                self._quarantine_receipt(path, str(exc), run_id)

    def _validate_receipt_document(self, raw: Any, path: Path) -> dict[str, Any]:
        required = {
            "schema_version", "receipt_id", "run_id", "plan_hash", "work_key",
            "stage_run_id", "attempt", "attempt_id", "computation_fingerprint",
            "result", "artifact_refs", "final_bundle_uri", "created_at",
        }
        if not isinstance(raw, dict) or set(raw) != required:
            raise WorkflowError("publication receipt has an invalid field set")
        if raw["schema_version"] != _RECEIPT_SCHEMA:
            raise WorkflowError("publication receipt schema version is unsupported")
        for key in ("receipt_id", "run_id", "plan_hash", "work_key", "stage_run_id",
                    "attempt_id", "computation_fingerprint", "final_bundle_uri", "created_at"):
            if not isinstance(raw[key], str) or not raw[key]:
                raise WorkflowError(f"publication receipt {key} must be a non-empty string")
        if type(raw["attempt"]) is not int or raw["attempt"] < 1:
            raise WorkflowError("publication receipt attempt must be a positive integer")
        if raw["attempt_id"] != f"{raw['stage_run_id']}:attempt:{raw['attempt']}":
            raise WorkflowError("publication receipt attempt identity is inconsistent")
        if not isinstance(raw["result"], dict) or not isinstance(raw["artifact_refs"], list):
            raise WorkflowError("publication receipt result/artifact refs are malformed")
        if not raw["artifact_refs"]:
            raise WorkflowError("publication receipt must list at least one artifact ref")
        expected_name = f"{raw['work_key']}.attempt-{raw['attempt']}.json"
        if path.name != expected_name:
            raise WorkflowError("publication receipt filename does not match its work/attempt")
        return raw

    def _recover_receipt(self, context: dict[str, Any], receipt: dict[str, Any],
                         receipt_path: Path) -> None:
        run_id = context["run_id"]
        plan = context["plan"]
        if receipt["plan_hash"] != plan.get("plan_hash"):
            raise WorkflowError("publication receipt plan hash does not match the frozen Run")
        work_by_key = {item["work_key"]: item for item in plan.get("work", [])}
        work = work_by_key.get(receipt["work_key"])
        if work is None:
            raise WorkflowError("publication receipt work is not in the frozen graph")
        stage = self._stage_for_work(run_id, {**work, "stage_run_id": receipt["stage_run_id"]})
        if stage["stage_run_id"] != receipt["stage_run_id"]:
            raise WorkflowError("publication receipt stage id does not match the Run ledger")
        result = receipt["result"]
        if result.get("computation_fingerprint") != receipt["computation_fingerprint"]:
            raise WorkflowError("publication receipt computation fingerprint is inconsistent")
        if result.get("producer_run_id") != run_id or result.get("producer_work_key") != work["work_key"]:
            raise WorkflowError("publication receipt result has the wrong producer")
        if result.get("producer_attempt") != receipt["attempt"]:
            raise WorkflowError("publication receipt result has the wrong attempt")
        final_path = artifact_path(receipt["final_bundle_uri"]).resolve()
        permitted_root = (self._store.root / "runs" / run_id / "stages").resolve()
        try:
            final_path.relative_to(permitted_root)
        except ValueError as exc:
            raise WorkflowError("publication receipt bundle is outside its Run artifact directory") from exc
        if not final_path.is_dir():
            raise WorkflowError("publication receipt bundle was never atomically published")

        refs = self._validate_result_for_work(result, context, work, set())
        receipt_refs = [ArtifactRef.from_dict(raw) for raw in receipt["artifact_refs"]]
        result_refs = self._result_artifact_map(result)
        result_refs_by_hash = {ref.content_hash: ref for ref in result_refs.values()}
        if {ref.content_hash for ref in receipt_refs} != set(result_refs_by_hash):
            raise WorkflowError("publication receipt refs do not match its FeatureResult")
        for ref in refs:
            payload = artifact_path(ref.uri).resolve()
            manifest = artifact_path(ref.manifest_uri).resolve()
            if not payload.is_relative_to(final_path) or not manifest.is_relative_to(final_path):
                raise WorkflowError("publication receipt artifact escapes its final bundle")
        metrics = self._quality_metrics(result["quality"])
        self._registry.commit_research_result(
            run_id=run_id,
            work_key=work["work_key"],
            stage_run_id=stage["stage_run_id"],
            result=result,
            artifacts=result_refs,
            metrics=metrics,
            receipt_id=receipt["receipt_id"],
            attempt=receipt["attempt"],
        )

    def _quarantine_receipt(self, path: Path, reason: str, run_id: str | None) -> None:
        raw_run = run_id or path.parent.name
        try:
            component = _safe_component(raw_run, "receipt run_id")
        except ResearchValidationError:
            component = "unknown"
        quarantine = self._store.root / ".receipts" / "quarantine" / "research" / component
        quarantine.mkdir(parents=True, exist_ok=True)
        destination = quarantine / path.name
        if destination.exists():
            destination = quarantine / f"{path.stem}.{uuid.uuid4().hex}{path.suffix}"
        if path.exists():
            os.replace(path, destination)
            from quantsage_discovery.core.artifacts import fsync_directory

            fsync_directory(path.parent)
            fsync_directory(quarantine)
        durable_write_json(destination.with_name(destination.name + ".reason.json"), {
            "schema_version": "receipt_quarantine_reason_v1",
            "source_path": str(path),
            "reason": reason,
            "quarantined_at": _now(),
        })
