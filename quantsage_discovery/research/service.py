"""Application service for immutable DataSources and revisioned Studies."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from quantsage_discovery.core.artifacts import ArtifactStore
from quantsage_discovery.core.registry import Registry

from .catalog import FeatureCatalog
from .errors import ResearchValidationError, StudyNotActiveError, UnknownReferenceError
from .repository import ResearchRepository
from .schemas import (
    parse_versioned_ref,
    scope_is_within,
    source_access_policy,
    study_scope_is_covered,
    validate_source_document,
    validate_study_document,
)

_STUDY_MUTABLE_FIELDS = {
    "name", "objective", "status", "source_refs", "feature_set_refs", "backlog", "limits",
    "default_feature_set_ref", "default_scope", "evaluation_guide", "notes", "tags",
}
_STUDY_DOCUMENT_FIELDS = _STUDY_MUTABLE_FIELDS | {
    "schema_version", "study_id", "created_by", "change_reason",
}


class ResearchService:
    """Validated management use cases backed by the Registry's single owner."""

    def __init__(self, registry: Registry):
        self._registry = registry
        self._repository = ResearchRepository(registry)
        self._catalog = FeatureCatalog(registry)

    def register_provider(self, document: Mapping[str, Any]) -> dict[str, Any]:
        return self._catalog.register_provider(document)

    def register_template(self, document: Mapping[str, Any]) -> dict[str, Any]:
        return self._catalog.register_template(document)

    def register_metric(self, document: Mapping[str, Any]) -> dict[str, Any]:
        return self._catalog.register_metric(document)

    def register_metric_set(self, document: Mapping[str, Any]) -> dict[str, Any]:
        return self._catalog.register_metric_set(document)

    def get_provider(self, ref: str) -> dict[str, Any]:
        return self._catalog.get_provider(ref)

    def get_template(self, ref: str) -> dict[str, Any]:
        return self._catalog.get_template(ref)

    def get_metric(self, ref: str) -> dict[str, Any]:
        return self._catalog.get_metric(ref)

    def get_metric_set(self, ref: str) -> dict[str, Any]:
        return self._catalog.get_metric_set(ref)

    def save_feature_set(self, document: Mapping[str, Any], *,
                         expected_revision: int | None = None) -> dict[str, Any]:
        return self._catalog.save_feature_set(document, expected_revision=expected_revision)

    def freeze_feature_set(self, ref: str, *, expected_revision: int) -> dict[str, Any]:
        return self._catalog.freeze_feature_set(ref, expected_revision=expected_revision)

    def get_feature_set(self, ref: str) -> dict[str, Any]:
        return self._catalog.get_feature_set(ref)

    def get_feature(self, ref: str) -> dict[str, Any]:
        return self._catalog.get_feature(ref)

    def list_features(self, filters: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        return self._catalog.list_features(filters)

    def plan_run(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return self._run_planner().plan_run(request)

    def submit_run(self, request: Mapping[str, Any], *, request_key: str,
                   expected_plan_hash: str | None = None) -> dict[str, Any]:
        return self._run_planner().submit_run(
            request, request_key=request_key, expected_plan_hash=expected_plan_hash,
        )

    def get_run(self, run_id: str) -> dict[str, Any]:
        return self._run_planner().get_run(run_id)

    def execute_run(self, run_id: str, *, fault_hook=None) -> dict[str, Any]:
        return self._run_executor(fault_hook=fault_hook).execute_run(run_id)

    def resume_run(self, run_id: str, *, fault_hook=None) -> dict[str, Any]:
        return self._run_executor(fault_hook=fault_hook).resume_run(run_id)

    def recover_runs(self, run_id: str | None = None) -> dict[str, Any]:
        return self._run_executor().recover(run_id)

    def _run_executor(self, *, fault_hook=None):
        from .execution import ResearchExecutor

        return ResearchExecutor(
            self._registry, ArtifactStore(self._registry.path.parent / "artifacts"),
            self._catalog, fault_hook=fault_hook,
        )

    def _run_planner(self):
        from .planner import RunPlanner

        return RunPlanner(self._registry, self._repository, self._catalog)

    def register_source(self, document: Mapping[str, Any]) -> dict[str, Any]:
        """Validate and publish one immutable DataSource version."""
        source = validate_source_document(dict(document))
        return self._repository.register_source(source)

    def get_source(self, ref: str) -> dict[str, Any]:
        source_id, version = parse_versioned_ref(ref, "source_ref")
        source = self._repository.get_source(source_id, version)
        if source is None:
            raise UnknownReferenceError(f"unknown DataSource ref: {ref}", details={"ref": ref})
        return source

    def create_study(self, document: Mapping[str, Any]) -> dict[str, Any]:
        study = validate_study_document(dict(document))
        self._validate_study_bindings(study)
        return self._repository.create_study(study)

    def update_study(self, study_id: str, changes: Mapping[str, Any], *,
                     expected_revision: int, actor: Mapping[str, str], reason: str) -> dict[str, Any]:
        if type(expected_revision) is not int or expected_revision < 1:
            raise ResearchValidationError("expected_revision must be an integer >= 1")
        if not isinstance(changes, Mapping) or not changes:
            raise ResearchValidationError("changes must be a non-empty object")
        unknown = set(changes) - _STUDY_MUTABLE_FIELDS
        if unknown:
            raise ResearchValidationError(
                f"unknown or immutable Study fields: {', '.join(sorted(unknown))}",
                details={"fields": sorted(unknown)},
            )
        current = self._repository.get_study(study_id)
        if current is None:
            raise UnknownReferenceError(f"unknown Study: {study_id}", details={"study_id": study_id})
        merged = {field: current[field] for field in _STUDY_DOCUMENT_FIELDS if field in current}
        merged.update(dict(changes))
        merged["change_reason"] = reason
        validated = validate_study_document(merged)
        validated["created_by"] = current["created_by"]
        validated_actor = _validated_actor(actor)
        self._validate_study_bindings(validated)
        return self._repository.update_study(
            study_id, validated, expected_revision=expected_revision,
            actor=validated_actor, reason=_required_reason(reason),
        )

    def get_study(self, study_id: str, revision: int | None = None) -> dict[str, Any]:
        if revision is not None and (type(revision) is not int or revision < 1):
            raise ResearchValidationError("revision must be an integer >= 1")
        study = self._repository.get_study(study_id, revision)
        if study is None:
            details = {"study_id": study_id}
            if revision is not None:
                details["revision"] = revision
            raise UnknownReferenceError(f"unknown Study revision: {study_id}@{revision or 'current'}",
                                        details=details)
        return study

    def study_history(self, study_id: str) -> list[dict[str, Any]]:
        if self._repository.get_study(study_id) is None:
            raise UnknownReferenceError(f"unknown Study: {study_id}", details={"study_id": study_id})
        return self._repository.study_history(study_id)

    def ensure_active_for_execution(self, study_id: str) -> dict[str, Any]:
        """Guard used by later plan/submit calls; paused and closed Studies stay queryable."""
        study = self.get_study(study_id)
        if study["status"] != "active":
            raise StudyNotActiveError(
                f"Study {study_id} is {study['status']} and cannot accept new execution",
                details={"study_id": study_id, "status": study["status"],
                         "revision": study["revision"]},
            )
        return study

    def _validate_study_bindings(self, study: dict[str, Any]) -> None:
        frozen_sets = {}
        for ref in study["feature_set_refs"]:
            feature_set = self.get_feature_set(ref)
            if feature_set["status"] != "frozen":
                raise ResearchValidationError(f"Study FeatureSet must be frozen: {ref}")
            frozen_sets[ref] = {item["item_id"] for item in feature_set["resolved_items"]}
        for item in study["backlog"]:
            if item["item_id"] not in frozen_sets[item["feature_set_ref"]]:
                raise UnknownReferenceError(
                    f"unknown frozen FeatureSet item: {item['feature_set_ref']}/{item['item_id']}"
                )
        sources = []
        for ref in study["source_refs"]:
            source_id, version = parse_versioned_ref(ref, "study.source_refs[]")
            source = self._repository.get_source(source_id, version)
            if source is None:
                raise UnknownReferenceError(f"unknown DataSource ref: {ref}", details={"ref": ref})
            if "research" not in source_access_policy(source)["allowed_uses"]:
                raise ResearchValidationError(
                    f"DataSource {ref} access policy does not permit research"
                )
            sources.append(source)

        for index, allowed in enumerate(study["limits"]["allowed_scope"]):
            if not study_scope_is_covered(allowed, sources):
                raise ResearchValidationError(
                    f"study.limits.allowed_scope[{index}] is not covered by its DataSource refs",
                    details={"scope_index": index},
                )
        if "default_scope" in study:
            default_scope = study["default_scope"]
            if not any(scope_is_within(default_scope, allowed)
                       for allowed in study["limits"]["allowed_scope"]):
                raise ResearchValidationError(
                    "study.default_scope exceeds every declared allowed_scope"
                )
            if not study_scope_is_covered(default_scope, sources):
                raise ResearchValidationError("study.default_scope is not covered by its DataSource refs")


def _validated_actor(actor: Mapping[str, str]) -> dict[str, str]:
    if not isinstance(actor, Mapping) or set(actor) != {"kind", "id"}:
        raise ResearchValidationError("actor must contain exactly kind and id")
    kind = actor["kind"]
    actor_id = actor["id"]
    if kind not in {"human", "agent"} or not isinstance(actor_id, str) or not actor_id.strip():
        raise ResearchValidationError("actor.kind must be human/agent and actor.id non-empty")
    return {"kind": kind, "id": actor_id}


def _required_reason(reason: str) -> str:
    if not isinstance(reason, str) or not reason.strip():
        raise ResearchValidationError("reason must be a non-empty string")
    return reason
