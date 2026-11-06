"""Application service for immutable DataSources and revisioned Studies."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from quantsage_discovery.core.registry import Registry

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
        self._repository = ResearchRepository(registry)

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
