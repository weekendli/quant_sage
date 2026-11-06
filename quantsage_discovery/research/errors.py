"""Errors raised by the research management API."""

from __future__ import annotations

from typing import Any

from quantsage_discovery.core.errors import WorkflowError


class ResearchError(WorkflowError):
    """Base error with a stable machine-readable code."""

    code = "RESEARCH_ERROR"
    retryable = False

    def __init__(self, message: str, *, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.details = details or {}


class ResearchValidationError(ResearchError):
    code = "INVALID_CONFIG"


class RevisionConflictError(ResearchError):
    code = "REVISION_CONFLICT"


class UnknownReferenceError(ResearchError):
    code = "UNKNOWN_REF"


class ImmutableVersionError(ResearchError):
    code = "IDENTITY_CONFLICT"


class StudyNotActiveError(ResearchError):
    code = "STUDY_NOT_ACTIVE"
