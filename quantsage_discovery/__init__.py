"""QuantSage's extensible, local workflow engine."""

from .core import (
    ArtifactManifest,
    ArtifactRef,
    ResourceBudget,
    RunContext,
    Stage,
    StageResult,
    StageStatus,
    WorkflowEngine,
)

__all__ = [
    "ArtifactManifest",
    "ArtifactRef",
    "ResourceBudget",
    "RunContext",
    "Stage",
    "StageResult",
    "StageStatus",
    "WorkflowEngine",
]

__version__ = "0.1.0"
