"""Workflow errors and their retry classification."""


class WorkflowError(Exception):
    """Base class for workflow contract errors."""


class ConfigError(WorkflowError):
    """Configuration failed validation."""


class GraphError(WorkflowError):
    """The registered stage graph is invalid."""


class ArtifactError(WorkflowError):
    """An artifact is missing, corrupt, or violates its contract."""


class ArtifactValidationError(ArtifactError):
    """A candidate output failed schema validation."""


class TransientError(WorkflowError):
    """A stage explicitly identifies an execution failure as retryable."""


class StageExecutionError(WorkflowError):
    """A stage failed and will not be retried automatically."""
