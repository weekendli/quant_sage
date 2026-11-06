"""Small deterministic stages for isolated workflow-engine acceptance tests."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from quantsage_discovery.core import (
    ArtifactStore,
    ArtifactValidationError,
    RunContext,
    StageResult,
    TransientError,
)
from quantsage_discovery.core.artifacts import artifact_path

RECORD_SCHEMA = {
    "type": "object",
    "required": ["value"],
    "properties": {"value": {}},
    "additionalProperties": False,
}


def validate_record(path: Path) -> None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ArtifactValidationError(str(exc)) from exc
    if not isinstance(value, dict) or "value" not in value or set(value) - {"value"}:
        raise ArtifactValidationError("record must be an object containing only value")


def register_record_validator(store: ArtifactStore) -> None:
    store.register_validator("test_record", "1.0", validate_record)


class JsonStage:
    name = "json_stage"
    version = "1.0"
    dependencies: tuple[str, ...] = ()
    config_keys: tuple[str, ...] = ()
    partitioned = True

    def __init__(self, *, name: str | None = None, dependencies: tuple[str, ...] = (),
                 config_keys: tuple[str, ...] | None = (), value: Any = "same",
                 record: Path | None = None):
        if name is not None:
            self.name = name
        self.dependencies = dependencies
        self.config_keys = config_keys
        self.value = value
        self.record = record
        self.execute_calls = 0
        self.validate_calls = 0

    def register_validators(self, store: ArtifactStore) -> None:
        register_record_validator(store)

    def validate(self, ctx: RunContext, inputs: dict[str, Any]) -> None:
        self.validate_calls += 1

    def execute(self, ctx: RunContext, inputs: dict[str, Any]) -> StageResult:
        self.execute_calls += 1
        if self.record:
            with self.record.open("a", encoding="utf-8") as stream:
                stream.write(f"{ctx.partition_keys}\n")
        value = self.value(ctx, inputs) if callable(self.value) else self.value
        ref = ctx.artifact_transaction.write_json(
            "result", {"value": value}, artifact_type="test_record", schema_version="1.0",
            row_count=1,
        )
        return StageResult(artifacts={"result": ref}, metrics={"rows": 1})


class AtomicWriter(JsonStage):
    name = "writer"
    config_keys = ("bad_output",)

    def __init__(self):
        super().__init__(name="writer", config_keys=("bad_output",))

    def execute(self, ctx: RunContext, inputs: dict[str, Any]) -> StageResult:
        self.execute_calls += 1
        good = ctx.artifact_transaction.write_json(
            "good", {"value": "stable"}, artifact_type="test_record", schema_version="1.0"
        )
        artifacts = {"good": good}
        if ctx.config.get("bad_output"):
            bad = ctx.artifact_transaction.write_bytes(
                "bad", b"no schema", artifact_type="unknown_type", schema_version="9.0"
            )
            artifacts["bad"] = bad
        return StageResult(artifacts=artifacts)


class MutatesAfterRegister(JsonStage):
    name = "mutator"

    def __init__(self):
        super().__init__(name="mutator")

    def execute(self, ctx: RunContext, inputs: dict[str, Any]) -> StageResult:
        path = ctx.artifact_transaction.reserve_output(
            "result", artifact_type="test_record", schema_version="1.0"
        )
        path.write_text('{"value":"before"}\n', encoding="utf-8")
        ref = ctx.artifact_transaction.register_output("result")
        path.write_text('{"value":"changed"}\n', encoding="utf-8")
        return StageResult(artifacts={"result": ref})


class RetriesOnce(JsonStage):
    name = "retry_once"

    def __init__(self):
        super().__init__(name="retry_once")
        self.attempts = 0

    def execute(self, ctx: RunContext, inputs: dict[str, Any]) -> StageResult:
        self.attempts += 1
        if self.attempts == 1:
            raise TransientError("test retry")
        return super().execute(ctx, inputs)


class FailsValidation(JsonStage):
    name = "bad_validation"

    def __init__(self):
        super().__init__(name="bad_validation")

    def validate(self, ctx: RunContext, inputs: dict[str, Any]) -> None:
        self.validate_calls += 1
        raise ValueError("input contract failed")


class EmptyInput(JsonStage):
    name = "empty_input"

    def register_validators(self, store: ArtifactStore) -> None:
        store.register_json_schema("empty_rows", "1.0", {"type": "array"})

    def execute(self, ctx: RunContext, inputs: dict[str, Any]) -> StageResult:
        ref = ctx.artifact_transaction.write_json(
            "rows", [], artifact_type="empty_rows", schema_version="1.0", row_count=0
        )
        return StageResult(artifacts={"rows": ref}, metrics={"rows": 0}, warnings=("no rows",))


class CrashByPartition(JsonStage):
    name = "crash_by_partition"

    def __init__(self):
        super().__init__(name="crash_by_partition")

    def execute(self, ctx: RunContext, inputs: dict[str, Any]) -> StageResult:
        marker = Path(ctx.config["crash_marker"])
        if ctx.partition_keys.get("item") == "b" and not marker.exists():
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text("interrupted once", encoding="utf-8")
            os._exit(23)
        return super().execute(ctx, inputs)


class CliStage(JsonStage):
    name = "cli_stage"

    def __init__(self):
        super().__init__(name="cli_stage", value="cli")


def read_record(ref: Any) -> dict[str, Any]:
    return json.loads(artifact_path(ref.uri).read_text(encoding="utf-8"))
