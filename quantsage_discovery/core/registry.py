"""SQLite run ledger and structured JSONL operation logs."""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifacts import artifact_path
from .errors import WorkflowError
from .types import ArtifactRef, StageStatus
from .utils import canonical_json


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


_TRANSITIONS = {
    StageStatus.PENDING: {StageStatus.RUNNING, StageStatus.CACHED, StageStatus.SKIPPED,
                          StageStatus.BLOCKED},
    StageStatus.RUNNING: {StageStatus.RUNNING, StageStatus.SUCCEEDED, StageStatus.CACHED,
                          StageStatus.FAILED, StageStatus.BLOCKED},
    StageStatus.FAILED: {StageStatus.RUNNING, StageStatus.CACHED, StageStatus.BLOCKED},
    StageStatus.BLOCKED: {StageStatus.RUNNING, StageStatus.CACHED},
    StageStatus.SKIPPED: {StageStatus.RUNNING, StageStatus.CACHED},
    StageStatus.CACHED: {StageStatus.RUNNING, StageStatus.BLOCKED},
    StageStatus.SUCCEEDED: {StageStatus.RUNNING, StageStatus.BLOCKED},
}


class StructuredLogger:
    def __init__(self, path: Path, run_id: str):
        self.path = path
        self.run_id = run_id
        self._lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event: str, **fields: Any) -> None:
        record = {
            "timestamp": utc_now(),
            "event": event,
            "run_id": self.run_id,
            "stage_run_id": fields.pop("stage_run_id", None),
            "stage": fields.pop("stage", None),
            "partition": fields.pop("partition", None),
            "candidate_id": fields.pop("candidate_id", None),
            **fields,
        }
        line = canonical_json(record) + "\n"
        with self._lock, self.path.open("a", encoding="utf-8") as stream:
            stream.write(line)
            stream.flush()


class Registry:
    def __init__(self, path: Path | str):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    config_json TEXT NOT NULL,
                    config_hash TEXT NOT NULL,
                    data_snapshot_id TEXT,
                    split_id TEXT,
                    as_of_cutoff TEXT,
                    code_revision TEXT NOT NULL,
                    graph_json TEXT NOT NULL,
                    execution_json TEXT NOT NULL DEFAULT '{}',
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS stage_runs (
                    stage_run_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
                    stage_name TEXT NOT NULL,
                    stage_version TEXT NOT NULL,
                    partition_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    cache_key TEXT,
                    config_hash TEXT,
                    code_revision TEXT,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    started_at TEXT,
                    finished_at TEXT,
                    duration_ms REAL,
                    inputs_json TEXT NOT NULL DEFAULT '{}',
                    artifacts_json TEXT NOT NULL DEFAULT '{}',
                    metrics_json TEXT NOT NULL DEFAULT '{}',
                    warnings_json TEXT NOT NULL DEFAULT '[]',
                    error_json TEXT,
                    source_stage_run_id TEXT,
                    cache_valid INTEGER NOT NULL DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS stage_run_cache_idx
                    ON stage_runs(stage_name, stage_version, cache_key, cache_valid, status);
                CREATE INDEX IF NOT EXISTS stage_run_run_idx ON stage_runs(run_id, stage_name);
                CREATE TABLE IF NOT EXISTS stage_state_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stage_run_id TEXT NOT NULL REFERENCES stage_runs(stage_run_id),
                    state TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    fields_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            run_columns = {row[1] for row in connection.execute("PRAGMA table_info(runs)")}
            if "execution_json" not in run_columns:
                connection.execute("ALTER TABLE runs ADD COLUMN execution_json TEXT NOT NULL DEFAULT '{}' ")

    def create_run(self, *, run_id: str, config: Mapping[str, Any], config_hash: str,
                   data_snapshot_id: str | None, split_id: str | None,
                   as_of_cutoff: str | None, code_revision: str,
                   graph: Mapping[str, Any], execution: Mapping[str, Any] | None = None) -> None:
        timestamp = utc_now()
        try:
            with self._connect() as connection:
                connection.execute(
                    """INSERT INTO runs(run_id, config_json, config_hash, data_snapshot_id,
                       split_id, as_of_cutoff, code_revision, graph_json, execution_json,
                       status, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (run_id, canonical_json(config), config_hash, data_snapshot_id, split_id,
                     as_of_cutoff, code_revision, canonical_json(graph), canonical_json(execution or {}),
                     "RUNNING", timestamp, timestamp),
                )
        except sqlite3.IntegrityError as exc:
            raise WorkflowError(f"run_id already exists: {run_id}") from exc

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            return None
        result = dict(row)
        for key in ("config_json", "graph_json", "execution_json"):
            result[key.removesuffix("_json")] = json.loads(result[key])
        return result

    def finish_run(self, run_id: str, status: str) -> None:
        with self._connect() as connection:
            connection.execute("UPDATE runs SET status=?, updated_at=? WHERE run_id=?",
                               (status, utc_now(), run_id))

    def ensure_stage_run(self, *, run_id: str, stage_name: str, stage_version: str,
                         partition: Mapping[str, str], stage_run_id: str,
                         cache_key: str | None, config_hash: str | None,
                         code_revision: str | None, force_new: bool = False) -> dict[str, Any]:
        partition_json = canonical_json(dict(partition))
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM stage_runs WHERE run_id=? AND stage_name=? AND partition_json=? ORDER BY rowid DESC LIMIT 1",
                (run_id, stage_name, partition_json),
            ).fetchone()
            if row is None or force_new:
                connection.execute(
                    """INSERT INTO stage_runs(stage_run_id, run_id, stage_name, stage_version,
                       partition_json, status, cache_key, config_hash, code_revision)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (stage_run_id, run_id, stage_name, stage_version, partition_json,
                     StageStatus.PENDING, cache_key, config_hash, code_revision),
                )
                row = connection.execute("SELECT * FROM stage_runs WHERE stage_run_id=?",
                                         (stage_run_id,)).fetchone()
        return self._decode_stage_row(row)

    def get_stage_run(self, run_id: str, stage_name: str,
                      partition: Mapping[str, str]) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM stage_runs WHERE run_id=? AND stage_name=? AND partition_json=? ORDER BY rowid DESC LIMIT 1",
                (run_id, stage_name, canonical_json(dict(partition))),
            ).fetchone()
        return self._decode_stage_row(row) if row else None

    @staticmethod
    def _decode_stage_row(row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
        result = dict(row)
        for key in ("partition_json", "inputs_json", "artifacts_json", "metrics_json", "warnings_json"):
            result[key.removesuffix("_json")] = json.loads(result[key])
        if result.get("error_json"):
            result["error"] = json.loads(result["error_json"])
        else:
            result["error"] = None
        return result

    def set_stage_running(self, stage_run_id: str, *, cache_key: str, config_hash: str,
                          code_revision: str, inputs: Mapping[str, ArtifactRef]) -> int:
        return self._transition(
            stage_run_id,
            StageStatus.RUNNING,
            cache_key=cache_key,
            config_hash=config_hash,
            code_revision=code_revision,
            inputs_json=canonical_json({key: ref.to_dict() for key, ref in sorted(inputs.items())}),
            started_at=utc_now(),
            finished_at=None,
            duration_ms=None,
            error_json=None,
            attempt_increment=1,
        )

    def set_stage_complete(self, stage_run_id: str, status: StageStatus, *,
                           artifacts: Mapping[str, ArtifactRef] | None = None,
                           metrics: Mapping[str, Any] | None = None,
                           warnings: tuple[str, ...] | list[str] = (),
                           duration_ms: float | None = None,
                           source_stage_run_id: str | None = None) -> None:
        if status not in {StageStatus.SUCCEEDED, StageStatus.CACHED}:
            raise ValueError("complete status must be SUCCEEDED or CACHED")
        self._transition(
            stage_run_id,
            status,
            artifacts_json=canonical_json({key: ref.to_dict() for key, ref in sorted((artifacts or {}).items())}),
            metrics_json=canonical_json(metrics or {}),
            warnings_json=canonical_json(list(warnings)),
            duration_ms=duration_ms,
            finished_at=utc_now(),
            error_json=None,
            source_stage_run_id=source_stage_run_id,
        )

    def set_stage_failed(self, stage_run_id: str, *, error: Mapping[str, Any],
                         duration_ms: float | None = None) -> None:
        self._transition(stage_run_id, StageStatus.FAILED,
                         error_json=canonical_json(error), duration_ms=duration_ms,
                         finished_at=utc_now())

    def set_stage_status(self, stage_run_id: str, status: StageStatus, *, error: Mapping[str, Any] | None = None) -> None:
        self._transition(stage_run_id, status,
                         error_json=canonical_json(error) if error else None,
                         finished_at=utc_now() if status in {StageStatus.SKIPPED, StageStatus.BLOCKED} else None)

    def _transition(self, stage_run_id: str, status: StageStatus, **fields: Any) -> int:
        with self._connect() as connection:
            row = connection.execute("SELECT status, attempt FROM stage_runs WHERE stage_run_id=?",
                                     (stage_run_id,)).fetchone()
            if row is None:
                raise WorkflowError(f"unknown stage_run_id: {stage_run_id}")
            current = StageStatus(row["status"])
            if status not in _TRANSITIONS[current]:
                raise WorkflowError(f"invalid stage status transition {current.value} -> {status.value}")
            if fields.pop("attempt_increment", 0):
                fields["attempt"] = int(row["attempt"]) + 1
            fields["status"] = status.value
            assignments = ", ".join(f"{key}=?" for key in fields)
            values = list(fields.values()) + [stage_run_id]
            connection.execute(f"UPDATE stage_runs SET {assignments} WHERE stage_run_id=?", values)
            connection.execute(
                "INSERT INTO stage_state_events(stage_run_id,state,attempt,fields_json,created_at) "
                "VALUES(?,?,?,?,?)", (stage_run_id, status.value,
                                       int(fields.get('attempt', row['attempt'])),
                                       canonical_json(fields), utc_now()))
            return int(fields.get("attempt", row["attempt"]))

    def get_stage_run_by_id(self, stage_run_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute('SELECT * FROM stage_runs WHERE stage_run_id=?',
                                     (stage_run_id,)).fetchone()
        return self._decode_stage_row(row) if row else None

    def get_stage_events(self, stage_run_id: str) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                'SELECT * FROM stage_state_events WHERE stage_run_id=? ORDER BY event_id',
                (stage_run_id,)).fetchall()
        return [{**dict(row), 'fields': json.loads(row['fields_json'])} for row in rows]

    def find_cache(self, stage_name: str, stage_version: str, cache_key: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM stage_runs WHERE stage_name=? AND stage_version=? AND cache_key=?
                   AND cache_valid=1 AND status='SUCCEEDED' ORDER BY finished_at DESC LIMIT 1""",
                (stage_name, stage_version, cache_key),
            ).fetchone()
        return self._decode_stage_row(row) if row else None

    def invalidate_cache(self, stage_names: set[str]) -> int:
        if not stage_names:
            return 0
        placeholders = ",".join("?" for _ in stage_names)
        with self._connect() as connection:
            cursor = connection.execute(
                f"UPDATE stage_runs SET cache_valid=0 WHERE stage_name IN ({placeholders})",
                sorted(stage_names),
            )
            return int(cursor.rowcount)

    def invalidate_stage_run(self, stage_run_id: str) -> None:
        with self._connect() as connection:
            connection.execute("UPDATE stage_runs SET cache_valid=0 WHERE stage_run_id=?", (stage_run_id,))

    def get_run_stages(self, run_id: str) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM stage_runs WHERE run_id=? ORDER BY stage_name, partition_json",
                                      (run_id,)).fetchall()
        return [self._decode_stage_row(row) for row in rows]

    def explain_artifact(self, artifact_id: str) -> list[dict[str, Any]]:
        matches = []
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM stage_runs WHERE status IN ('SUCCEEDED','CACHED')").fetchall()
        for row in rows:
            decoded = self._decode_stage_row(row)
            for alias, raw_ref in decoded["artifacts"].items():
                if raw_ref.get("artifact_id") == artifact_id:
                    matches.append({
                        "run_id": decoded["run_id"],
                        "stage_run_id": decoded["stage_run_id"],
                        "stage_name": decoded["stage_name"],
                        "stage_version": decoded["stage_version"],
                        "alias": alias,
                        "status": decoded["status"],
                        "artifact": raw_ref,
                        "inputs": decoded["inputs"],
                    })
        return matches

    def find_artifact_manifest(self, artifact_id: str) -> tuple[ArtifactRef, dict[str, Any]] | None:
        matches = self.explain_artifact(artifact_id)
        for match in reversed(matches):
            try:
                ref = ArtifactRef.from_dict(match["artifact"])
                manifest_path = artifact_path(ref.manifest_uri)
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                return ref, manifest
            except (OSError, json.JSONDecodeError, ValueError):
                continue
        return None

    def get_stage_statuses(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run is None:
            raise WorkflowError(f"unknown run_id: {run_id}")
        stages = self.get_run_stages(run_id)
        return {"run_id": run_id, "status": run["status"], "stages": stages,
                "created_at": run["created_at"], "updated_at": run["updated_at"]}


def new_stage_run_id() -> str:
    return "stage_run_" + uuid.uuid4().hex
