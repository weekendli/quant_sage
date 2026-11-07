"""DuckDB-backed run ledger with a process-local, single-threaded database owner."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import queue
import threading
import uuid
import weakref
from collections.abc import Callable, Mapping
from concurrent.futures import Future
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Self, TypeVar

import duckdb

from .artifacts import artifact_path
from .errors import RegistrySchemaError, WorkflowError, WorkspaceBusyError
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


_SCHEMA_V1_DDL = """
CREATE SEQUENCE stage_run_order_seq START 1;
CREATE SEQUENCE stage_state_event_seq START 1;
CREATE TABLE runs (
    run_id VARCHAR PRIMARY KEY,
    config_json VARCHAR NOT NULL,
    config_hash VARCHAR NOT NULL,
    data_snapshot_id VARCHAR,
    split_id VARCHAR,
    as_of_cutoff VARCHAR,
    code_revision VARCHAR NOT NULL,
    graph_json VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL
);
CREATE TABLE stage_runs (
    stage_run_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    stage_version VARCHAR NOT NULL,
    partition_json VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    cache_key VARCHAR,
    config_hash VARCHAR,
    code_revision VARCHAR,
    attempt INTEGER NOT NULL DEFAULT 0,
    started_at VARCHAR,
    finished_at VARCHAR,
    duration_ms DOUBLE,
    inputs_json VARCHAR NOT NULL DEFAULT '{}',
    artifacts_json VARCHAR NOT NULL DEFAULT '{}',
    metrics_json VARCHAR NOT NULL DEFAULT '{}',
    warnings_json VARCHAR NOT NULL DEFAULT '[]',
    error_json VARCHAR,
    source_stage_run_id VARCHAR,
    cache_valid INTEGER NOT NULL DEFAULT 1,
    registry_order BIGINT NOT NULL UNIQUE DEFAULT nextval('stage_run_order_seq')
);
CREATE INDEX stage_run_cache_idx
    ON stage_runs(stage_name, stage_version, cache_key, cache_valid, status);
CREATE INDEX stage_run_run_idx ON stage_runs(run_id, stage_name);
CREATE TABLE stage_state_events (
    event_id BIGINT PRIMARY KEY DEFAULT nextval('stage_state_event_seq'),
    stage_run_id VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    attempt INTEGER NOT NULL,
    fields_json VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL
);
""".strip()

# v1 is the first supported DuckDB ledger. v2 adds the execution scope already
# required by the existing workflow API; SQLite files are never imported.
_MIGRATION_V2_SQL = "ALTER TABLE runs ADD COLUMN execution_json VARCHAR DEFAULT '{}'"
_SCHEMA_V1_CHECKSUM = hashlib.sha256(_SCHEMA_V1_DDL.encode("utf-8")).hexdigest()
_SCHEMA_V2_CHECKSUM = hashlib.sha256(
    f"{_SCHEMA_V1_CHECKSUM}\n{_MIGRATION_V2_SQL}".encode()
).hexdigest()
_MIGRATION_V3_SQL = """
CREATE TABLE data_sources (
    source_id VARCHAR NOT NULL,
    version INTEGER NOT NULL,
    content_hash VARCHAR NOT NULL,
    manifest_uri VARCHAR NOT NULL,
    manifest_hash VARCHAR NOT NULL,
    document_json VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL,
    created_by_json VARCHAR NOT NULL,
    PRIMARY KEY (source_id, version)
);
CREATE TABLE studies (
    study_id VARCHAR PRIMARY KEY,
    current_revision INTEGER NOT NULL,
    status VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL
);
CREATE TABLE study_revisions (
    study_id VARCHAR NOT NULL,
    revision INTEGER NOT NULL,
    document_json VARCHAR NOT NULL,
    actor_json VARCHAR NOT NULL,
    reason VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL,
    PRIMARY KEY (study_id, revision)
);
CREATE INDEX data_sources_hash_idx ON data_sources(source_id, content_hash);
CREATE INDEX studies_status_idx ON studies(status, updated_at);
""".strip()
_SCHEMA_V3_CHECKSUM = hashlib.sha256(
    f"{_SCHEMA_V2_CHECKSUM}\n{_MIGRATION_V3_SQL}".encode()
).hexdigest()
_MIGRATION_V4_SQL = """
CREATE TABLE providers (
    provider_id VARCHAR NOT NULL,
    version VARCHAR NOT NULL,
    provider_key VARCHAR NOT NULL,
    document_json VARCHAR NOT NULL,
    PRIMARY KEY (provider_id, version)
);
CREATE TABLE feature_templates (
    template_id VARCHAR NOT NULL,
    version VARCHAR NOT NULL,
    document_json VARCHAR NOT NULL,
    PRIMARY KEY (template_id, version)
);
CREATE TABLE metrics (
    metric_id VARCHAR NOT NULL,
    version VARCHAR NOT NULL,
    document_json VARCHAR NOT NULL,
    PRIMARY KEY (metric_id, version)
);
CREATE TABLE metric_sets (
    metric_set_id VARCHAR NOT NULL,
    version INTEGER NOT NULL,
    document_json VARCHAR NOT NULL,
    PRIMARY KEY (metric_set_id, version)
);
CREATE TABLE features (
    feature_id VARCHAR NOT NULL,
    version INTEGER NOT NULL,
    definition_hash VARCHAR NOT NULL,
    kind VARCHAR NOT NULL,
    step INTEGER NOT NULL,
    status VARCHAR NOT NULL,
    document_json VARCHAR NOT NULL,
    PRIMARY KEY (feature_id, version)
);
CREATE INDEX features_definition_idx ON features(definition_hash);
CREATE TABLE feature_sets (
    set_id VARCHAR NOT NULL,
    version INTEGER NOT NULL,
    state VARCHAR NOT NULL,
    draft_revision INTEGER NOT NULL,
    config_hash VARCHAR,
    document_json VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL,
    PRIMARY KEY (set_id, version)
);
CREATE TABLE feature_set_revisions (
    set_id VARCHAR NOT NULL,
    version INTEGER NOT NULL,
    draft_revision INTEGER NOT NULL,
    document_json VARCHAR NOT NULL,
    actor_json VARCHAR NOT NULL,
    reason VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL,
    PRIMARY KEY (set_id, version, draft_revision)
);
CREATE TABLE feature_set_outputs (
    set_id VARCHAR NOT NULL,
    version INTEGER NOT NULL,
    item_id VARCHAR NOT NULL,
    output_id VARCHAR NOT NULL,
    feature_id VARCHAR NOT NULL,
    feature_version INTEGER NOT NULL,
    definition_hash VARCHAR NOT NULL,
    PRIMARY KEY (set_id, version, item_id, output_id)
);
CREATE INDEX feature_set_outputs_feature_idx
    ON feature_set_outputs(feature_id, feature_version);
""".strip()
_SCHEMA_V4_CHECKSUM = hashlib.sha256(
    f"{_SCHEMA_V3_CHECKSUM}\n{_MIGRATION_V4_SQL}".encode()
).hexdigest()
_MIGRATION_V5_SQL = """
ALTER TABLE runs ADD COLUMN research_study_id VARCHAR;
ALTER TABLE runs ADD COLUMN request_key VARCHAR;
ALTER TABLE runs ADD COLUMN request_hash VARCHAR;
ALTER TABLE runs ADD COLUMN plan_hash VARCHAR;
ALTER TABLE runs ADD COLUMN research_request_json VARCHAR;
ALTER TABLE runs ADD COLUMN research_plan_json VARCHAR;
ALTER TABLE runs ADD COLUMN research_summary_json VARCHAR;
ALTER TABLE runs ADD COLUMN created_by_json VARCHAR;
ALTER TABLE runs ADD COLUMN environment_json VARCHAR;
ALTER TABLE stage_runs ADD COLUMN work_key VARCHAR;
ALTER TABLE stage_runs ADD COLUMN research_role VARCHAR;
ALTER TABLE stage_runs ADD COLUMN definition_ref VARCHAR;
ALTER TABLE stage_runs ADD COLUMN partition_scope_json VARCHAR;
ALTER TABLE stage_runs ADD COLUMN required_history_json VARCHAR;
ALTER TABLE stage_runs ADD COLUMN dependency_work_keys_json VARCHAR;
ALTER TABLE stage_runs ADD COLUMN item_ids_json VARCHAR;
ALTER TABLE stage_runs ADD COLUMN blocked_reason VARCHAR;
CREATE UNIQUE INDEX research_run_idempotency_idx ON runs(research_study_id, request_key);
""".strip()
_SCHEMA_V5_CHECKSUM = hashlib.sha256(
    f"{_SCHEMA_V4_CHECKSUM}\n{_MIGRATION_V5_SQL}".encode()
).hexdigest()
_SCHEMA_CHECKSUMS = {
    1: _SCHEMA_V1_CHECKSUM,
    2: _SCHEMA_V2_CHECKSUM,
    3: _SCHEMA_V3_CHECKSUM,
    4: _SCHEMA_V4_CHECKSUM,
    5: _SCHEMA_V5_CHECKSUM,
}
_CURRENT_SCHEMA_VERSION = 5
_T = TypeVar("_T")


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


class _DatabaseOwner:
    """Own one DuckDB connection and execute every request on its owner thread."""

    def __init__(self, path: Path):
        self.path = path
        self.pid = os.getpid()
        self._queue: queue.Queue[tuple[Callable[[duckdb.DuckDBPyConnection], Any] | None,
                                      Future[Any]]] = queue.Queue()
        self._ready = threading.Event()
        self._startup_error: BaseException | None = None
        self._closed = False
        self._lifecycle_lock = threading.Lock()
        self._references = 0
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._thread = threading.Thread(
            target=self._serve, name=f"quantsage-duckdb-{path.stem}", daemon=True
        )
        # One active catalog per workspace, even if a caller changes its filename.
        lock_path = path.parent / ".quantsage-owner.lock"
        self._lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_CLOEXEC, 0o600)
        os.fchmod(self._lock_fd, 0o600)
        try:
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(self._lock_fd)
            raise WorkspaceBusyError(
                f"WORKSPACE_BUSY: another process owns registry workspace {path.parent}"
            ) from exc
        try:
            self._thread.start()
            self._ready.wait()
            if self._startup_error is not None:
                raise self._startup_error
            self.call(self._initialize)
        except BaseException:
            self._shutdown()
            raise

    def _serve(self) -> None:
        try:
            self._connection = duckdb.connect(str(self.path))
        except BaseException as exc:  # noqa: BLE001 — deliver owner startup errors to the caller
            self._startup_error = exc
            self._ready.set()
            return
        self._ready.set()
        while True:
            operation, future = self._queue.get()
            if operation is None:
                try:
                    self._connection.close()
                    future.set_result(None)
                except BaseException as exc:  # noqa: BLE001 — wake the waiting close caller
                    future.set_exception(exc)
                return
            try:
                self._connection.execute("BEGIN TRANSACTION")
                result = operation(self._connection)
                self._connection.execute("COMMIT")
                if operation == self._initialize:
                    # Persist schema ALTERs before dispatching work. DuckDB 1.5.5
                    # can fail replaying a new catalog's ALTER DEFAULT records
                    # after an abrupt process exit. Checkpoint only after the
                    # migration transaction commits, preserving rollback safety.
                    self._connection.execute("CHECKPOINT")
                future.set_result(result)
                del result
            except BaseException as exc:  # noqa: BLE001 — transfer all failures through the Future
                try:
                    self._connection.execute("ROLLBACK")
                except duckdb.Error:
                    pass
                future.set_exception(exc)
            finally:
                # Do not retain a callback's Registry handle while the queue is idle.
                del operation, future

    def call(self, operation: Callable[[duckdb.DuckDBPyConnection], _T]) -> _T:
        if os.getpid() != self.pid:
            raise WorkflowError(
                "Registry owner was inherited across fork; create a new Registry in the child process"
            )
        if threading.current_thread() is self._thread:
            raise WorkflowError("Registry operations cannot be nested on the owner thread")
        future: Future[_T] = Future()
        with self._lifecycle_lock:
            if self._closed:
                raise WorkflowError("Registry is closed")
            self._queue.put((operation, future))
        return future.result()

    def _initialize(self, connection: duckdb.DuckDBPyConnection) -> None:
        tables = {
            row[0] for row in connection.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
            ).fetchall()
        }
        if "schema_migrations" not in tables:
            if tables:
                names = ", ".join(sorted(tables))
                raise RegistrySchemaError(
                    "DuckDB file has ledger tables but no schema_migrations record; "
                    f"refusing unversioned database ({names})"
                )
            connection.execute(
                "CREATE TABLE schema_migrations ("
                "version INTEGER PRIMARY KEY, checksum VARCHAR NOT NULL, "
                "applied_at VARCHAR NOT NULL, duckdb_version VARCHAR NOT NULL)"
            )
            for statement in _SCHEMA_V1_DDL.split(";"):
                if statement.strip():
                    connection.execute(statement)
            self._record_schema_version(connection, 1)
            self._apply_migration_v2(connection)
            self._apply_migration_v3(connection)
            self._apply_migration_v4(connection)
            self._apply_migration_v5(connection)
            return

        rows = connection.execute(
            "SELECT version, checksum FROM schema_migrations ORDER BY version"
        ).fetchall()
        if not rows:
            raise RegistrySchemaError("DuckDB registry has an empty schema_migrations table")
        versions = [int(row[0]) for row in rows]
        current = versions[-1]
        if current > _CURRENT_SCHEMA_VERSION:
            raise RegistrySchemaError(f"unsupported DuckDB registry schema version: {current}")
        if versions != list(range(1, versions[-1] + 1)):
            raise RegistrySchemaError(f"DuckDB registry schema history is not contiguous: {versions}")
        for version, checksum in rows:
            version = int(version)
            expected = _SCHEMA_CHECKSUMS.get(version)
            if expected is None:
                raise RegistrySchemaError(f"unsupported DuckDB registry schema version: {version}")
            if checksum != expected:
                raise RegistrySchemaError(
                    f"DuckDB registry schema checksum mismatch for version {version}"
                )
        if current == 1:
            self._apply_migration_v2(connection)
            current = 2
        if current == 2:
            self._apply_migration_v3(connection)
            current = 3
        if current == 3:
            self._apply_migration_v4(connection)
            current = 4
        if current == 4:
            self._apply_migration_v5(connection)

    @staticmethod
    def _record_schema_version(connection: duckdb.DuckDBPyConnection, version: int) -> None:
        connection.execute(
            "INSERT INTO schema_migrations(version, checksum, applied_at, duckdb_version) "
            "VALUES (?, ?, ?, ?)",
            (version, _SCHEMA_CHECKSUMS[version], utc_now(), duckdb.__version__),
        )

    def _apply_migration_v2(self, connection: duckdb.DuckDBPyConnection) -> None:
        connection.execute(_MIGRATION_V2_SQL)
        self._record_schema_version(connection, 2)

    def _apply_migration_v3(self, connection: duckdb.DuckDBPyConnection) -> None:
        for statement in _MIGRATION_V3_SQL.split(";"):
            if statement.strip():
                connection.execute(statement)
        self._record_schema_version(connection, 3)

    def _apply_migration_v4(self, connection: duckdb.DuckDBPyConnection) -> None:
        for statement in _MIGRATION_V4_SQL.split(";"):
            if statement.strip():
                connection.execute(statement)
        self._record_schema_version(connection, 4)

    def _apply_migration_v5(self, connection: duckdb.DuckDBPyConnection) -> None:
        for statement in _MIGRATION_V5_SQL.split(";"):
            if statement.strip():
                connection.execute(statement)
        self._record_schema_version(connection, 5)

    def _shutdown(self) -> None:
        if self.pid != os.getpid():
            # The child cannot use or shut down a thread/connection copied by fork.
            if self._closed:
                return
            self._closed = True
            try:
                os.close(self._lock_fd)
            except OSError:
                pass
            return
        with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            future: Future[Any] = Future()
            running = self._thread.is_alive()
            if running:
                self._queue.put((None, future))
        try:
            if running:
                try:
                    future.result()
                finally:
                    self._thread.join()
        finally:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(self._lock_fd)


_OWNERS: dict[Path, _DatabaseOwner] = {}
_OWNERS_LOCK = threading.RLock()


def _release_owner(path: Path, owner: _DatabaseOwner) -> None:
    with _OWNERS_LOCK:
        if owner.pid != os.getpid():
            if _OWNERS.get(path) is owner:
                _OWNERS.pop(path, None)
            owner._shutdown()
            return
        owner._references -= 1
        if owner._references <= 0 and _OWNERS.get(path) is owner:
            _OWNERS.pop(path, None)
            owner._shutdown()


class Registry:
    """Persistent workflow ledger; a shared owner serializes all DB calls."""

    schema_version = _CURRENT_SCHEMA_VERSION

    def __init__(self, path: Path | str):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with _OWNERS_LOCK:
            owner = _OWNERS.get(self.path)
            if owner is not None and owner.pid != os.getpid():
                raise WorkflowError(
                    "Registry owner was inherited across fork; create a new Registry in the child process"
                )
            if owner is None:
                try:
                    owner = _DatabaseOwner(self.path)
                except duckdb.Error as exc:
                    raise RegistrySchemaError(f"Cannot open DuckDB registry {self.path}: {exc}") from exc
                _OWNERS[self.path] = owner
            owner._references += 1
        self._owner = owner
        self._closed = False
        self._finalizer = weakref.finalize(self, _release_owner, self.path, owner)

    def close(self) -> None:
        """Release this handle; the owner closes after its final handle is released."""
        if self._closed:
            return
        self._closed = True
        self._finalizer()

    def __enter__(self) -> Self:
        if self._closed:
            raise WorkflowError("Registry is closed")
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def _call(self, operation: Callable[[duckdb.DuckDBPyConnection], _T]) -> _T:
        if self._closed:
            raise WorkflowError("Registry is closed")
        return self._owner.call(operation)

    def create_run(self, *, run_id: str, config: Mapping[str, Any], config_hash: str,
                   data_snapshot_id: str | None, split_id: str | None,
                   as_of_cutoff: str | None, code_revision: str,
                   graph: Mapping[str, Any], execution: Mapping[str, Any] | None = None) -> None:
        timestamp = utc_now()

        def insert(connection: duckdb.DuckDBPyConnection) -> None:
            connection.execute(
                """INSERT INTO runs(run_id, config_json, config_hash, data_snapshot_id,
                   split_id, as_of_cutoff, code_revision, graph_json, execution_json,
                   status, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (run_id, canonical_json(config), config_hash, data_snapshot_id, split_id,
                 as_of_cutoff, code_revision, canonical_json(graph), canonical_json(execution or {}),
                 "RUNNING", timestamp, timestamp),
            )

        try:
            self._call(insert)
        except duckdb.ConstraintException as exc:
            raise WorkflowError(f"run_id already exists: {run_id}") from exc

    def find_research_run(self, study_id: str, request_key: str) -> dict[str, Any] | None:
        """Find a previously registered research Run by its study-local idempotency key."""
        row = self._call(lambda connection: _fetchone(
            connection,
            "SELECT * FROM runs WHERE research_study_id=? AND request_key=?",
            (study_id, request_key),
        ))
        return self._decode_run_row(row) if row else None

    def register_research_run(
        self, *, run_id: str, study_id: str, study_revision: int, request_key: str,
        request_hash: str, plan_hash: str, request: Mapping[str, Any],
        config_snapshot: Mapping[str, Any], config_hash: str, plan: Mapping[str, Any],
        summary: Mapping[str, Any], created_by: Mapping[str, str], split_id: str,
        code_revision: str, environment: Mapping[str, Any], work_items: list[Mapping[str, Any]],
    ) -> dict[str, Any]:
        """Atomically register one immutable research plan and all of its initial tasks.

        The owner queue serializes this operation with Study updates and other submissions.
        Existing `(study_id, request_key)` entries win idempotently before current-revision
        checks, so retries never re-plan an already registered request.
        """
        timestamp = utc_now()
        if plan.get("status") == "NO_WORK":
            status = "NO_WORK"
        elif plan.get("status") == "BLOCKED":
            status = "BLOCKED"
        elif not work_items:
            status = "NO_WORK"
        else:
            status = "NOT_STARTED"
        graph = {"research_run_plan": {
            "schema_version": plan.get("schema_version"),
            "plan_hash": plan_hash,
            "work": list(work_items),
            "dependencies": list(plan.get("dependencies", [])),
        }}

        def register(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
            existing = _fetchone(
                connection,
                "SELECT run_id,request_hash FROM runs WHERE research_study_id=? AND request_key=?",
                (study_id, request_key),
            )
            if existing is not None:
                if existing["request_hash"] != request_hash:
                    return {"outcome": "idempotency_conflict", "run_id": existing["run_id"]}
                return {"outcome": "existing", "run_id": existing["run_id"]}

            study = _fetchone(
                connection,
                "SELECT s.current_revision,r.document_json FROM studies s JOIN study_revisions r "
                "ON r.study_id=s.study_id AND r.revision=s.current_revision WHERE s.study_id=?",
                (study_id,),
            )
            if study is None:
                return {"outcome": "unknown_study", "run_id": None}
            current_document = json.loads(study["document_json"])
            if int(study["current_revision"]) != study_revision:
                return {"outcome": "revision_conflict", "run_id": None,
                        "current_revision": int(study["current_revision"])}
            if current_document.get("status") != "active":
                return {"outcome": "study_not_active", "run_id": None,
                        "status": current_document.get("status")}

            connection.execute(
                """INSERT INTO runs(
                    run_id,config_json,config_hash,data_snapshot_id,split_id,as_of_cutoff,
                    code_revision,graph_json,execution_json,status,created_at,updated_at,
                    research_study_id,request_key,request_hash,plan_hash,research_request_json,
                    research_plan_json,research_summary_json,created_by_json,environment_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (run_id, canonical_json(config_snapshot), config_hash, None, split_id, None,
                 code_revision, canonical_json(graph), canonical_json(environment), status,
                 timestamp, timestamp, study_id, request_key, request_hash, plan_hash,
                 canonical_json(request), canonical_json(plan), canonical_json(summary),
                 canonical_json(created_by), canonical_json(environment)),
            )
            for item in work_items:
                initial_state = item["state"]
                error = None
                if initial_state == StageStatus.BLOCKED.value:
                    error = canonical_json({"code": item.get("blocked_reason"),
                                            "message": item.get("blocked_reason")})
                partition = item["partition_scope"]
                connection.execute(
                    """INSERT INTO stage_runs(
                        stage_run_id,run_id,stage_name,stage_version,partition_json,status,
                        cache_key,config_hash,code_revision,error_json,work_key,research_role,
                        definition_ref,partition_scope_json,required_history_json,
                        dependency_work_keys_json,item_ids_json,blocked_reason
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (item["stage_run_id"], run_id, item["stage_name"], item["stage_version"],
                     canonical_json({key: value for key, value in partition.items()
                                     if key in {"exchange", "instrument_id", "trade_date", "session_id"}}),
                     initial_state, None, request_hash, code_revision, error,
                     item["work_key"], item["role"], item["feature_ref"],
                     canonical_json(partition), canonical_json(item["required_history"]),
                     canonical_json(item["dependency_work_keys"]),
                     canonical_json(item["item_ids"]), item.get("blocked_reason")),
                )
                connection.execute(
                    "INSERT INTO stage_state_events(stage_run_id,state,attempt,fields_json,created_at) "
                    "VALUES(?,?,?,?,?)",
                    (item["stage_run_id"], initial_state, 0,
                     canonical_json({"work_key": item["work_key"],
                                    "blocked_reason": item.get("blocked_reason")}), timestamp),
                )
            return {"outcome": "created", "run_id": run_id}

        try:
            return self._call(register)
        except duckdb.ConstraintException as exc:
            raise WorkflowError("research Run registration violated a Registry uniqueness constraint") from exc

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        def select(connection: duckdb.DuckDBPyConnection):
            return _fetchone(connection, "SELECT * FROM runs WHERE run_id=?", (run_id,))

        row = self._call(select)
        return self._decode_run_row(row) if row else None

    @staticmethod
    def _decode_run_row(row: Mapping[str, Any]) -> dict[str, Any]:
        result = dict(row)
        for key in ("config_json", "graph_json", "execution_json", "research_request_json",
                    "research_plan_json", "research_summary_json", "created_by_json",
                    "environment_json"):
            if key in result and result[key] is not None:
                result[key.removesuffix("_json")] = json.loads(result[key])
        return result

    def finish_run(self, run_id: str, status: str) -> None:
        def update(connection: duckdb.DuckDBPyConnection) -> None:
            connection.execute(
                "UPDATE runs SET status=?, updated_at=? WHERE run_id=?",
                (status, utc_now(), run_id),
            )

        self._call(update)

    def ensure_stage_run(self, *, run_id: str, stage_name: str, stage_version: str,
                         partition: Mapping[str, str], stage_run_id: str,
                         cache_key: str | None, config_hash: str | None,
                         code_revision: str | None, force_new: bool = False) -> dict[str, Any]:
        partition_json = canonical_json(dict(partition))

        def ensure(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
            if _fetchone(connection, "SELECT run_id FROM runs WHERE run_id=?", (run_id,)) is None:
                raise WorkflowError(f"unknown run_id: {run_id}")
            row = _fetchone(
                connection,
                """SELECT * FROM stage_runs WHERE run_id=? AND stage_name=? AND partition_json=?
                   ORDER BY registry_order DESC LIMIT 1""",
                (run_id, stage_name, partition_json),
            )
            if row is None or force_new:
                connection.execute(
                    """INSERT INTO stage_runs(stage_run_id, run_id, stage_name, stage_version,
                       partition_json, status, cache_key, config_hash, code_revision)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (stage_run_id, run_id, stage_name, stage_version, partition_json,
                     StageStatus.PENDING.value, cache_key, config_hash, code_revision),
                )
                row = _fetchone(connection,
                                "SELECT * FROM stage_runs WHERE stage_run_id=?", (stage_run_id,))
            return Registry._decode_stage_row(row)

        return self._call(ensure)

    def get_stage_run(self, run_id: str, stage_name: str,
                      partition: Mapping[str, str]) -> dict[str, Any] | None:
        row = self._call(lambda connection: _fetchone(
            connection,
            """SELECT * FROM stage_runs WHERE run_id=? AND stage_name=? AND partition_json=?
               ORDER BY registry_order DESC LIMIT 1""",
            (run_id, stage_name, canonical_json(dict(partition))),
        ))
        return self._decode_stage_row(row) if row else None

    @staticmethod
    def _decode_stage_row(row: Mapping[str, Any]) -> dict[str, Any]:
        result = dict(row)
        result.pop("registry_order", None)
        for key in ("partition_json", "inputs_json", "artifacts_json", "metrics_json",
                    "warnings_json", "partition_scope_json", "required_history_json",
                    "dependency_work_keys_json", "item_ids_json"):
            if key not in result or result[key] is None:
                continue
            result[key.removesuffix("_json")] = json.loads(result[key])
        result["error"] = json.loads(result["error_json"]) if result.get("error_json") else None
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
            artifacts_json=canonical_json({key: ref.to_dict()
                                           for key, ref in sorted((artifacts or {}).items())}),
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

    def set_stage_status(self, stage_run_id: str, status: StageStatus,
                         *, error: Mapping[str, Any] | None = None) -> None:
        self._transition(stage_run_id, status,
                         error_json=canonical_json(error) if error else None,
                         finished_at=utc_now() if status in {StageStatus.SKIPPED,
                                                             StageStatus.BLOCKED} else None)

    def _transition(self, stage_run_id: str, status: StageStatus, **fields: Any) -> int:
        def transition(connection: duckdb.DuckDBPyConnection) -> int:
            row = _fetchone(connection,
                            "SELECT status, attempt FROM stage_runs WHERE stage_run_id=?",
                            (stage_run_id,))
            if row is None:
                raise WorkflowError(f"unknown stage_run_id: {stage_run_id}")
            current = StageStatus(row["status"])
            if status not in _TRANSITIONS[current]:
                raise WorkflowError(
                    f"invalid stage status transition {current.value} -> {status.value}"
                )
            next_attempt = int(row["attempt"])
            if fields.pop("attempt_increment", 0):
                next_attempt += 1
                fields["attempt"] = next_attempt
            fields["status"] = status.value
            assignments = ", ".join(f"{key}=?" for key in fields)
            connection.execute(
                f"UPDATE stage_runs SET {assignments} WHERE stage_run_id=?",
                [*fields.values(), stage_run_id],
            )
            connection.execute(
                "INSERT INTO stage_state_events(stage_run_id,state,attempt,fields_json,created_at) "
                "VALUES(?,?,?,?,?)",
                (stage_run_id, status.value, next_attempt, canonical_json(fields), utc_now()),
            )
            return next_attempt

        return self._call(transition)

    def get_stage_run_by_id(self, stage_run_id: str) -> dict[str, Any] | None:
        row = self._call(lambda connection: _fetchone(
            connection, "SELECT * FROM stage_runs WHERE stage_run_id=?", (stage_run_id,)
        ))
        return self._decode_stage_row(row) if row else None

    def get_stage_events(self, stage_run_id: str) -> list[dict[str, Any]]:
        rows = self._call(lambda connection: _fetchall(
            connection,
            "SELECT * FROM stage_state_events WHERE stage_run_id=? ORDER BY event_id",
            (stage_run_id,),
        ))
        return [{**row, "fields": json.loads(row["fields_json"])} for row in rows]

    def find_cache(self, stage_name: str, stage_version: str, cache_key: str) -> dict[str, Any] | None:
        row = self._call(lambda connection: _fetchone(
            connection,
            """SELECT * FROM stage_runs WHERE stage_name=? AND stage_version=? AND cache_key=?
               AND cache_valid=1 AND status='SUCCEEDED' ORDER BY finished_at DESC, registry_order DESC
               LIMIT 1""",
            (stage_name, stage_version, cache_key),
        ))
        return self._decode_stage_row(row) if row else None

    def invalidate_cache(self, stage_names: set[str]) -> int:
        if not stage_names:
            return 0
        placeholders = ",".join("?" for _ in stage_names)

        def invalidate(connection: duckdb.DuckDBPyConnection) -> int:
            rows = connection.execute(
                f"UPDATE stage_runs SET cache_valid=0 WHERE stage_name IN ({placeholders}) "
                "RETURNING stage_run_id",
                sorted(stage_names),
            ).fetchall()
            return len(rows)

        return self._call(invalidate)

    def invalidate_stage_run(self, stage_run_id: str) -> None:
        def invalidate(connection: duckdb.DuckDBPyConnection) -> None:
            connection.execute(
                "UPDATE stage_runs SET cache_valid=0 WHERE stage_run_id=?", (stage_run_id,)
            )

        self._call(invalidate)

    def get_run_stages(self, run_id: str) -> list[dict[str, Any]]:
        rows = self._call(lambda connection: _fetchall(
            connection,
            "SELECT * FROM stage_runs WHERE run_id=? ORDER BY stage_name, partition_json, registry_order",
            (run_id,),
        ))
        return [self._decode_stage_row(row) for row in rows]

    def explain_artifact(self, artifact_id: str) -> list[dict[str, Any]]:
        rows = self._call(lambda connection: _fetchall(
            connection, "SELECT * FROM stage_runs WHERE status IN ('SUCCEEDED','CACHED') "
                        "ORDER BY registry_order"
        ))
        matches = []
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


def _fetchone(connection: duckdb.DuckDBPyConnection, sql: str,
              params: tuple[Any, ...] | list[Any] = ()) -> dict[str, Any] | None:
    cursor = connection.execute(sql, params)
    row = cursor.fetchone()
    if row is None:
        return None
    return {description[0]: value for description, value in zip(cursor.description, row, strict=True)}


def _fetchall(connection: duckdb.DuckDBPyConnection, sql: str,
              params: tuple[Any, ...] | list[Any] = ()) -> list[dict[str, Any]]:
    cursor = connection.execute(sql, params)
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def new_stage_run_id() -> str:
    return "stage_run_" + uuid.uuid4().hex
