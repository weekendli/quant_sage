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
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Self, TypeVar

import duckdb

from .artifacts import artifact_path
from .errors import (
    RegistrySchemaError,
    RunDispatchQueueFullError,
    WorkflowError,
    WorkspaceBusyError,
)
from .types import ArtifactRef, StageStatus
from .utils import canonical_json


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


_TRANSITIONS = {
    StageStatus.PENDING: {StageStatus.RUNNING, StageStatus.CACHED, StageStatus.SKIPPED,
                          StageStatus.BLOCKED},
    StageStatus.RUNNING: {StageStatus.RUNNING, StageStatus.SUCCEEDED, StageStatus.CACHED,
                          StageStatus.FAILED, StageStatus.BLOCKED, StageStatus.PENDING},
    StageStatus.FAILED: {StageStatus.RUNNING, StageStatus.SUCCEEDED, StageStatus.CACHED,
                         StageStatus.BLOCKED, StageStatus.SKIPPED},
    StageStatus.BLOCKED: {StageStatus.RUNNING, StageStatus.CACHED, StageStatus.SKIPPED},
    StageStatus.SKIPPED: {StageStatus.RUNNING, StageStatus.CACHED},
    StageStatus.CACHED: {StageStatus.RUNNING, StageStatus.BLOCKED, StageStatus.PENDING},
    StageStatus.SUCCEEDED: {StageStatus.RUNNING, StageStatus.BLOCKED, StageStatus.PENDING},
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
_MIGRATION_V6_SQL = """
CREATE SEQUENCE feature_result_link_order_seq START 1;
CREATE SEQUENCE feature_result_health_order_seq START 1;
CREATE TABLE feature_results (
    result_id VARCHAR PRIMARY KEY,
    feature_ref VARCHAR NOT NULL,
    definition_hash VARCHAR NOT NULL,
    source_ref VARCHAR NOT NULL,
    split_id VARCHAR NOT NULL,
    scope_json VARCHAR NOT NULL,
    required_history_json VARCHAR NOT NULL,
    computation_fingerprint VARCHAR NOT NULL,
    producer_run_id VARCHAR NOT NULL,
    producer_work_key VARCHAR NOT NULL,
    result_json VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL
);
CREATE INDEX feature_result_cache_idx ON feature_results(
    feature_ref, definition_hash, source_ref, split_id, computation_fingerprint
);
CREATE TABLE run_result_links (
    link_id BIGINT PRIMARY KEY DEFAULT nextval('feature_result_link_order_seq'),
    run_id VARCHAR NOT NULL,
    work_key VARCHAR NOT NULL,
    result_id VARCHAR NOT NULL,
    relation VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL,
    UNIQUE(run_id, work_key, result_id)
);
CREATE INDEX run_result_link_run_idx ON run_result_links(run_id, work_key, link_id);
CREATE INDEX run_result_link_result_idx ON run_result_links(result_id);
CREATE TABLE feature_result_health (
    health_id BIGINT PRIMARY KEY DEFAULT nextval('feature_result_health_order_seq'),
    result_id VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    reason VARCHAR,
    checked_at VARCHAR NOT NULL,
    details_json VARCHAR NOT NULL
);
CREATE INDEX feature_result_health_result_idx ON feature_result_health(result_id, health_id);
""".strip()
_SCHEMA_V6_CHECKSUM = hashlib.sha256(
    f"{_SCHEMA_V5_CHECKSUM}\n{_MIGRATION_V6_SQL}".encode()
).hexdigest()
_MIGRATION_V7_SQL = """
CREATE SEQUENCE research_review_order_seq START 1;
CREATE TABLE reviews (
    review_order BIGINT PRIMARY KEY DEFAULT nextval('research_review_order_seq'),
    review_id VARCHAR NOT NULL UNIQUE,
    study_id VARCHAR NOT NULL,
    run_refs_json VARCHAR NOT NULL,
    result_refs_json VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL,
    document_json VARCHAR NOT NULL
);
CREATE INDEX research_review_study_idx ON reviews(study_id, review_order);
""".strip()
_SCHEMA_V7_CHECKSUM = hashlib.sha256(
    f"{_SCHEMA_V6_CHECKSUM}\n{_MIGRATION_V7_SQL}".encode()
).hexdigest()
_MIGRATION_V8_SQL = """
CREATE TABLE research_dispatch_queue (
    run_id VARCHAR PRIMARY KEY,
    status VARCHAR NOT NULL,
    requested_at VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    error_json VARCHAR
);
CREATE INDEX research_dispatch_state_idx
    ON research_dispatch_queue(status, requested_at, run_id);
""".strip()
_SCHEMA_V8_CHECKSUM = hashlib.sha256(
    f"{_SCHEMA_V7_CHECKSUM}\n{_MIGRATION_V8_SQL}".encode()
).hexdigest()
_SCHEMA_CHECKSUMS = {
    1: _SCHEMA_V1_CHECKSUM,
    2: _SCHEMA_V2_CHECKSUM,
    3: _SCHEMA_V3_CHECKSUM,
    4: _SCHEMA_V4_CHECKSUM,
    5: _SCHEMA_V5_CHECKSUM,
    6: _SCHEMA_V6_CHECKSUM,
    7: _SCHEMA_V7_CHECKSUM,
    8: _SCHEMA_V8_CHECKSUM,
}
_CURRENT_SCHEMA_VERSION = 8
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
            self._apply_migration_v6(connection)
            self._apply_migration_v7(connection)
            self._apply_migration_v8(connection)
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
            current = 5
        if current == 5:
            self._apply_migration_v6(connection)
            current = 6
        if current == 6:
            self._apply_migration_v7(connection)
            current = 7
        if current == 7:
            self._apply_migration_v8(connection)

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

    def _apply_migration_v6(self, connection: duckdb.DuckDBPyConnection) -> None:
        for statement in _MIGRATION_V6_SQL.split(";"):
            if statement.strip():
                connection.execute(statement)
        self._record_schema_version(connection, 6)

    def _apply_migration_v7(self, connection: duckdb.DuckDBPyConnection) -> None:
        for statement in _MIGRATION_V7_SQL.split(";"):
            if statement.strip():
                connection.execute(statement)
        self._record_schema_version(connection, 7)

    def _apply_migration_v8(self, connection: duckdb.DuckDBPyConnection) -> None:
        for statement in _MIGRATION_V8_SQL.split(";"):
            if statement.strip():
                connection.execute(statement)
        self._record_schema_version(connection, 8)

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

    def claim_research_work(self, stage_run_id: str, *, computation_fingerprint: str,
                            config_hash: str, code_revision: str,
                            inputs: Mapping[str, ArtifactRef]) -> dict[str, Any]:
        """Atomically claim one pending work row so concurrent executors cannot duplicate it."""
        timestamp = utc_now()
        inputs_json = canonical_json({key: ref.to_dict() for key, ref in sorted(inputs.items())})

        def claim(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
            row = _fetchone(connection,
                            "SELECT status,attempt FROM stage_runs WHERE stage_run_id=?",
                            (stage_run_id,))
            if row is None:
                raise WorkflowError(f"unknown stage_run_id: {stage_run_id}")
            current = StageStatus(row["status"])
            if current not in {StageStatus.PENDING, StageStatus.FAILED, StageStatus.SKIPPED}:
                return {"claimed": False, "status": current.value,
                        "attempt": int(row["attempt"])}
            attempt = int(row["attempt"]) + 1
            fields = {
                "status": StageStatus.RUNNING.value,
                "cache_key": computation_fingerprint,
                "config_hash": config_hash,
                "code_revision": code_revision,
                "inputs_json": inputs_json,
                "started_at": timestamp,
                "finished_at": None,
                "duration_ms": None,
                "error_json": None,
                "attempt": attempt,
            }
            assignments = ", ".join(f"{key}=?" for key in fields)
            connection.execute(f"UPDATE stage_runs SET {assignments} WHERE stage_run_id=?",
                               [*fields.values(), stage_run_id])
            connection.execute(
                "INSERT INTO stage_state_events(stage_run_id,state,attempt,fields_json,created_at) "
                "VALUES(?,?,?,?,?)",
                (stage_run_id, StageStatus.RUNNING.value, attempt,
                 canonical_json(fields), timestamp),
            )
            return {"claimed": True, "status": StageStatus.RUNNING.value, "attempt": attempt}

        return self._call(claim)

    def set_research_work_pending(self, stage_run_id: str, *, reason: str) -> None:
        """Reconcile work: preserve interruption as a failed attempt, requeue invalid results."""
        timestamp = utc_now()

        def reconcile(connection: duckdb.DuckDBPyConnection) -> None:
            row = _fetchone(connection,
                            "SELECT status,attempt FROM stage_runs WHERE stage_run_id=?",
                            (stage_run_id,))
            if row is None:
                raise WorkflowError(f"unknown stage_run_id: {stage_run_id}")
            current = StageStatus(row["status"])
            if current == StageStatus.RUNNING:
                status = StageStatus.FAILED
                error = {"code": "INTERRUPTED", "message": reason}
                event_attempt = int(row["attempt"])
            elif current in {StageStatus.SUCCEEDED, StageStatus.CACHED}:
                status = StageStatus.PENDING
                error = {"code": "RESULT_INVALIDATED", "message": reason}
                # This event requeues work without rewriting its completed attempt history.
                event_attempt = 0
            else:
                return
            connection.execute(
                "UPDATE stage_runs SET status=?,finished_at=?,error_json=? WHERE stage_run_id=?",
                (status.value, timestamp, canonical_json(error), stage_run_id),
            )
            connection.execute(
                "INSERT INTO stage_state_events(stage_run_id,state,attempt,fields_json,created_at) "
                "VALUES(?,?,?,?,?)",
                (stage_run_id, status.value, event_attempt,
                 canonical_json({**error, "requeued": status == StageStatus.PENDING}), timestamp),
            )

        self._call(reconcile)

    @staticmethod
    def _research_plan_work(connection: duckdb.DuckDBPyConnection, run_id: str,
                            work_key: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        row = _fetchone(
            connection,
            "SELECT research_study_id,research_plan_json FROM runs WHERE run_id=?",
            (run_id,),
        )
        if row is None or row["research_study_id"] is None or not row["research_plan_json"]:
            raise WorkflowError(f"run has no frozen research plan: {run_id}")
        plan = json.loads(row["research_plan_json"])
        works = plan.get("work")
        if not isinstance(works, list):
            raise WorkflowError(f"research Run plan has no work list: {run_id}")
        matching = [item for item in works
                    if isinstance(item, dict) and item.get("work_key") == work_key]
        if len(matching) != 1:
            raise WorkflowError(
                f"frozen research plan does not identify exactly one work item: {work_key}"
            )
        work_by_key = {
            item["work_key"]: item for item in works
            if isinstance(item, dict) and isinstance(item.get("work_key"), str)
        }
        if len(work_by_key) != len(works):
            raise WorkflowError(f"research Run plan has missing or duplicate work keys: {run_id}")
        return plan, matching[0], work_by_key

    @staticmethod
    def _assert_feature_result_identity(result: Mapping[str, Any], plan: Mapping[str, Any],
                                        work: Mapping[str, Any], *, producer_run_id: str,
                                        producer_work_key: str,
                                        producer_stage_run_id: str | None = None) -> None:
        expected = {
            "feature_ref": work.get("feature_ref"),
            "definition_hash": work.get("definition_hash"),
            "source_ref": work.get("source_ref"),
            "scope": work.get("partition_scope"),
            "required_history": work.get("required_history"),
            "source_refs": [work.get("source_ref")],
            "producer_run_id": producer_run_id,
            "producer_work_key": producer_work_key,
            "producer_work_id": producer_work_key,
            "split_ref": {
                "split_id": plan.get("split_id"),
                **(plan.get("split_ref") or {}),
            },
        }
        for field, wanted in expected.items():
            if canonical_json(result.get(field)) != canonical_json(wanted):
                raise WorkflowError(
                    f"FeatureResult {field} does not match its frozen Run work identity"
                )
        if (producer_stage_run_id is not None
                and result.get("producer_stage_run_id") != producer_stage_run_id):
            raise WorkflowError("FeatureResult producer stage does not match the registered work")

    @staticmethod
    def _assert_feature_result_artifacts(result: Mapping[str, Any],
                                         artifacts: Mapping[str, ArtifactRef]) -> None:
        descriptors = result.get("artifacts")
        if not isinstance(descriptors, list) or not descriptors or not artifacts:
            raise WorkflowError("a successful FeatureResult must reference at least one artifact")
        try:
            described = []
            for descriptor in descriptors:
                if not isinstance(descriptor, Mapping):
                    raise TypeError("artifact descriptor must be an object")
                artifact_ref = descriptor.get("artifact_ref")
                if not isinstance(artifact_ref, Mapping):
                    raise TypeError("artifact_ref must be an object")
                ref = ArtifactRef.from_dict(artifact_ref)
                if (descriptor.get("hash") != ref.content_hash
                        or descriptor.get("schema") != ref.schema_version):
                    raise ValueError("artifact descriptor hash/schema disagrees with its ref")
                described.append(canonical_json(ref.to_dict()))
            if any(not isinstance(ref, ArtifactRef) for ref in artifacts.values()):
                raise TypeError("published artifacts must be ArtifactRef values")
            supplied = [canonical_json(ref.to_dict()) for ref in artifacts.values()]
        except (TypeError, ValueError) as exc:
            raise WorkflowError(f"FeatureResult artifact references are invalid: {exc}") from exc
        if sorted(described) != sorted(supplied):
            raise WorkflowError("FeatureResult artifacts do not match the published artifact refs")

    @staticmethod
    def _assert_feature_result_inputs(
        connection: duckdb.DuckDBPyConnection, result: Mapping[str, Any],
        plan: Mapping[str, Any], work: Mapping[str, Any],
        work_by_key: Mapping[str, Mapping[str, Any]], *, producer_run_id: str,
    ) -> None:
        input_refs = result.get("input_result_refs")
        dependency_keys = work.get("dependency_work_keys")
        if (not isinstance(input_refs, list)
                or any(not isinstance(ref, str) or not ref for ref in input_refs)
                or not isinstance(dependency_keys, list)
                or any(not isinstance(key, str) for key in dependency_keys)
                or len(set(dependency_keys)) != len(dependency_keys)):
            raise WorkflowError("FeatureResult inputs do not match a valid frozen dependency list")
        if len(input_refs) != len(dependency_keys):
            raise WorkflowError("FeatureResult input refs do not cover every frozen dependency")

        links_by_dependency: dict[str, set[str]] = {}
        for dependency_key in dependency_keys:
            dependency = work_by_key.get(dependency_key)
            if dependency is None:
                raise WorkflowError("frozen dependency work key is absent from its Run plan")
            rows = _fetchall(
                connection,
                "SELECT result_id FROM run_result_links WHERE run_id=? AND work_key=?",
                (producer_run_id, dependency_key),
            )
            linked_ids = {row["result_id"] for row in rows}
            if not linked_ids:
                raise WorkflowError("FeatureResult dependency has no registered result link")
            links_by_dependency[dependency_key] = linked_ids

        # Match each result ref to one distinct planned dependency link. This also
        # rejects a real result link belonging only to an unrelated work item.
        ref_to_dependency: dict[int, str] = {}

        def match(dependency_key: str, visited_refs: set[int]) -> bool:
            for index, result_id in enumerate(input_refs):
                if index in visited_refs or result_id not in links_by_dependency[dependency_key]:
                    continue
                visited_refs.add(index)
                previous = ref_to_dependency.get(index)
                if previous is None or match(previous, visited_refs):
                    ref_to_dependency[index] = dependency_key
                    return True
            return False

        for dependency_key in dependency_keys:
            if not match(dependency_key, set()):
                raise WorkflowError("FeatureResult input refs do not match frozen dependency links")
        if len(ref_to_dependency) != len(input_refs):
            raise WorkflowError("FeatureResult contains refs outside its frozen dependency links")

        for index, dependency_key in ref_to_dependency.items():
            row = _fetchone(
                connection,
                "SELECT result_json FROM feature_results WHERE result_id=?",
                (input_refs[index],),
            )
            if row is None:
                raise WorkflowError("FeatureResult input ref is not a registered FeatureResult")
            dependency_result = json.loads(row["result_json"])
            dependency_work = work_by_key[dependency_key]
            Registry._assert_feature_result_identity(
                dependency_result, plan, dependency_work,
                producer_run_id=dependency_result.get("producer_run_id"),
                producer_work_key=dependency_result.get("producer_work_key"),
            )

    def commit_research_result(self, *, run_id: str, work_key: str,
                               stage_run_id: str, result: Mapping[str, Any],
                               artifacts: Mapping[str, ArtifactRef],
                               metrics: Mapping[str, Any], receipt_id: str,
                               attempt: int) -> dict[str, Any]:
        """Atomically register a FeatureResult, its Run link, success state, and event."""
        result_doc = dict(result)
        required = {
            "result_id", "feature_ref", "definition_hash", "source_ref", "split_ref",
            "scope", "processed_scope", "required_history", "input_result_refs",
            "producer_run_id", "producer_work_key", "computation_fingerprint",
            "producer_work_id", "source_refs", "artifacts", "coverage", "quality",
            "time_metadata",
        }
        missing = required - result_doc.keys()
        if missing:
            raise WorkflowError("FeatureResult is missing fields: " + ", ".join(sorted(missing)))
        if not isinstance(result_doc["result_id"], str) or not result_doc["result_id"]:
            raise WorkflowError("FeatureResult result_id must be a non-empty string")
        self._assert_feature_result_artifacts(result_doc, artifacts)
        if result_doc["producer_run_id"] != run_id or result_doc["producer_work_key"] != work_key:
            raise WorkflowError("FeatureResult producer does not match the committing Run work")
        if result_doc["producer_work_id"] != work_key or result_doc["source_refs"] != [
            result_doc["source_ref"]
        ]:
            raise WorkflowError("FeatureResult canonical producer/source refs do not match")
        if type(attempt) is not int or attempt < 1:
            raise WorkflowError("publication attempt must be a positive integer")
        try:
            result_json = canonical_json(result_doc)
        except (TypeError, ValueError) as exc:
            raise WorkflowError(f"FeatureResult is not JSON serializable: {exc}") from exc
        artifacts_json = canonical_json({key: ref.to_dict()
                                         for key, ref in sorted(artifacts.items())})
        metrics_json = canonical_json(metrics)
        timestamp = utc_now()

        def commit(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
            stage = _fetchone(connection,
                              "SELECT status,attempt,run_id,work_key,stage_run_id,cache_key "
                              "FROM stage_runs "
                              "WHERE stage_run_id=?", (stage_run_id,))
            if stage is None or stage["run_id"] != run_id or stage["work_key"] != work_key:
                raise WorkflowError("publication receipt does not match a registered Run work row")
            plan, work, work_by_key = self._research_plan_work(connection, run_id, work_key)
            self._assert_feature_result_identity(
                result_doc, plan, work, producer_run_id=run_id,
                producer_work_key=work_key, producer_stage_run_id=stage_run_id,
            )
            self._assert_feature_result_inputs(
                connection, result_doc, plan, work, work_by_key,
                producer_run_id=run_id,
            )
            existing_result = _fetchone(connection,
                                        "SELECT result_json FROM feature_results WHERE result_id=?",
                                        (result_doc["result_id"],))
            if existing_result is not None and existing_result["result_json"] != result_json:
                raise WorkflowError("FeatureResult id collision with different immutable content")
            linked = _fetchone(
                connection,
                "SELECT link_id FROM run_result_links WHERE run_id=? AND work_key=? AND result_id=?",
                (run_id, work_key, result_doc["result_id"]),
            )
            if linked is not None:
                return {"outcome": "existing", "result_id": result_doc["result_id"]}
            if stage["cache_key"] != result_doc["computation_fingerprint"]:
                raise WorkflowError(
                    "FeatureResult computation fingerprint does not match the claimed work"
                )
            if int(stage["attempt"]) != attempt:
                raise WorkflowError(
                    "publication receipt attempt is stale for the registered work row"
                )
            current = StageStatus(stage["status"])
            if current == StageStatus.SUCCEEDED:
                raise WorkflowError(
                    "a successful attempt cannot publish a different FeatureResult"
                )
            if current not in {StageStatus.RUNNING, StageStatus.FAILED}:
                raise WorkflowError(
                    f"cannot commit published result from work state {current.value}"
                )
            if existing_result is None:
                connection.execute(
                    "INSERT INTO feature_results(result_id,feature_ref,definition_hash,source_ref,"
                    "split_id,scope_json,required_history_json,computation_fingerprint,"
                    "producer_run_id,producer_work_key,result_json,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (result_doc["result_id"], result_doc["feature_ref"],
                     result_doc["definition_hash"], result_doc["source_ref"],
                     result_doc["split_ref"]["split_id"], canonical_json(result_doc["scope"]),
                     canonical_json(result_doc["required_history"]),
                     result_doc["computation_fingerprint"], run_id, work_key,
                     result_json, timestamp),
                )
            connection.execute(
                "INSERT INTO run_result_links(run_id,work_key,result_id,relation,created_at) "
                "VALUES(?,?,?,?,?)",
                (run_id, work_key, result_doc["result_id"], "PRODUCED", timestamp),
            )
            fields = {
                "status": StageStatus.SUCCEEDED.value,
                "artifacts_json": artifacts_json,
                "metrics_json": metrics_json,
                "warnings_json": canonical_json([]),
                "finished_at": timestamp,
                "error_json": None,
                "source_stage_run_id": None,
            }
            assignments = ", ".join(f"{key}=?" for key in fields)
            connection.execute(f"UPDATE stage_runs SET {assignments} WHERE stage_run_id=?",
                               [*fields.values(), stage_run_id])
            connection.execute(
                "INSERT INTO stage_state_events(stage_run_id,state,attempt,fields_json,created_at) "
                "VALUES(?,?,?,?,?)",
                (stage_run_id, StageStatus.SUCCEEDED.value, int(stage["attempt"]),
                 canonical_json({**fields, "receipt_id": receipt_id,
                                "result_id": result_doc["result_id"]}), timestamp),
            )
            return {"outcome": "created", "result_id": result_doc["result_id"]}

        try:
            return self._call(commit)
        except duckdb.ConstraintException as exc:
            raise WorkflowError("FeatureResult publication violated a Registry uniqueness constraint") from exc

    def link_cached_feature_result(self, *, run_id: str, work_key: str,
                                   stage_run_id: str, result: Mapping[str, Any],
                                   artifacts: Mapping[str, ArtifactRef],
                                   metrics: Mapping[str, Any],
                                   cache_source_run_id: str) -> dict[str, Any]:
        """Link a previously produced, verified result without changing its producer."""
        timestamp = utc_now()
        result_id = result.get("result_id")
        if not isinstance(result_id, str) or not result_id:
            raise WorkflowError("cached FeatureResult requires a non-empty result_id")

        def link(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
            stage = _fetchone(connection,
                              "SELECT status,attempt,run_id,work_key FROM stage_runs "
                              "WHERE stage_run_id=?", (stage_run_id,))
            if stage is None or stage["run_id"] != run_id or stage["work_key"] != work_key:
                raise WorkflowError("cached result does not match a registered Run work row")
            stored = _fetchone(connection,
                               "SELECT feature_ref,definition_hash,source_ref,split_id,scope_json,"
                               "required_history_json,computation_fingerprint,producer_run_id,"
                               "producer_work_key,result_json FROM feature_results WHERE result_id=?",
                               (result_id,))
            if stored is None:
                raise WorkflowError("cannot link an unregistered FeatureResult as cached")
            result_doc = json.loads(stored["result_json"])
            if (result_doc.get("result_id") != result_id
                    or result_doc.get("producer_run_id") != result.get("producer_run_id")):
                raise WorkflowError("cached result producer identity changed")
            if any(stored[field] != result_doc.get(field) for field in (
                "feature_ref", "definition_hash", "source_ref", "computation_fingerprint",
                "producer_run_id", "producer_work_key",
            )):
                raise WorkflowError("FeatureResult index identity disagrees with its immutable document")
            if (stored["split_id"] != result_doc.get("split_ref", {}).get("split_id")
                    or stored["scope_json"] != canonical_json(result_doc.get("scope"))
                    or stored["required_history_json"] != canonical_json(
                        result_doc.get("required_history"))):
                raise WorkflowError("FeatureResult index scope disagrees with its immutable document")
            target_plan, target_work, _ = self._research_plan_work(connection, run_id, work_key)
            producer_run_id = result_doc.get("producer_run_id")
            producer_work_key = result_doc.get("producer_work_key")
            if (not isinstance(producer_run_id, str) or not isinstance(producer_work_key, str)
                    or cache_source_run_id != producer_run_id):
                raise WorkflowError("cached result source must name its actual producer Run")
            producer_plan, producer_work, producer_work_by_key = self._research_plan_work(
                connection, producer_run_id, producer_work_key,
            )
            producer_stage = _fetchone(
                connection,
                "SELECT stage_run_id FROM stage_runs WHERE run_id=? AND work_key=?",
                (producer_run_id, producer_work_key),
            )
            if producer_stage is None:
                raise WorkflowError("cached result producer work is not registered")
            self._assert_feature_result_identity(
                result_doc, producer_plan, producer_work,
                producer_run_id=producer_run_id, producer_work_key=producer_work_key,
                producer_stage_run_id=producer_stage["stage_run_id"],
            )
            self._assert_feature_result_identity(
                result_doc, target_plan, target_work,
                producer_run_id=producer_run_id, producer_work_key=producer_work_key,
            )
            self._assert_feature_result_inputs(
                connection, result_doc, producer_plan, producer_work, producer_work_by_key,
                producer_run_id=producer_run_id,
            )
            self._assert_feature_result_artifacts(result_doc, artifacts)
            produced_link = _fetchone(
                connection,
                "SELECT link_id FROM run_result_links WHERE run_id=? AND work_key=? "
                "AND result_id=? AND relation='PRODUCED'",
                (producer_run_id, producer_work_key, result_id),
            )
            if produced_link is None:
                raise WorkflowError("cached FeatureResult has no registered producer link")
            current = StageStatus(stage["status"])
            linked = _fetchone(
                connection,
                "SELECT link_id FROM run_result_links WHERE run_id=? AND work_key=? AND result_id=?",
                (run_id, work_key, result_id),
            )
            if current in {StageStatus.SUCCEEDED, StageStatus.CACHED}:
                state_event = _fetchone(
                    connection,
                    "SELECT fields_json FROM stage_state_events WHERE stage_run_id=? AND state=? "
                    "ORDER BY event_id DESC LIMIT 1",
                    (stage_run_id, current.value),
                )
                active_result_id = (json.loads(state_event["fields_json"]).get("result_id")
                                    if state_event else None)
                if linked is not None and active_result_id == result_id:
                    return {"outcome": "existing", "result_id": result_id}
                if current == StageStatus.SUCCEEDED:
                    raise WorkflowError("cannot replace an already successful work result with cache")
                raise WorkflowError("a different cached result requires explicit work requeue")
            if current not in {StageStatus.PENDING, StageStatus.FAILED, StageStatus.BLOCKED}:
                raise WorkflowError(f"cannot cache result from work state {current.value}")
            artifacts_json = canonical_json({key: ref.to_dict()
                                             for key, ref in sorted(artifacts.items())})
            metrics_json = canonical_json(metrics)
            if linked is None:
                connection.execute(
                    "INSERT INTO run_result_links(run_id,work_key,result_id,relation,created_at) "
                    "VALUES(?,?,?,?,?)",
                    (run_id, work_key, result_id, "CACHED", timestamp),
                )
            fields = {
                "status": StageStatus.CACHED.value,
                "artifacts_json": artifacts_json,
                "metrics_json": metrics_json,
                "warnings_json": canonical_json([]),
                "finished_at": timestamp,
                "error_json": None,
                "source_stage_run_id": producer_stage["stage_run_id"],
            }
            assignments = ", ".join(f"{key}=?" for key in fields)
            connection.execute(f"UPDATE stage_runs SET {assignments} WHERE stage_run_id=?",
                               [*fields.values(), stage_run_id])
            connection.execute(
                "INSERT INTO stage_state_events(stage_run_id,state,attempt,fields_json,created_at) "
                "VALUES(?,?,?,?,?)",
                (stage_run_id, StageStatus.CACHED.value, 0,
                 canonical_json({**fields, "cache_source_run_id": cache_source_run_id,
                                "result_id": result_id}), timestamp),
            )
            return {"outcome": "existing" if linked is not None else "linked",
                    "result_id": result_id}

        return self._call(link)

    def get_feature_result(self, result_id: str) -> dict[str, Any] | None:
        row = self._call(lambda connection: _fetchone(
            connection, "SELECT result_json FROM feature_results WHERE result_id=?", (result_id,)
        ))
        if row is None:
            return None
        result = json.loads(row["result_json"])
        result["health_checks"] = self._feature_result_health(result_id)
        return result

    def find_feature_result_candidates(
        self, *, feature_ref: str, definition_hash: str, source_ref: str, split_id: str,
        scope: Mapping[str, Any], required_history: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        rows = self._call(lambda connection: _fetchall(
            connection,
            "SELECT result_id,result_json FROM feature_results WHERE feature_ref=? "
            "AND definition_hash=? AND source_ref=? AND split_id=? AND scope_json=? "
            "AND required_history_json=? ORDER BY created_at DESC,result_id",
            (feature_ref, definition_hash, source_ref, split_id, canonical_json(scope),
             canonical_json(required_history)),
        ))
        return [self.get_feature_result(row["result_id"]) for row in rows]

    def list_feature_results(self, *, run_id: str | None = None) -> list[dict[str, Any]]:
        if run_id is None:
            rows = self._call(lambda connection: _fetchall(
                connection, "SELECT result_id FROM feature_results ORDER BY created_at,result_id"
            ))
        else:
            rows = self._call(lambda connection: _fetchall(
                connection,
                "SELECT DISTINCT result_id FROM run_result_links WHERE run_id=? "
                "ORDER BY result_id", (run_id,),
            ))
        return [self.get_feature_result(row["result_id"]) for row in rows]

    def query_feature_results(self, filters: Mapping[str, Any], *,
                               offset: int, limit: int) -> tuple[list[dict[str, Any]], bool]:
        """Filter and page the FeatureResult index before decoding JSON documents."""
        clauses: list[str] = []
        params: list[Any] = []
        direct_fields = {
            "feature_ref": "f.feature_ref",
            "definition_hash": "f.definition_hash",
            "source_ref": "f.source_ref",
            "split_id": "f.split_id",
        }
        for field, column in direct_fields.items():
            if field in filters:
                clauses.append(f"{column}=?")
                params.append(filters[field])
        if "result_ids" in filters:
            result_ids = filters["result_ids"]
            if not result_ids:
                return [], False
            clauses.append("f.result_id IN (" + ",".join("?" for _ in result_ids) + ")")
            params.extend(result_ids)
        if "run_id" in filters:
            clauses.append(
                "EXISTS (SELECT 1 FROM run_result_links l WHERE l.result_id=f.result_id "
                "AND l.run_id=?)"
            )
            params.append(filters["run_id"])
        if "study_id" in filters:
            clauses.append(
                "EXISTS (SELECT 1 FROM run_result_links l JOIN runs r ON r.run_id=l.run_id "
                "WHERE l.result_id=f.result_id AND r.research_study_id=?)"
            )
            params.append(filters["study_id"])
        for field in ("instrument_id", "trade_date"):
            if field in filters:
                clauses.append(f"json_extract_string(f.result_json, '$.scope.{field}')=?")
                params.append(filters[field])
        if "start" in filters:
            clauses.append("json_extract_string(f.result_json, '$.scope.end') > ?")
            params.append(filters["start"])
        if "end" in filters:
            clauses.append("json_extract_string(f.result_json, '$.scope.start') < ?")
            params.append(filters["end"])
        where = " WHERE " + " AND ".join(clauses) if clauses else ""

        def select(connection: duckdb.DuckDBPyConnection) -> tuple[list[dict[str, Any]], bool]:
            rows = _fetchall(
                connection,
                "SELECT f.result_id,f.result_json FROM feature_results f"
                + where + " ORDER BY f.created_at,f.result_id LIMIT ? OFFSET ?",
                [*params, limit, offset],
            )
            docs = [{**json.loads(row["result_json"]), "health_checks": []} for row in rows]
            result_ids = [doc["result_id"] for doc in docs]
            if result_ids:
                placeholders = ",".join("?" for _ in result_ids)
                health = _fetchall(
                    connection,
                    "SELECT health_id,result_id,status,reason,checked_at,details_json "
                    "FROM feature_result_health WHERE result_id IN (" + placeholders + ") "
                    "ORDER BY health_id",
                    result_ids,
                )
                by_result = {result_id: [] for result_id in result_ids}
                for item in health:
                    by_result[item["result_id"]].append({
                        "health_id": int(item["health_id"]),
                        "status": item["status"],
                        "reason": item["reason"],
                        "checked_at": item["checked_at"],
                        "details": json.loads(item["details_json"]),
                    })
                for doc in docs:
                    doc["health_checks"] = by_result[doc["result_id"]]
            return docs, len(rows) >= limit

        return self._call(select)

    def list_run_result_links(self, run_id: str) -> list[dict[str, Any]]:
        rows = self._call(lambda connection: _fetchall(
            connection,
            "SELECT l.run_id,l.work_key,l.result_id,l.relation,l.created_at,r.result_json "
            "FROM run_result_links l JOIN feature_results r USING(result_id) "
            "WHERE l.run_id=? ORDER BY l.link_id", (run_id,),
        ))
        return [{"run_id": row["run_id"], "work_key": row["work_key"],
                 "result_id": row["result_id"], "relation": row["relation"],
                 "created_at": row["created_at"],
                 "result": {**json.loads(row["result_json"]),
                            "health_checks": self._feature_result_health(row["result_id"])}}
                for row in rows]

    def append_feature_result_health(self, *, result_id: str, status: str,
                                     reason: str | None = None,
                                     details: Mapping[str, Any] | None = None) -> dict[str, Any]:
        if status not in {"available", "missing", "invalid", "unverified"}:
            raise ValueError("health status must be available, missing, invalid, or unverified")
        timestamp = utc_now()

        def append(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
            if _fetchone(connection, "SELECT result_id FROM feature_results WHERE result_id=?",
                         (result_id,)) is None:
                raise WorkflowError(f"unknown FeatureResult: {result_id}")
            cursor = connection.execute(
                "INSERT INTO feature_result_health(result_id,status,reason,checked_at,details_json) "
                "VALUES(?,?,?,?,?) RETURNING health_id",
                (result_id, status, reason, timestamp, canonical_json(details or {})),
            )
            return {"health_id": int(cursor.fetchone()[0]), "result_id": result_id,
                    "status": status, "reason": reason, "checked_at": timestamp,
                    "details": dict(details or {})}

        return self._call(append)

    def _feature_result_health(self, result_id: str) -> list[dict[str, Any]]:
        rows = self._call(lambda connection: _fetchall(
            connection,
            "SELECT health_id,status,reason,checked_at,details_json FROM feature_result_health "
            "WHERE result_id=? ORDER BY health_id", (result_id,),
        ))
        return [{**row, "details": json.loads(row["details_json"])} for row in rows]

    def update_research_run_summary(self, run_id: str, *, status: str,
                                    summary: Mapping[str, Any]) -> None:
        timestamp = utc_now()

        def update(connection: duckdb.DuckDBPyConnection) -> None:
            if _fetchone(connection,
                         "SELECT run_id FROM runs WHERE run_id=? AND research_study_id IS NOT NULL",
                         (run_id,)) is None:
                raise WorkflowError(f"unknown research run_id: {run_id}")
            connection.execute(
                "UPDATE runs SET status=?,research_summary_json=?,updated_at=? WHERE run_id=?",
                (status, canonical_json(summary), timestamp, run_id),
            )

        self._call(update)

    def list_research_runs(self, *, study_id: str | None = None) -> list[dict[str, Any]]:
        if study_id is None:
            rows = self._call(lambda connection: _fetchall(
                connection,
                "SELECT run_id FROM runs WHERE research_study_id IS NOT NULL ORDER BY created_at,run_id",
            ))
        else:
            rows = self._call(lambda connection: _fetchall(
                connection,
                "SELECT run_id FROM runs WHERE research_study_id=? ORDER BY created_at,run_id",
                (study_id,),
            ))
        return [self.get_run(row["run_id"]) for row in rows]

    def enqueue_research_run(self, run_id: str, *, max_queued: int = 32) -> dict[str, Any]:
        """Persist a bounded dispatch intent without doing numerical work inline."""
        if not isinstance(run_id, str) or not run_id.strip():
            raise WorkflowError("run_id must be a non-empty string")
        if type(max_queued) is not int or max_queued < 1:
            raise WorkflowError("max_queued must be a positive integer")
        timestamp = utc_now()

        def enqueue(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
            exists = _fetchone(
                connection,
                "SELECT run_id FROM runs WHERE run_id=? AND research_study_id IS NOT NULL",
                (run_id,),
            )
            if exists is None:
                raise WorkflowError(f"unknown research Run: {run_id}")
            current = _fetchone(
                connection,
                "SELECT status,attempt FROM research_dispatch_queue WHERE run_id=?",
                (run_id,),
            )
            if current is not None and current["status"] in {"QUEUED", "RUNNING"}:
                return {"run_id": run_id, "status": current["status"], "already_queued": True}
            queued = _fetchone(
                connection,
                "SELECT count(*) AS n FROM research_dispatch_queue "
                "WHERE status IN ('QUEUED','RUNNING')",
            )["n"]
            if int(queued) >= max_queued:
                raise RunDispatchQueueFullError(
                    f"research dispatch queue is full ({max_queued})",
                )
            if current is None:
                connection.execute(
                    "INSERT INTO research_dispatch_queue"
                    "(run_id,status,requested_at,updated_at,attempt,error_json) "
                    "VALUES(?,'QUEUED',?,?,0,NULL)",
                    (run_id, timestamp, timestamp),
                )
            else:
                connection.execute(
                    "UPDATE research_dispatch_queue SET status='QUEUED',requested_at=?,"
                    "updated_at=?,error_json=NULL WHERE run_id=?",
                    (timestamp, timestamp, run_id),
                )
            return {"run_id": run_id, "status": "QUEUED", "already_queued": False}

        return self._call(enqueue)

    def recover_research_dispatches(self) -> int:
        """Make pre-crash claimed dispatch intents eligible for a new owner worker."""
        timestamp = utc_now()

        def recover(connection: duckdb.DuckDBPyConnection) -> int:
            row = _fetchone(
                connection,
                "SELECT count(*) AS n FROM research_dispatch_queue WHERE status='RUNNING'",
            )
            connection.execute(
                "UPDATE research_dispatch_queue SET status='QUEUED',updated_at=?,error_json=NULL "
                "WHERE status='RUNNING'",
                (timestamp,),
            )
            return int(row["n"])

        return self._call(recover)

    def claim_research_dispatches(self, *, limit: int = 1) -> list[str]:
        """Claim a bounded page of durable intents for owner compute workers."""
        if type(limit) is not int or not 1 <= limit <= 32:
            raise WorkflowError("dispatch claim limit must be between 1 and 32")
        timestamp = utc_now()

        def claim(connection: duckdb.DuckDBPyConnection) -> list[str]:
            rows = _fetchall(
                connection,
                "SELECT run_id FROM research_dispatch_queue WHERE status='QUEUED' "
                "ORDER BY requested_at,run_id LIMIT ?",
                (limit,),
            )
            run_ids = [row["run_id"] for row in rows]
            for run_id in run_ids:
                connection.execute(
                    "UPDATE research_dispatch_queue SET status='RUNNING',attempt=attempt+1,"
                    "updated_at=?,error_json=NULL WHERE run_id=? AND status='QUEUED'",
                    (timestamp, run_id),
                )
            return run_ids

        return self._call(claim)

    def finish_research_dispatch(self, run_id: str, *, error: dict[str, Any] | None = None) -> None:
        """Mark one dispatch attempt terminal; a later explicit resume can enqueue it again."""
        if error is not None and not isinstance(error, dict):
            raise WorkflowError("dispatch error must be an object or null")
        timestamp = utc_now()
        encoded = canonical_json(error) if error is not None else None

        def finish(connection: duckdb.DuckDBPyConnection) -> None:
            row = _fetchone(
                connection,
                "SELECT status FROM research_dispatch_queue WHERE run_id=?",
                (run_id,),
            )
            if row is None:
                return
            connection.execute(
                "UPDATE research_dispatch_queue SET status=?,updated_at=?,error_json=? "
                "WHERE run_id=?",
                ("FAILED" if error is not None else "DONE", timestamp, encoded, run_id),
            )

        self._call(finish)

    def research_dispatch_status(self) -> dict[str, int]:
        """Return bounded queue counters for the owner status endpoint."""
        rows = self._call(lambda connection: _fetchall(
            connection,
            "SELECT status,count(*) AS n FROM research_dispatch_queue "
            "GROUP BY status ORDER BY status",
        ))
        return {row["status"].lower(): int(row["n"]) for row in rows}

    def query_research_runs_page(self, study_id: str, *, start: str, end: str,
                                 instruments: Sequence[str], offset: int,
                                 limit: int) -> tuple[list[dict[str, Any]], bool]:
        """Page only a Study's Runs whose frozen scopes can overlap a query range."""
        if not instruments:
            return [], False
        instrument_predicate = " OR ".join(
            "json_contains(json_extract(r.research_plan_json, '$.scope.instruments'), to_json(?))"
            for _ in instruments
        )

        def select(connection: duckdb.DuckDBPyConnection) -> tuple[list[dict[str, Any]], bool]:
            rows = _fetchall(
                connection,
                "SELECT r.* FROM runs r WHERE r.research_study_id=? "
                "AND json_extract_string(r.research_plan_json, '$.scope.end')>? "
                "AND json_extract_string(r.research_plan_json, '$.scope.start')<? "
                f"AND ({instrument_predicate}) ORDER BY r.created_at,r.run_id LIMIT ? OFFSET ?",
                [study_id, start, end, *instruments, limit, offset],
            )
            return ([self._decode_run_row(row) for row in rows], len(rows) >= limit)

        return self._call(select)

    def get_run_work_history(self, run_ids: Sequence[str]) -> dict[str, Any]:
        """Fetch bounded Run work rows and their ordered attempt events in two owner queries."""
        if not run_ids:
            return {"stages_by_run": {}, "events_by_stage": {}}
        if len(run_ids) > 500:
            raise WorkflowError("get_run_work_history accepts at most 500 Run IDs")
        placeholders = ",".join("?" for _ in run_ids)

        def select(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
            rows = _fetchall(
                connection,
                f"SELECT * FROM stage_runs WHERE run_id IN ({placeholders}) "
                "ORDER BY run_id,stage_name,partition_json,registry_order",
                list(run_ids),
            )
            stages_by_run: dict[str, list[dict[str, Any]]] = {run_id: [] for run_id in run_ids}
            stage_ids = []
            for row in rows:
                stage = self._decode_stage_row(row)
                stages_by_run[stage["run_id"]].append(stage)
                stage_ids.append(stage["stage_run_id"])
            events_by_stage: dict[str, list[dict[str, Any]]] = {stage_id: [] for stage_id in stage_ids}
            if stage_ids:
                stage_placeholders = ",".join("?" for _ in stage_ids)
                events = _fetchall(
                    connection,
                    "SELECT * FROM stage_state_events WHERE stage_run_id IN ("
                    + stage_placeholders + ") ORDER BY event_id",
                    stage_ids,
                )
                for event in events:
                    events_by_stage[event["stage_run_id"]].append({
                        **event,
                        "fields": json.loads(event["fields_json"]),
                    })
            return {"stages_by_run": stages_by_run, "events_by_stage": events_by_stage}

        return self._call(select)

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
