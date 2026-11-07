"""Private, same-user Unix socket RPC for the ResearchService owner."""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import socket
import stat
import struct
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any

from quantsage_discovery.core.errors import WorkflowError, WorkspaceBusyError
from quantsage_discovery.core.registry import Registry

from .errors import ResearchError, ResearchValidationError
from .planner import MAX_PLAN_PARTITIONS, MAX_PLAN_WORK_UNITS
from .service import ResearchService

RPC_SCHEMA_VERSION = "research_rpc_v1"
MAX_REQUEST_BYTES = 8_388_608
MAX_RESPONSE_BYTES = 16_777_216
MAX_LIST_LIMIT = 100
MAX_READ_ROWS = 500
MAX_READ_REFS = 100
MAX_READ_COLUMNS = 6
MAX_QUERY_SECONDS = 20
MAX_EXECUTION_WORKERS = 2
MAX_DISPATCH_QUEUE = 32
MAX_CONNECTION_WORKERS = 8
SOCKET_IO_TIMEOUT_SECONDS = 5.0
CLIENT_TIMEOUT_SECONDS = 25.0
_ALLOWED_RESULT_COLUMNS = {
    "event_time", "available_time", "instrument_id", "trade_date", "session_id", "value",
}
_LOG = logging.getLogger(__name__)


class RpcError(Exception):
    """Base protocol error with stable wire fields."""

    code = "RPC_ERROR"
    retryable = False

    def __init__(self, message: str, *, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.details = details or {}


class RpcProtocolError(RpcError):
    code = "INVALID_REQUEST"


class RpcRequestTooLargeError(RpcError):
    code = "REQUEST_TOO_LARGE"


class RpcPeerRejectedError(RpcError):
    code = "PEER_UID_REJECTED"


class RpcOwnerUnavailableError(RpcError):
    code = "OWNER_UNAVAILABLE"
    retryable = True


class RpcRequestTimeoutError(RpcError):
    code = "REQUEST_TIMEOUT"
    retryable = True


class RpcDatabaseMismatchError(RpcError):
    code = "DATABASE_MISMATCH"


class RpcResponseTooLargeError(RpcError):
    code = "RESPONSE_TOO_LARGE"


def peer_uid_from_socket(connection: socket.socket) -> int:
    """Return the kernel-authenticated UID from Linux SO_PEERCRED."""
    if not hasattr(socket, "SO_PEERCRED"):
        raise RpcPeerRejectedError("SO_PEERCRED is required for local RPC authentication")
    raw = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
    _pid, uid, _gid = struct.unpack("3i", raw)
    return uid


def _require_same_uid(peer_uid: int, expected_uid: int | None = None) -> None:
    expected = os.getuid() if expected_uid is None else expected_uid
    if type(peer_uid) is not int or peer_uid != expected:
        raise RpcPeerRejectedError(
            "only processes running as the owner UID may use this socket",
            details={"expected_uid": expected, "peer_uid": peer_uid},
        )


def _resolved_database_path(value: str | Path) -> Path:
    path = Path(value).expanduser().resolve()
    if not path.is_absolute():
        raise RpcProtocolError("database path must resolve to an absolute path")
    return path


def _private_directory(path: Path, *, create: bool = False) -> Path:
    path = Path(os.path.abspath(os.fspath(path)))
    if create:
        try:
            path.mkdir(mode=0o700)
        except FileExistsError:
            pass
    try:
        info = path.lstat()
    except OSError as exc:
        raise RpcProtocolError(f"private socket directory is unavailable: {path}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise RpcProtocolError("socket parent must be a real directory, not a symlink")
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise RpcProtocolError(
            "socket parent must be owned by the current UID with mode 0700",
            details={"path": str(path), "owner_uid": info.st_uid,
                     "mode": oct(stat.S_IMODE(info.st_mode))},
        )
    return path


def default_socket_path(database_path: str | Path) -> Path:
    """Return a socket path isolated by both UID and resolved database path."""
    database = _resolved_database_path(database_path)
    runtime = _private_directory(Path("/tmp") / f"quantsage-{os.getuid()}", create=True)
    stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", database.stem)[:40] or "research"
    import hashlib

    suffix = hashlib.sha256(os.fsencode(database)).hexdigest()[:16]
    return runtime / f"{stem}-{suffix}.sock"


def _socket_path(value: str | Path | None, database_path: Path) -> Path:
    if value is None:
        return default_socket_path(database_path)
    path = Path(os.path.abspath(os.path.expanduser(os.fspath(value))))
    _private_directory(path.parent)
    return path


def _canonical_json(value: Any) -> bytes:
    try:
        payload = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RpcProtocolError("RPC value is not JSON serializable") from exc
    return payload


def _envelope(*, request_id: str | None, ok: bool,
              data: Any = None, error: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "schema_version": RPC_SCHEMA_VERSION,
        "request_id": request_id,
        "ok": ok,
        "data": data,
        "error": error,
    }


def _error_document(exc: BaseException) -> dict[str, Any]:
    code = getattr(exc, "code", None) or "INTERNAL_ERROR"
    message = str(exc) if isinstance(exc, (RpcError, ResearchError, WorkflowError)) else (
        "owner could not complete the request"
    )
    details = getattr(exc, "details", None)
    return {
        "code": str(code),
        "message": message,
        "details": details if isinstance(details, dict) else {},
        "retryable": bool(getattr(exc, "retryable", False)),
    }


def _error_exit_code(error: dict[str, Any] | None) -> int:
    if error is None:
        return 0
    code = error.get("code")
    if code == "WORKSPACE_BUSY":
        return 4
    if code in {
        "OWNER_UNAVAILABLE", "REQUEST_TIMEOUT", "RUN_QUEUE_FULL", "OWNER_SHUTTING_DOWN",
    }:
        return 3
    if code == "INTERNAL_ERROR":
        return 5
    return 2


def _object(value: Any, name: str = "params") -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RpcProtocolError(f"{name} must be a JSON object")
    return value


def _fields(value: dict[str, Any], *, required: set[str], optional: set[str],
            name: str = "params") -> dict[str, Any]:
    missing = required - value.keys()
    extra = value.keys() - required - optional
    if missing or extra:
        details = {}
        if missing:
            details["missing"] = sorted(missing)
        if extra:
            details["unknown"] = sorted(extra)
        raise RpcProtocolError(f"invalid {name} fields", details=details)
    return value


def _nonempty_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RpcProtocolError(f"{field} must be a non-empty string")
    return value


def _limit(value: Any, *, maximum: int, field: str = "limit") -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ResearchValidationError(f"{field} must be an integer from 1 through {maximum}")
    return value


def _bool(value: Any, field: str) -> bool:
    if type(value) is not bool:
        raise RpcProtocolError(f"{field} must be a boolean")
    return value


class ResearchOwner:
    """Single owner process for one Registry and its ResearchService."""

    def __init__(self, database_path: str | Path, socket_path: str | Path | None = None, *,
                 fault_hook: Callable[[str, dict[str, Any]], None] | None = None):
        self.database_path = _resolved_database_path(database_path)
        self.socket_path = _socket_path(socket_path, self.database_path)
        self._fault_hook = fault_hook
        self._stop = threading.Event()
        self._registry: Registry | None = None
        self._service: ResearchService | None = None
        self._listener: socket.socket | None = None
        self._request_pool: ThreadPoolExecutor | None = None
        self._execution_pool: ThreadPoolExecutor | None = None
        self._scheduler: threading.Thread | None = None
        self._inflight: dict[str, Future[Any]] = {}
        self._inflight_lock = threading.RLock()
        self._client_slots = threading.BoundedSemaphore(MAX_CONNECTION_WORKERS)
        self._socket_identity: tuple[int, int] | None = None
        self._client_count = 0
        self._client_count_lock = threading.Lock()

    @property
    def registry(self) -> Registry:
        if self._registry is None:
            raise RpcOwnerUnavailableError("owner has not started")
        return self._registry

    @property
    def service(self) -> ResearchService:
        if self._service is None:
            raise RpcOwnerUnavailableError("owner has not started")
        return self._service

    def _remove_stale_socket(self) -> None:
        try:
            info = self.socket_path.lstat()
        except FileNotFoundError:
            return
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
            raise WorkspaceBusyError(
                f"WORKSPACE_BUSY: socket path is occupied by a non-owned or non-socket file: "
                f"{self.socket_path}"
            )
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        probe.settimeout(0.2)
        try:
            probe.connect(str(self.socket_path))
        except ConnectionRefusedError:
            current = self.socket_path.lstat()
            if (stat.S_ISSOCK(current.st_mode) and current.st_uid == os.getuid()
                    and current.st_ino == info.st_ino and current.st_dev == info.st_dev):
                self.socket_path.unlink()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise WorkspaceBusyError(
                f"WORKSPACE_BUSY: existing socket may be active: {self.socket_path}"
            ) from exc
        else:
            raise WorkspaceBusyError(f"WORKSPACE_BUSY: socket is already active: {self.socket_path}")
        finally:
            probe.close()

    def _bind_socket(self) -> None:
        _private_directory(self.socket_path.parent)
        self._remove_stale_socket()
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.settimeout(0.5)
        old_umask = os.umask(0o177)
        try:
            listener.bind(str(self.socket_path))
        except OSError as exc:
            listener.close()
            if exc.errno in {98, 48, 10048}:
                raise WorkspaceBusyError(f"WORKSPACE_BUSY: socket already exists: {self.socket_path}") from exc
            raise
        finally:
            os.umask(old_umask)
        info = self.socket_path.lstat()
        self._socket_identity = (info.st_dev, info.st_ino)
        try:
            os.chmod(self.socket_path, 0o600)
            listener.listen(MAX_CONNECTION_WORKERS)
        except OSError:
            listener.close()
            self._close_socket()
            raise
        self._listener = listener

    def _close_socket(self) -> None:
        if self._listener is not None:
            try:
                self._listener.close()
            finally:
                self._listener = None
        try:
            info = self.socket_path.lstat()
        except FileNotFoundError:
            return
        if (stat.S_ISSOCK(info.st_mode) and info.st_uid == os.getuid()
                and self._socket_identity == (info.st_dev, info.st_ino)):
            self.socket_path.unlink()

    def _startup(self) -> None:
        self._registry = Registry(self.database_path)
        self._service = ResearchService(self._registry)
        # Receipt reconciliation is independent of dispatch intent: it restores
        # durable publications without scheduling any merely registered Run.
        self._service.recover_runs()
        self._registry.recover_research_dispatches()
        self._request_pool = ThreadPoolExecutor(
            max_workers=MAX_CONNECTION_WORKERS, thread_name_prefix="quantsage-rpc"
        )
        self._execution_pool = ThreadPoolExecutor(
            max_workers=MAX_EXECUTION_WORKERS, thread_name_prefix="quantsage-run"
        )
        self._bind_socket()
        self._scheduler = threading.Thread(
            target=self._schedule_loop, name="quantsage-dispatcher", daemon=True
        )
        self._scheduler.start()

    def _schedule_loop(self) -> None:
        while not self._stop.is_set():
            pool = self._execution_pool
            if pool is None:
                return
            with self._inflight_lock:
                completed = [run_id for run_id, future in self._inflight.items()
                             if future.done()]
                for run_id in completed:
                    future = self._inflight.pop(run_id)
                    failure = future.exception()
                    if failure is not None:
                        _LOG.error("research dispatch finalization failed for %s: %s",
                                   run_id, failure)
                capacity = MAX_EXECUTION_WORKERS - len(self._inflight)
            if capacity > 0:
                try:
                    run_ids = self.registry.claim_research_dispatches(limit=capacity)
                except (WorkflowError, OSError):
                    run_ids = []
                for run_id in run_ids:
                    future = pool.submit(self._execute_queued_run, run_id)
                    with self._inflight_lock:
                        self._inflight[run_id] = future
            self._stop.wait(0.1)

    def _execute_queued_run(self, run_id: str) -> None:
        error = None
        try:
            self.service.execute_run(run_id, fault_hook=self._fault_hook)
        except Exception as exc:  # noqa: BLE001 — persist any worker failure before acknowledging completion.
            error = {
                "code": getattr(exc, "code", "EXECUTION_ERROR"),
                "message": str(exc) if isinstance(exc, (ResearchError, WorkflowError))
                else "owner execution failed",
            }
        finally:
            self.registry.finish_research_dispatch(run_id, error=error)

    def _request_error(self, request_id: str | None, exc: BaseException) -> dict[str, Any]:
        return _envelope(request_id=request_id, ok=False, error=_error_document(exc))

    def _validate_request(self, request: Any) -> tuple[str | None, str, dict[str, Any]]:
        value = _object(request, "request")
        _fields(value, required={"schema_version", "request_id", "database", "method", "params"},
                optional=set(), name="request")
        if value["schema_version"] != RPC_SCHEMA_VERSION:
            raise RpcProtocolError("unsupported RPC schema_version")
        request_id = _nonempty_text(value["request_id"], "request_id")
        if len(request_id) > 128:
            raise RpcProtocolError("request_id exceeds 128 characters")
        database = _resolved_database_path(_nonempty_text(value["database"], "database"))
        if database != self.database_path:
            raise RpcDatabaseMismatchError("request database does not match this owner", details={
                "requested_database": str(database), "owner_database": str(self.database_path),
            })
        method = _nonempty_text(value["method"], "method")
        params = _object(value["params"])
        return request_id, method, params

    def _dispatch(self, method: str, params: dict[str, Any]) -> Any:
        # This explicit table is the entire callable RPC surface. No arbitrary attribute
        # lookup, SQL, import path, or Python expression crosses the socket.
        if method == "owner.status":
            _fields(params, required=set(), optional=set())
            with self._inflight_lock:
                active_runs = sorted(self._inflight)
            with self._client_count_lock:
                clients = self._client_count
            return {
                "status": "RUNNING", "pid": os.getpid(), "uid": os.getuid(),
                "database": str(self.database_path), "socket": str(self.socket_path),
                "active_clients": clients, "active_runs": active_runs,
                "execution_workers": MAX_EXECUTION_WORKERS,
                "dispatch_capacity": MAX_DISPATCH_QUEUE,
                "request_workers": MAX_CONNECTION_WORKERS,
                "max_request_bytes": MAX_REQUEST_BYTES,
                "max_response_bytes": MAX_RESPONSE_BYTES,
                "max_list_rows": MAX_LIST_LIMIT,
                "max_result_read_rows": MAX_READ_ROWS,
                "max_result_refs": MAX_READ_REFS,
                "max_result_columns": MAX_READ_COLUMNS,
                "max_plan_work_units": MAX_PLAN_WORK_UNITS,
                "max_plan_partitions": MAX_PLAN_PARTITIONS,
                "query_deadline_seconds": MAX_QUERY_SECONDS,
                "socket_read_deadline_seconds": SOCKET_IO_TIMEOUT_SECONDS,
                "dispatch_counts": self.registry.research_dispatch_status(),
            }
        if method == "owner.stop":
            _fields(params, required=set(), optional=set())
            self._stop.set()
            return {"status": "STOPPING", "pid": os.getpid()}

        if method == "sources.register":
            _fields(params, required={"document"}, optional=set())
            return self.service.register_source(_object(params["document"], "document"))
        if method == "sources.get":
            _fields(params, required={"ref"}, optional=set())
            return self.service.get_source(_nonempty_text(params["ref"], "ref"))

        if method == "studies.create":
            _fields(params, required={"document"}, optional=set())
            return self.service.create_study(_object(params["document"], "document"))
        if method == "studies.update":
            _fields(params, required={"study_id", "changes", "expected_revision", "actor", "reason"},
                    optional=set())
            return self.service.update_study(
                _nonempty_text(params["study_id"], "study_id"),
                _object(params["changes"], "changes"),
                expected_revision=params["expected_revision"],
                actor=_object(params["actor"], "actor"),
                reason=_nonempty_text(params["reason"], "reason"),
            )
        if method == "studies.get":
            _fields(params, required={"study_id"}, optional={"revision"})
            return self.service.get_study(
                _nonempty_text(params["study_id"], "study_id"), params.get("revision")
            )
        if method == "studies.coverage":
            _fields(params, required={"study_id"}, optional={
                "revision", "source_refs", "scope", "feature_set_ref", "item_ids", "verify",
            })
            verify = _bool(params.get("verify", True), "verify")
            return self.service.study_coverage(
                _nonempty_text(params["study_id"], "study_id"),
                revision=params.get("revision"), source_refs=params.get("source_refs"),
                scope=params.get("scope"), feature_set_ref=params.get("feature_set_ref"),
                item_ids=params.get("item_ids"), verify=verify,
            )

        catalog_methods = {
            "catalog.providers.register": self.service.register_provider,
            "catalog.templates.register": self.service.register_template,
            "catalog.metrics.register": self.service.register_metric,
            "catalog.metric_sets.register": self.service.register_metric_set,
        }
        if method in catalog_methods:
            _fields(params, required={"document"}, optional=set())
            return catalog_methods[method](_object(params["document"], "document"))
        catalog_getters = {
            "catalog.providers.get": self.service.get_provider,
            "catalog.templates.get": self.service.get_template,
            "catalog.metrics.get": self.service.get_metric,
            "catalog.metric_sets.get": self.service.get_metric_set,
        }
        if method in catalog_getters:
            _fields(params, required={"ref"}, optional=set())
            return catalog_getters[method](_nonempty_text(params["ref"], "ref"))

        if method == "feature_sets.save":
            _fields(params, required={"document"}, optional={"expected_revision"})
            return self.service.save_feature_set(
                _object(params["document"], "document"),
                expected_revision=params.get("expected_revision"),
            )
        if method == "feature_sets.freeze":
            _fields(params, required={"ref", "expected_revision"}, optional=set())
            return self.service.freeze_feature_set(
                _nonempty_text(params["ref"], "ref"), expected_revision=params["expected_revision"],
            )
        if method == "feature_sets.get":
            _fields(params, required={"ref"}, optional=set())
            return self.service.get_feature_set(_nonempty_text(params["ref"], "ref"))

        if method == "features.get":
            _fields(params, required={"ref"}, optional=set())
            return self.service.get_feature(_nonempty_text(params["ref"], "ref"))
        if method == "features.list":
            _fields(params, required=set(), optional={"filters", "limit", "cursor"})
            limit = _limit(params.get("limit", MAX_LIST_LIMIT), maximum=MAX_LIST_LIMIT)
            page = self.service.list_feature_page(
                _object(params.get("filters", {}), "filters"),
                limit=limit, cursor=params.get("cursor"),
            )
            return page

        if method == "runs.plan":
            _fields(params, required={"request"}, optional=set())
            return self.service.plan_run(_object(params["request"], "request"))
        if method == "runs.submit":
            _fields(params, required={"request", "request_key"}, optional={"expected_plan_hash"})
            return self.service.submit_run(
                _object(params["request"], "request"),
                request_key=_nonempty_text(params["request_key"], "request_key"),
                expected_plan_hash=params.get("expected_plan_hash"),
            )
        if method in {"runs.execute", "runs.resume"}:
            _fields(params, required={"run_id"}, optional=set())
            run_id = _nonempty_text(params["run_id"], "run_id")
            result = self.registry.enqueue_research_run(run_id, max_queued=MAX_DISPATCH_QUEUE)
            return {
                "run_id": run_id,
                "dispatch_status": result["status"],
                "already_queued": result["already_queued"],
            }
        if method == "runs.get":
            _fields(params, required={"run_id"}, optional=set())
            return self.service.get_run(_nonempty_text(params["run_id"], "run_id"))
        if method == "runs.compare":
            _fields(params, required={"left_run_id", "right_run_id"},
                    optional={"scope", "verify"})
            return self.service.compare_runs(
                _nonempty_text(params["left_run_id"], "left_run_id"),
                _nonempty_text(params["right_run_id"], "right_run_id"),
                scope=params.get("scope"), verify=_bool(params.get("verify", True), "verify"),
            )

        if method == "results.list":
            _fields(params, required=set(), optional={"filters", "limit", "cursor", "verify"})
            limit = _limit(params.get("limit", MAX_LIST_LIMIT), maximum=MAX_LIST_LIMIT)
            return self.service.list_results(
                params.get("filters"), limit=limit, cursor=params.get("cursor"),
                verify=_bool(params.get("verify", True), "verify"),
            )
        if method == "results.read":
            _fields(params, required={"result_refs"}, optional={"columns", "scope", "limit", "cursor"})
            refs = params["result_refs"]
            if not isinstance(refs, list) or len(refs) > MAX_READ_REFS:
                raise ResearchValidationError(f"result_refs must contain at most {MAX_READ_REFS} refs")
            columns = params.get("columns")
            if columns is not None and (
                not isinstance(columns, list) or len(columns) > MAX_READ_COLUMNS
                or any(column not in _ALLOWED_RESULT_COLUMNS for column in columns)
            ):
                raise ResearchValidationError(
                    f"columns must be from the six-field result whitelist "
                    f"and contain at most {MAX_READ_COLUMNS} entries"
                )
            return self.service.read_results(
                refs, columns=columns, scope=params.get("scope"),
                limit=_limit(params.get("limit", MAX_READ_ROWS), maximum=MAX_READ_ROWS),
                cursor=params.get("cursor"),
            )

        if method == "reviews.add":
            _fields(params, required={"document"}, optional=set())
            return self.service.add_review(_object(params["document"], "document"))
        if method == "reviews.list":
            _fields(params, required={"study_id"}, optional={
                "run_id", "result_ref", "limit", "cursor",
            })
            return self.service.list_reviews(
                _nonempty_text(params["study_id"], "study_id"),
                run_id=params.get("run_id"), result_ref=params.get("result_ref"),
                limit=_limit(params.get("limit", MAX_LIST_LIMIT), maximum=MAX_LIST_LIMIT),
                cursor=params.get("cursor"),
            )
        raise RpcProtocolError(f"method is not in the RPC allowlist: {method}",
                               details={"method": method})

    def _handle_request(self, request: Any) -> dict[str, Any]:
        request_id = request.get("request_id") if isinstance(request, dict) else None
        if not isinstance(request_id, str) or len(request_id) > 128:
            request_id = None
        try:
            request_id, method, params = self._validate_request(request)
            data = self._dispatch(method, params)
            return _envelope(request_id=request_id, ok=True, data=data)
        except Exception as exc:  # noqa: BLE001 — every service failure needs a JSON envelope.
            return self._request_error(request_id, exc)

    def _read_request(self, connection: socket.socket) -> Any:
        deadline = time.monotonic() + SOCKET_IO_TIMEOUT_SECONDS
        chunks = bytearray()
        terminated = False
        while len(chunks) <= MAX_REQUEST_BYTES:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RpcRequestTimeoutError(
                    "request body exceeded the 5 second read deadline",
                    details={"timeout_seconds": SOCKET_IO_TIMEOUT_SECONDS},
                )
            connection.settimeout(remaining)
            block = connection.recv(min(65_536, MAX_REQUEST_BYTES + 1 - len(chunks)))
            if not block:
                break
            newline = block.find(b"\n")
            if newline >= 0:
                chunks.extend(block[:newline])
                if len(chunks) > MAX_REQUEST_BYTES:
                    raise RpcRequestTooLargeError("request exceeds the 8 MiB byte limit")
                trailing = block[newline + 1:]
                if trailing.strip():
                    raise RpcProtocolError("one JSON request per connection is allowed")
                terminated = True
                break
            chunks.extend(block)
        if len(chunks) > MAX_REQUEST_BYTES:
            raise RpcRequestTooLargeError("request exceeds the 8 MiB byte limit")
        if not chunks:
            raise RpcProtocolError("request body is empty")
        if not terminated:
            raise RpcProtocolError("request must end with a newline within the 8 MiB limit")
        try:
            return json.loads(chunks.decode("utf-8"), parse_constant=lambda x: (_ for _ in ()).throw(
                ValueError(f"invalid JSON constant {x}")))
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise RpcProtocolError(f"request is not valid finite UTF-8 JSON: {exc}") from exc

    def _serve_connection(self, connection: socket.socket) -> None:
        with self._client_count_lock:
            self._client_count += 1
        request_id = None
        try:
            _require_same_uid(peer_uid_from_socket(connection))
            try:
                request = self._read_request(connection)
                if isinstance(request, dict) and isinstance(request.get("request_id"), str):
                    candidate_id = request["request_id"]
                    request_id = candidate_id if len(candidate_id) <= 128 else None
                response = self._handle_request(request)
            except (RpcError, OSError) as exc:
                response = self._request_error(request_id, exc)
            payload = _canonical_json(response)
            if len(payload) > MAX_RESPONSE_BYTES:
                payload = _canonical_json(self._request_error(
                    request_id, RpcResponseTooLargeError(
                        "response exceeds the 16 MiB byte limit",
                        details={"maximum_bytes": MAX_RESPONSE_BYTES},
                    ),
                ))
            connection.settimeout(SOCKET_IO_TIMEOUT_SECONDS)
            connection.sendall(payload + b"\n")
        except (OSError, RpcError):
            return
        finally:
            with self._client_count_lock:
                self._client_count -= 1
            try:
                connection.close()
            finally:
                self._client_slots.release()

    def _accept_loop(self) -> None:
        assert self._listener is not None and self._request_pool is not None
        while not self._stop.is_set():
            try:
                connection, _ = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                if self._stop.is_set():
                    break
                raise
            if not self._client_slots.acquire(blocking=False):
                connection.close()
                continue
            try:
                self._request_pool.submit(self._serve_connection, connection)
            except BaseException:
                self._client_slots.release()
                connection.close()
                raise

    def serve_forever(self) -> None:
        previous_term = None
        try:
            self._startup()
            if threading.current_thread() is threading.main_thread():
                previous_term = signal.getsignal(signal.SIGTERM)
                signal.signal(signal.SIGTERM, lambda _signum, _frame: self._stop.set())
            self._accept_loop()
        finally:
            self._stop.set()
            if previous_term is not None:
                signal.signal(signal.SIGTERM, previous_term)
            if self._listener is not None:
                self._listener.close()
                self._listener = None
            if self._request_pool is not None:
                self._request_pool.shutdown(wait=True, cancel_futures=False)
                self._request_pool = None
            if self._scheduler is not None:
                self._scheduler.join()
                self._scheduler = None
            if self._execution_pool is not None:
                self._execution_pool.shutdown(wait=True, cancel_futures=False)
                self._execution_pool = None
            self._close_socket()
            if self._registry is not None:
                self._registry.close()
                self._registry = None
                self._service = None


def rpc_call(database_path: str | Path, method: str, params: dict[str, Any], *,
             socket_path: str | Path | None = None,
             timeout: float = CLIENT_TIMEOUT_SECONDS) -> dict[str, Any]:
    """Send one bounded JSON request to the explicit database's owner."""
    database = _resolved_database_path(database_path)
    endpoint = _socket_path(socket_path, database)
    request_id = uuid.uuid4().hex
    request = {
        "schema_version": RPC_SCHEMA_VERSION,
        "request_id": request_id,
        "database": str(database),
        "method": method,
        "params": params,
    }
    payload = _canonical_json(request)
    if len(payload) > MAX_REQUEST_BYTES:
        raise RpcRequestTooLargeError(
            "request exceeds the 8 MiB byte limit", details={"maximum_bytes": MAX_REQUEST_BYTES}
        )
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(timeout)
    deadline = time.monotonic() + timeout
    try:
        try:
            connection.connect(str(endpoint))
        except (FileNotFoundError, ConnectionRefusedError, TimeoutError, OSError) as exc:
            raise RpcOwnerUnavailableError(
                f"OWNER_UNAVAILABLE: no ResearchService owner is reachable at {endpoint}",
                details={"socket": str(endpoint), "database": str(database)},
            ) from exc
        connection.sendall(payload + b"\n")
        response_bytes = bytearray()
        terminated = False
        while len(response_bytes) <= MAX_RESPONSE_BYTES:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError
            connection.settimeout(remaining)
            block = connection.recv(min(65_536, MAX_RESPONSE_BYTES + 1 - len(response_bytes)))
            if not block:
                break
            newline = block.find(b"\n")
            if newline >= 0:
                response_bytes.extend(block[:newline])
                terminated = True
                break
            response_bytes.extend(block)
        if len(response_bytes) > MAX_RESPONSE_BYTES:
            raise RpcResponseTooLargeError("response exceeds the 16 MiB byte limit")
        if not terminated:
            raise RpcOwnerUnavailableError("owner disconnected before completing the response")
        try:
            response = json.loads(response_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RpcProtocolError("owner returned an invalid JSON envelope") from exc
        if not isinstance(response, dict) or set(response) != {
            "schema_version", "request_id", "ok", "data", "error",
        }:
            raise RpcProtocolError("owner response envelope has an invalid shape")
        if response["schema_version"] != RPC_SCHEMA_VERSION or response["request_id"] != request_id:
            raise RpcProtocolError("owner response schema or request_id does not match")
        if type(response["ok"]) is not bool or (response["ok"] == (response["error"] is not None)):
            raise RpcProtocolError("owner response ok/error fields are inconsistent")
        return response
    except TimeoutError as exc:
        raise RpcRequestTimeoutError(
            f"owner did not answer before the {timeout:g} second client deadline",
            details={"timeout_seconds": timeout, "request_id": request_id},
        ) from exc
    except OSError as exc:
        raise RpcOwnerUnavailableError(
            "owner connection ended before a complete response was received",
            details={"socket": str(endpoint), "database": str(database)},
        ) from exc
    finally:
        connection.close()
