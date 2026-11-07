"""Bounded Results, Coverage, Review, and Run comparison queries."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from quantsage_discovery.core.artifacts import ArtifactStore, artifact_path
from quantsage_discovery.core.registry import Registry, utc_now
from quantsage_discovery.core.types import ArtifactRef
from quantsage_discovery.core.utils import canonical_json

from .catalog import FeatureCatalog
from .errors import (
    ArtifactInvalidError,
    ResearchValidationError,
    SourceScopeMissingError,
    UnknownReferenceError,
)
from .repository import ResearchRepository
from .schemas import _read_json_ref, parse_versioned_ref, scope_is_within

_RESULT_FILTERS = {
    "run_id", "study_id", "feature_ref", "definition_hash", "source_ref", "split_id",
    "instrument_id", "trade_date", "start", "end", "result_ids",
}
_DATA_META_COLUMNS = (
    "result_id", "feature_ref", "definition_hash", "source_ref", "producer_run_id",
)
_MAX_LIST_LIMIT = 500
_MAX_READ_LIMIT = 10_000
_MAX_READ_REFS = 100
_MAX_REVIEW_REFS = 100
_QUERY_TIMEOUT_S = 20.0
_REVIEW_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


def _text(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ResearchValidationError(f"{path} must be a non-empty string without surrounding whitespace")
    return value


def _strings(value: Any, path: str, *, nonempty: bool = True) -> list[str]:
    if not isinstance(value, (list, tuple)) or (nonempty and not value):
        raise ResearchValidationError(f"{path} must be an {'non-empty ' if nonempty else ''}array of strings")
    result = [_text(item, f"{path}[{index}]") for index, item in enumerate(value)]
    if len(set(result)) != len(result):
        raise ResearchValidationError(f"{path} must not contain duplicates")
    return result


def _timestamp(value: Any, path: str) -> datetime:
    if not isinstance(value, str):
        raise ResearchValidationError(f"{path} must be a timezone-aware ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ResearchValidationError(f"{path} must be a timezone-aware ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ResearchValidationError(f"{path} must include a timezone offset")
    return parsed.astimezone(UTC)


def _time_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _date(value: Any, path: str) -> str:
    if not isinstance(value, str):
        raise ResearchValidationError(f"{path} must be an ISO trade date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ResearchValidationError(f"{path} must be an ISO trade date") from exc
    if parsed.isoformat() != value:
        raise ResearchValidationError(f"{path} must use YYYY-MM-DD")
    return value


def _normalize_scope(value: Mapping[str, Any] | None, *, required: bool = False) -> dict[str, Any]:
    if value is None:
        if required:
            raise ResearchValidationError("scope is required")
        return {}
    if not isinstance(value, Mapping):
        raise ResearchValidationError("scope must be an object")
    allowed = {"instruments", "start", "end", "trade_dates", "session_filter", "session_ids"}
    unknown = set(value) - allowed
    if unknown:
        raise ResearchValidationError(f"unknown scope fields: {', '.join(sorted(unknown))}")
    result: dict[str, Any] = {}
    if "instruments" in value:
        result["instruments"] = sorted(_strings(value["instruments"], "scope.instruments"))
    if ("start" in value) != ("end" in value):
        raise ResearchValidationError("scope.start and scope.end must be supplied together")
    if "start" in value:
        start, end = _timestamp(value["start"], "scope.start"), _timestamp(value["end"], "scope.end")
        if start >= end:
            raise ResearchValidationError("scope.start must be earlier than scope.end")
        result["start"], result["end"] = _time_text(start), _time_text(end)
    if "trade_dates" in value:
        if not isinstance(value["trade_dates"], (list, tuple)):
            raise ResearchValidationError("scope.trade_dates must be an array")
        result["trade_dates"] = sorted({
            _date(day, f"scope.trade_dates[{index}]")
            for index, day in enumerate(value["trade_dates"])
        })
        if not result["trade_dates"]:
            raise ResearchValidationError("scope.trade_dates must not be empty")
    sessions = value.get("session_filter", value.get("session_ids"))
    if "session_filter" in value and "session_ids" in value:
        left = _strings(value["session_filter"], "scope.session_filter")
        right = _strings(value["session_ids"], "scope.session_ids")
        if set(left) != set(right):
            raise ResearchValidationError("scope.session_filter and session_ids conflict")
        sessions = left
    if sessions is not None:
        result["session_filter"] = sorted(_strings(sessions, "scope.session_filter"))
    if required and not {"instruments", "start", "end"}.issubset(result):
        raise ResearchValidationError("scope requires instruments, start, and end")
    return result


def _page_args(limit: int, cursor: str | None, *, maximum: int,
               query: Any) -> tuple[int, int, str]:
    if type(limit) is not int or limit < 1 or limit > maximum:
        raise ResearchValidationError(f"limit must be an integer from 1 through {maximum}")
    signature = hashlib.sha256(canonical_json(query).encode()).hexdigest()[:24]
    if cursor is None:
        return 0, limit, signature
    if not isinstance(cursor, str) or not cursor:
        raise ResearchValidationError("cursor must be a non-empty string")
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor.encode("ascii") + b"=" * (-len(cursor) % 4)))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ResearchValidationError("cursor is malformed") from exc
    if not isinstance(payload, dict) or payload.get("query") != signature:
        raise ResearchValidationError("cursor does not match this query")
    offset = payload.get("offset")
    if type(offset) is not int or offset < 0:
        raise ResearchValidationError("cursor offset is invalid")
    return offset, limit, signature


def _next_cursor(signature: str, offset: int) -> str:
    payload = json.dumps({"query": signature, "offset": offset}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def _health_view(result: dict[str, Any], verification: dict[str, Any] | None) -> dict[str, Any]:
    history = result.get("health_checks", [])
    last = history[-1] if history else None
    if verification is not None:
        return {
            "status": verification["status"],
            "checked_at": verification["checked_at"],
            "reason": verification["reason"],
            "health_unverified_since_check": False,
        }
    return {
        "status": "unverified",
        "checked_at": None if last is None else last.get("checked_at"),
        "last_status": None if last is None else last.get("status"),
        "reason": "health_unverified_since_check",
        "health_unverified_since_check": True,
    }


def _interval_doc(scope: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "exchange": scope.get("exchange"),
        "instrument_id": scope.get("instrument_id"),
        "trade_date": scope.get("trade_date"),
        "session_id": scope.get("session_id"),
        "start": scope.get("start"),
        "end": scope.get("end"),
    }


def _bounds(interval: Mapping[str, Any]) -> tuple[datetime, datetime]:
    return _timestamp(interval["start"], "interval.start"), _timestamp(interval["end"], "interval.end")


def _intersect(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any] | None:
    left_start, left_end = _bounds(left)
    right_start, right_end = _bounds(right)
    start, end = max(left_start, right_start), min(left_end, right_end)
    if start >= end:
        return None
    merged = dict(left)
    merged["start"], merged["end"] = _time_text(start), _time_text(end)
    return merged


def _subtract(interval: Mapping[str, Any], covered: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    start, end = _bounds(interval)
    pieces = [(start, end)]
    for existing in sorted(covered, key=lambda item: item["start"]):
        other_start, other_end = _bounds(existing)
        next_pieces = []
        for piece_start, piece_end in pieces:
            if other_end <= piece_start or other_start >= piece_end:
                next_pieces.append((piece_start, piece_end))
            else:
                if piece_start < other_start:
                    next_pieces.append((piece_start, other_start))
                if other_end < piece_end:
                    next_pieces.append((other_end, piece_end))
        pieces = next_pieces
    return [{**interval, "start": _time_text(piece_start), "end": _time_text(piece_end)}
            for piece_start, piece_end in pieces if piece_start < piece_end]


def _merge_intervals(intervals: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for interval in intervals:
        key = (interval.get("source_ref"), interval.get("definition_hash"),
               interval.get("instrument_id"), interval.get("exchange"),
               interval.get("trade_date"), interval.get("session_id"))
        grouped[key].append(interval)
    merged: list[dict[str, Any]] = []
    for entries in grouped.values():
        group_merged: list[dict[str, Any]] = []
        for item in sorted(entries, key=lambda entry: entry["start"]):
            if (group_merged and _timestamp(item["start"], "interval.start")
                    <= _timestamp(group_merged[-1]["end"], "interval.end")):
                group_merged[-1]["end"] = max(group_merged[-1]["end"], item["end"])
            else:
                group_merged.append(dict(item))
        merged.extend(group_merged)
    merged.sort(key=lambda item: (item.get("source_ref", ""), item.get("definition_hash", ""),
                                  item.get("instrument_id", ""), item.get("trade_date", ""),
                                  item.get("session_id", ""), item["start"]))
    return merged


def _group_key(interval: Mapping[str, Any]) -> tuple[Any, ...]:
    return (interval.get("source_ref"), interval.get("definition_hash"),
            interval.get("instrument_id"), interval.get("exchange"),
            interval.get("trade_date"), interval.get("session_id"))


def _scope_overlaps_interval(scope: Mapping[str, Any],
                             interval: Mapping[str, Any]) -> dict[str, Any] | None:
    instrument = scope.get("instrument_id")
    instruments = scope.get("instruments")
    if instrument is not None and instrument != interval.get("instrument_id"):
        return None
    if instruments is not None and interval.get("instrument_id") not in instruments:
        return None
    for field in ("trade_date", "session_id", "exchange"):
        value = scope.get(field)
        if value is not None and interval.get(field) is not None and value != interval.get(field):
            return None
    session_filter = scope.get("session_filter")
    if session_filter is not None and interval.get("session_id") not in session_filter:
        return None
    return _intersect(scope, interval)


def _scope_filter(table: pa.Table, scope: Mapping[str, Any]) -> pa.Table:
    mask = pa.array([True] * table.num_rows, type=pa.bool_())
    if "instruments" in scope:
        mask = pc.and_kleene(mask, pc.is_in(table["instrument_id"], value_set=pa.array(scope["instruments"])))
    if "trade_dates" in scope:
        mask = pc.and_kleene(mask, pc.is_in(table["trade_date"], value_set=pa.array(scope["trade_dates"])))
    sessions = scope.get("session_filter")
    if sessions:
        mask = pc.and_kleene(mask, pc.is_in(table["session_id"], value_set=pa.array(sessions)))
    if "start" in scope:
        start = _timestamp(scope["start"], "scope.start")
        end = _timestamp(scope["end"], "scope.end")
        field_type = table.schema.field("event_time").type
        start_scalar = pa.scalar(start, type=field_type)
        end_scalar = pa.scalar(end, type=field_type)
        mask = pc.and_kleene(mask, pc.greater_equal(table["event_time"], start_scalar))
        mask = pc.and_kleene(mask, pc.less(table["event_time"], end_scalar))
    return table.filter(mask)


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


class ResearchQueries:
    """Read bounded result and Study evidence without changing frozen execution state."""

    def __init__(self, registry: Registry, repository: ResearchRepository,
                 catalog: FeatureCatalog, artifact_store: ArtifactStore):
        self._registry = registry
        self._repository = repository
        self._catalog = catalog
        self._store = artifact_store
        self._executor_instance = None

    def _executor(self):
        if self._executor_instance is None:
            from .execution import ResearchExecutor

            self._executor_instance = ResearchExecutor(self._registry, self._store, self._catalog)
        return self._executor_instance

    def _get_result(self, result_id: str) -> dict[str, Any]:
        result = self._registry.get_feature_result(result_id)
        if result is None:
            raise UnknownReferenceError(f"unknown FeatureResult: {result_id}",
                                        details={"result_id": result_id})
        return result

    def _verify(self, result: dict[str, Any], enabled: bool,
                deadline: float | None = None) -> dict[str, Any] | None:
        if not enabled:
            return None
        check = self._executor().verify_result(result["result_id"], record_health=True)
        if deadline is not None and time.perf_counter() > deadline:
            raise ResearchValidationError(
                "result verification exceeded its time budget",
                details={"reason": "QUERY_RESOURCE_LIMIT", "limit_seconds": _QUERY_TIMEOUT_S},
            )
        return check

    def list_results(self, filters: Mapping[str, Any] | None = None, *, limit: int = 100,
                     cursor: str | None = None, verify: bool = True) -> dict[str, Any]:
        if not isinstance(verify, bool):
            raise ResearchValidationError("verify must be a boolean")
        if filters is not None and not isinstance(filters, Mapping):
            raise ResearchValidationError("filters must be an object")
        filters = dict(filters or {})
        unknown = set(filters) - _RESULT_FILTERS
        if unknown:
            raise ResearchValidationError(f"unknown result filters: {', '.join(sorted(unknown))}")
        normalized = dict(filters)
        for key in ("run_id", "study_id", "feature_ref", "definition_hash", "source_ref", "split_id",
                    "instrument_id", "trade_date"):
            if key in normalized:
                normalized[key] = _text(normalized[key], f"filters.{key}")
        if "trade_date" in normalized:
            normalized["trade_date"] = _date(normalized["trade_date"], "filters.trade_date")
        if "start" in normalized:
            normalized["start"] = _time_text(_timestamp(normalized["start"], "filters.start"))
        if "end" in normalized:
            normalized["end"] = _time_text(_timestamp(normalized["end"], "filters.end"))
        if ("start" in normalized) != ("end" in normalized):
            raise ResearchValidationError("filters.start and filters.end must be supplied together")
        if "start" in normalized and normalized["start"] >= normalized["end"]:
            raise ResearchValidationError("filters.start must be earlier than filters.end")
        if "result_ids" in normalized:
            normalized["result_ids"] = _strings(normalized["result_ids"], "filters.result_ids", nonempty=False)
            if len(normalized["result_ids"]) > _MAX_READ_REFS:
                raise ResearchValidationError(
                    f"filters.result_ids may contain at most {_MAX_READ_REFS} refs"
                )
        if "study_id" in normalized and self._repository.get_study(normalized["study_id"]) is None:
            raise UnknownReferenceError(f"unknown Study: {normalized['study_id']}")
        if "run_id" in normalized:
            run = self._registry.get_run(normalized["run_id"])
            if run is None or run.get("research_study_id") is None:
                raise UnknownReferenceError(f"unknown research Run: {normalized['run_id']}")
            if "study_id" in normalized and run["research_study_id"] != normalized["study_id"]:
                raise ResearchValidationError("run_id does not belong to filters.study_id")
        offset, page_limit, signature = _page_args(
            limit, cursor, maximum=_MAX_LIST_LIMIT,
            query={"filters": normalized, "verify": verify},
        )
        page, has_more = self._registry.query_feature_results(
            normalized, offset=offset, limit=page_limit + 1,
        )
        selected = page[:page_limit]
        checked = []
        items = []
        deadline = time.perf_counter() + _QUERY_TIMEOUT_S
        for result in selected:
            health = self._verify(result, verify, deadline)
            item = dict(result)
            if health is not None:
                item.setdefault("health_checks", []).append({
                    "status": health["status"], "reason": health["reason"],
                    "checked_at": health["checked_at"],
                    "details": {"source": "research_query_verification"},
                })
                checked.append(health["checked_at"])
            item["health"] = _health_view(result, health)
            items.append(item)
        truncated = has_more or len(page) > page_limit
        next_cursor = (_next_cursor(signature, offset + len(selected)) if truncated else None)
        return {
            "schema_version": "result_page_v1",
            "items": items,
            "next_cursor": next_cursor,
            "truncated": truncated,
            "checked_at": max(checked) if checked else None,
        }

    def _scan_artifact(self, result: dict[str, Any], descriptor: dict[str, Any], *,
                       data_columns: Sequence[str], scope: Mapping[str, Any],
                       on_row=None, deadline: float | None = None) -> tuple[int, int]:
        raw_ref = descriptor.get("artifact_ref")
        if not isinstance(raw_ref, dict):
            raise ResearchValidationError("FeatureResult artifact descriptor has no artifact_ref")
        ref = ArtifactRef.from_dict(raw_ref)
        path = artifact_path(ref.uri)
        total = valid = 0
        predicate_columns = set()
        if "instruments" in scope:
            predicate_columns.add("instrument_id")
        if "trade_dates" in scope:
            predicate_columns.add("trade_date")
        if "session_filter" in scope:
            predicate_columns.add("session_id")
        if "start" in scope:
            predicate_columns.add("event_time")
        read_columns = sorted(set(data_columns) | predicate_columns | {"value"})
        try:
            parquet = pq.ParquetFile(path)
        except (OSError, pa.ArrowException) as exc:
            raise ResearchValidationError(f"cannot open FeatureResult Parquet: {exc}",
                                          details={"result_id": result["result_id"]}) from exc
        for batch in parquet.iter_batches(columns=read_columns, batch_size=4096):
            if deadline is not None and time.perf_counter() > deadline:
                raise ResearchValidationError(
                    "result query exceeded its time budget",
                    details={"reason": "QUERY_RESOURCE_LIMIT", "limit_seconds": _QUERY_TIMEOUT_S},
                )
            table = _scope_filter(pa.Table.from_batches([batch]), scope)
            if table.num_rows == 0:
                continue
            valid_mask = pc.is_valid(table["value"])
            total += table.num_rows
            valid += int(pc.sum(pc.cast(valid_mask, pa.int64())).as_py() or 0)
            if on_row is not None:
                visible = table.select(list(data_columns)).to_pylist()
                for row in visible:
                    if on_row({key: _json_safe(value) for key, value in row.items()}) is False:
                        return total, valid
        return total, valid

    def read_results(self, result_refs: Sequence[str], *, columns: Sequence[str] | None = None,
                     scope: Mapping[str, Any] | None = None, limit: int = 1000,
                     cursor: str | None = None) -> dict[str, Any]:
        refs = _strings(result_refs, "result_refs", nonempty=False)
        if len(refs) > _MAX_READ_REFS:
            raise ResearchValidationError(f"result_refs may contain at most {_MAX_READ_REFS} refs")
        normalized_scope = _normalize_scope(scope)
        requested_columns = None if columns is None else _strings(columns, "columns", nonempty=False)
        if requested_columns is not None and len(requested_columns) > 64:
            raise ResearchValidationError("columns may contain at most 64 names")
        if requested_columns is not None and any(
            name not in {"event_time", "available_time", "instrument_id", "trade_date",
                         "session_id", "value"} for name in requested_columns
        ):
            raise ResearchValidationError("result column whitelist contains an unsupported field")
        query = {"result_refs": refs, "columns": requested_columns, "scope": normalized_scope}
        offset, page_limit, signature = _page_args(
            limit, cursor, maximum=_MAX_READ_LIMIT, query=query,
        )
        if not refs:
            return {
                "schema_version": "result_rows_v1", "result_refs": [],
                "columns": list(_DATA_META_COLUMNS) + (requested_columns or []),
                "rows": [], "next_cursor": None, "truncated": False,
                "checked_at": None, "health_verification": "verified",
            }
        deadline = time.perf_counter() + _QUERY_TIMEOUT_S
        indexed, _ = self._registry.query_feature_results(
            {"result_ids": refs}, offset=0, limit=len(refs) + 1,
        )
        by_id = {item["result_id"]: item for item in indexed}
        missing = set(refs) - by_id.keys()
        if missing:
            raise UnknownReferenceError("unknown FeatureResult refs", details={"result_refs": sorted(missing)})
        results = [by_id[result_id] for result_id in refs]
        verification = {}
        checks = []
        for result in results:
            check = self._verify(result, True, deadline)
            verification[result["result_id"]] = check
            checks.append(check["checked_at"])
            if check["status"] != "available":
                raise ArtifactInvalidError(
                    f"FeatureResult {result['result_id']} is {check['status']}",
                    details={"result_id": result["result_id"], "reason": check["reason"],
                             "health_status": check["status"]},
                )
        allowed_by_result: dict[str, set[str]] = {}
        for result in results:
            descriptors = result.get("artifacts")
            if not isinstance(descriptors, list) or not descriptors:
                raise ResearchValidationError("FeatureResult has no artifact descriptors")
            declared = set()
            for descriptor in descriptors:
                if not isinstance(descriptor, dict) or not isinstance(descriptor.get("columns"), list):
                    raise ResearchValidationError("FeatureResult artifact columns are malformed")
                declared.update(descriptor["columns"])
            allowed_by_result[result["result_id"]] = declared
        if requested_columns is None:
            data_columns = sorted(set.intersection(*allowed_by_result.values()))
        else:
            data_columns = requested_columns
            for result_id, allowed in allowed_by_result.items():
                unknown_columns = set(data_columns) - allowed
                if unknown_columns:
                    raise ResearchValidationError(
                        f"columns are not present in FeatureResult {result_id}: "
                        + ", ".join(sorted(unknown_columns)),
                        details={"result_id": result_id, "unknown_columns": sorted(unknown_columns)},
                    )
        if any(name not in {"event_time", "available_time", "instrument_id", "trade_date",
                            "session_id", "value"} for name in data_columns):
            raise ResearchValidationError("result column whitelist contains an unsupported field")
        output_columns = list(_DATA_META_COLUMNS) + data_columns
        rows: list[dict[str, Any]] = []
        row_offset = 0
        has_more = False
        for result in results:
            descriptors = result["artifacts"]
            for descriptor in descriptors:
                def add_row(data_row, result=result):
                    nonlocal row_offset, has_more
                    if row_offset < offset:
                        row_offset += 1
                        return
                    if len(rows) >= page_limit:
                        has_more = True
                        return False
                    rows.append({
                        "result_id": result["result_id"],
                        "feature_ref": result["feature_ref"],
                        "definition_hash": result["definition_hash"],
                        "source_ref": result["source_ref"],
                        "producer_run_id": result["producer_run_id"],
                        **data_row,
                    })
                    return True

                self._scan_artifact(
                    result, descriptor, data_columns=data_columns,
                    scope=normalized_scope, on_row=add_row, deadline=deadline,
                )
                if has_more:
                    break
            if has_more:
                break
        truncated = has_more
        next_cursor = _next_cursor(signature, offset + len(rows)) if truncated else None
        health_statuses = {item["status"] for item in verification.values()}
        return {
            "schema_version": "result_rows_v1",
            "result_refs": refs,
            "columns": output_columns,
            "rows": rows,
            "next_cursor": next_cursor,
            "truncated": truncated,
            "checked_at": max(checks) if checks else None,
            "health_verification": "verified" if health_statuses == {"available"} else "invalid",
        }

    def _source(self, source_ref: str) -> dict[str, Any]:
        source_id, version = parse_versioned_ref(source_ref, "source_ref")
        source = self._repository.get_source(source_id, version)
        if source is None:
            raise UnknownReferenceError(f"unknown DataSource ref: {source_ref}")
        return source

    def _requested_intervals(self, study: dict[str, Any], sources: Sequence[dict[str, Any]],
                             scope: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
        query_scope = dict(scope)
        allowed_scopes = study["limits"]["allowed_scope"]
        study_instruments = {instrument for item in allowed_scopes for instrument in item["instruments"]}
        source_instruments = {instrument for source in sources for instrument in source["instruments"]}
        if "instruments" not in query_scope:
            allowed_instruments = study_instruments & source_instruments
        else:
            allowed_instruments = set(query_scope["instruments"])
            if not allowed_instruments.issubset(study_instruments):
                raise SourceScopeMissingError("coverage scope includes an instrument outside Study limits")
            if not allowed_instruments.issubset(source_instruments):
                raise SourceScopeMissingError("coverage scope includes an instrument outside selected sources")
        if query_scope.get("session_filter"):
            for instrument in allowed_instruments:
                instrument_scopes = [
                    allowed_scope for allowed_scope in allowed_scopes
                    if instrument in allowed_scope["instruments"]
                ]
                universal = any("session_filter" not in allowed_scope
                                for allowed_scope in instrument_scopes)
                authorized_sessions = {
                    session
                    for allowed_scope in instrument_scopes
                    for session in allowed_scope.get("session_filter", [])
                }
                if (not universal and authorized_sessions
                        and not set(query_scope["session_filter"]).issubset(authorized_sessions)):
                    raise SourceScopeMissingError(
                        "coverage scope includes a session outside Study limits",
                        details={"instrument_id": instrument,
                                 "sessions": sorted(set(query_scope["session_filter"])
                                                    - authorized_sessions)},
                    )
        if query_scope.get("start") is not None:
            query_start = _timestamp(query_scope["start"], "scope.start")
            query_end = _timestamp(query_scope["end"], "scope.end")
            for instrument in allowed_instruments:
                authorized_ranges = [
                    {"start": item["start"], "end": item["end"]}
                    for item in allowed_scopes if instrument in item["instruments"]
                ]
                requested = {"start": _time_text(query_start), "end": _time_text(query_end)}
                if _subtract(requested, authorized_ranges):
                    raise SourceScopeMissingError(
                        "coverage scope time range exceeds Study limits",
                        details={"instrument_id": instrument},
                    )
        source_sessions: dict[str, set[str]] = {}
        calendar_dates: dict[str, set[str]] = {}
        for source in sources:
            source_ref = f"{source['source_id']}@{source['version']}"
            session_policy = _read_json_ref(source["session_policy_ref"], "source.session_policy_ref")
            calendar = _read_json_ref(source["calendar_ref"], "source.calendar_ref")
            source_sessions[source_ref] = set(session_policy.get("sessions", []))
            calendar_dates[source_ref] = set(calendar.get("trading_dates", []))
        if query_scope.get("session_filter"):
            all_sessions = set().union(*source_sessions.values()) if source_sessions else set()
            unknown_sessions = set(query_scope["session_filter"]) - all_sessions
            if unknown_sessions:
                raise SourceScopeMissingError(
                    "coverage scope references an unknown session",
                    details={"unknown_sessions": sorted(unknown_sessions)},
                )
        if query_scope.get("trade_dates"):
            all_dates = set().union(*calendar_dates.values()) if calendar_dates else set()
            unknown_dates = set(query_scope["trade_dates"]) - all_dates
            if unknown_dates:
                raise SourceScopeMissingError(
                    "coverage scope references a date outside every source calendar",
                    details={"unknown_trade_dates": sorted(unknown_dates)},
                )
        intervals: list[dict[str, Any]] = []
        gaps: list[dict[str, Any]] = []
        diagnostics: list[str] = []
        for source in sources:
            source_ref = f"{source['source_id']}@{source['version']}"
            calendars = _read_json_ref(source["calendar_ref"], "source.calendar_ref")
            session_policy = _read_json_ref(source["session_policy_ref"], "source.session_policy_ref")
            raw_intervals = session_policy.get("intervals")
            if not isinstance(raw_intervals, list):
                diagnostics.append(f"{source_ref}:SESSION_INTERVALS_MISSING")
                continue
            declared = source.get("coverage", {}).get("partitions", [])
            for raw in raw_intervals:
                instrument = raw["instrument_id"]
                day = raw["trade_date"]
                session_id = raw["session_id"]
                if instrument not in allowed_instruments or instrument not in source["instruments"]:
                    continue
                if day not in calendars.get("trading_dates", []):
                    continue
                if query_scope.get("trade_dates") and day not in query_scope["trade_dates"]:
                    continue
                if query_scope.get("session_filter") and session_id not in query_scope["session_filter"]:
                    continue
                known_exchanges = {partition.get("exchange") for partition in declared
                                   if partition["instrument_id"] == instrument
                                   and partition.get("exchange") is not None}
                session_exchange = raw.get("exchange")
                if session_exchange is None and len(known_exchanges) == 1:
                    session_exchange = next(iter(known_exchanges))
                raw_interval = {
                    "exchange": session_exchange, "instrument_id": instrument,
                    "trade_date": day, "session_id": session_id,
                    "start": raw["start"], "end": raw["end"],
                }
                raw_start, raw_end = _bounds(raw_interval)
                overlaps = []
                for authorized_scope in allowed_scopes:
                    if instrument not in authorized_scope["instruments"]:
                        continue
                    allowed_sessions = authorized_scope.get("session_filter")
                    if allowed_sessions is not None and session_id not in allowed_sessions:
                        continue
                    start = max(raw_start, _timestamp(authorized_scope["start"], "allowed_scope.start"))
                    end = min(raw_end, _timestamp(authorized_scope["end"], "allowed_scope.end"))
                    if "start" in query_scope:
                        start = max(start, _timestamp(query_scope["start"], "scope.start"))
                        end = min(end, _timestamp(query_scope["end"], "scope.end"))
                    if start < end:
                        overlaps.append((start, end))
                if {"start", "trade_dates", "session_filter"} & query_scope.keys():
                    desired = {**raw_interval,
                               "start": _time_text(
                                   max(raw_start, _timestamp(query_scope["start"], "scope.start"))
                                   if "start" in query_scope else raw_start
                               ),
                               "end": _time_text(
                                   min(raw_end, _timestamp(query_scope["end"], "scope.end"))
                                   if "end" in query_scope else raw_end
                               )}
                    if desired["start"] < desired["end"] and _subtract(
                        desired,
                        [{**raw_interval, "start": _time_text(start), "end": _time_text(end)}
                         for start, end in overlaps],
                    ):
                        raise SourceScopeMissingError(
                            "coverage scope includes a session outside Study limits",
                            details={"source_ref": source_ref, "instrument_id": instrument,
                                     "trade_date": day, "session_id": session_id},
                        )
                matching = [partition for partition in declared
                            if partition["instrument_id"] == instrument
                            and partition["trade_date"] == day
                            and session_id in partition["session_ids"]
                            and (raw.get("exchange") is None
                                 or partition.get("exchange") in {None, raw.get("exchange")})]
                if session_exchange is None and matching:
                    row_exchanges = {item.get("exchange") for item in matching
                                     if item.get("exchange") is not None}
                    if len(row_exchanges) == 1:
                        raw_interval["exchange"] = next(iter(row_exchanges))
                for start, end in overlaps:
                    candidate = {**raw_interval, "source_ref": source_ref,
                                 "start": _time_text(start), "end": _time_text(end)}
                    intervals.append(candidate)
                if not overlaps:
                    continue
                if not matching:
                    for start, end in overlaps:
                        gaps.append({**raw_interval, "source_ref": source_ref,
                                     "start": _time_text(start), "end": _time_text(end),
                                     "reason": "SOURCE_PARTITION_MISSING"})
        return (_merge_intervals(intervals), _merge_intervals(gaps), diagnostics)

    def _coverage_targets(self, study: dict[str, Any], *, feature_set_ref: str | None,
                          item_ids: Sequence[str] | None) -> list[dict[str, Any]]:
        backlog = study["backlog"]
        if feature_set_ref is not None:
            if feature_set_ref not in study["feature_set_refs"]:
                raise UnknownReferenceError(f"FeatureSet is not registered in Study: {feature_set_ref}")
            set_refs = [feature_set_ref]
            backlog_state = {item["item_id"]: item["state"] for item in backlog
                             if item["feature_set_ref"] == feature_set_ref}
        elif backlog:
            set_refs = sorted({item["feature_set_ref"] for item in backlog})
            backlog_state = {(item["feature_set_ref"], item["item_id"]): item["state"]
                             for item in backlog}
        else:
            if item_ids:
                raise ResearchValidationError("item_ids requires feature_set_ref when Study backlog is empty")
            return []
        requested_ids = None if item_ids is None else set(_strings(item_ids, "item_ids", nonempty=False))
        groups: dict[tuple[str, str, str], dict[str, Any]] = {}
        for ref in set_refs:
            feature_set = self._catalog.get_feature_set(ref)
            if feature_set.get("status") != "frozen":
                raise ResearchValidationError(f"coverage requires a frozen FeatureSet: {ref}")
            known = {item["item_id"] for item in feature_set["resolved_items"]}
            if feature_set_ref is not None:
                selected = known if requested_ids is None else (known & requested_ids)
            else:
                backlog_ids = {item_id for (set_key, item_id) in backlog_state if set_key == ref}
                selected = backlog_ids if requested_ids is None else (backlog_ids & requested_ids)
            for item_id in sorted(selected):
                item = next(item for item in feature_set["resolved_items"]
                            if item["item_id"] == item_id)
                if feature_set_ref is not None:
                    state = backlog_state.get(item_id, "active")
                else:
                    state = backlog_state.get((ref, item_id), "active")
                for output in item["outputs"]:
                    feature = self._catalog.get_feature(output["feature_ref"])
                    key = (output["feature_ref"], output["definition_hash"], state)
                    target = groups.setdefault(key, {
                        "feature_ref": output["feature_ref"],
                        "definition_hash": output["definition_hash"],
                        "item_ids": set(),
                        "feature_set_refs": set(),
                        "backlog_state": state,
                        "kind": feature["kind"],
                    })
                    target["item_ids"].add(item_id)
                    target["feature_set_refs"].add(ref)
        unknown = set() if requested_ids is None else requested_ids - {
            item_id for target in groups.values() for item_id in target["item_ids"]
        }
        if unknown:
            raise UnknownReferenceError("item_ids are not in the requested Study FeatureSet(s)",
                                        details={"unknown_item_ids": sorted(unknown)})
        return [{**target, "item_ids": sorted(target["item_ids"]),
                 "feature_set_refs": sorted(target["feature_set_refs"])}
                for target in groups.values()]

    @staticmethod
    def _work_records(registry: Registry, study: dict[str, Any],
                      requested_intervals: Sequence[Mapping[str, Any]], *,
                      deadline: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        study_id = study["study_id"]
        if requested_intervals:
            start = min(item["start"] for item in requested_intervals)
            end = max(item["end"] for item in requested_intervals)
            instruments = sorted({item["instrument_id"] for item in requested_intervals})
        else:
            allowed = study["limits"]["allowed_scope"]
            if not allowed:
                return [], []
            start = min(item["start"] for item in allowed)
            end = max(item["end"] for item in allowed)
            instruments = sorted({instrument for item in allowed for instrument in item["instruments"]})
        if not instruments:
            return [], []
        work_records: list[dict[str, Any]] = []
        selected_runs: list[dict[str, Any]] = []
        offset = 0
        while True:
            if time.perf_counter() > deadline:
                raise ResearchValidationError(
                    "coverage Run lookup exceeded its time budget",
                    details={"reason": "QUERY_RESOURCE_LIMIT", "limit_seconds": _QUERY_TIMEOUT_S},
                )
            runs, has_more = registry.query_research_runs_page(
                study_id, start=start, end=end, instruments=instruments,
                offset=offset, limit=501,
            )
            if not runs:
                break
            history = registry.get_run_work_history([run["run_id"] for run in runs])
            if time.perf_counter() > deadline:
                raise ResearchValidationError(
                    "coverage Run history lookup exceeded its time budget",
                    details={"reason": "QUERY_RESOURCE_LIMIT", "limit_seconds": _QUERY_TIMEOUT_S},
                )
            for run in runs:
                plan = run.get("research_plan") or {}
                request = run.get("research_request") or {}
                stages = {item.get("work_key"): item for item in
                          history["stages_by_run"].get(run["run_id"], [])}
                from .execution_view import active_linked_result

                run_links = registry.list_run_result_links(run["run_id"])
                links_by_work: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for link in run_links:
                    links_by_work[link["work_key"]].append(link)
                active_result_ids = set()
                for work_key, work_links in links_by_work.items():
                    stage = stages.get(work_key, {})
                    events = history["events_by_stage"].get(stage.get("stage_run_id"), [])
                    active = active_linked_result(work_links, events)
                    if active is not None:
                        active_result_ids.add(active["result_id"])
                selected_runs.append({"run_id": run["run_id"], "request": request, "plan": plan,
                                      "active_result_ids": active_result_ids})
                for work in plan.get("work", []):
                    stage = stages.get(work.get("work_key"), {})
                    events = history["events_by_stage"].get(stage.get("stage_run_id"), [])
                    work_records.append({
                        "run_id": run["run_id"],
                        "feature_set_ref": plan.get("feature_set_ref"),
                        "source_ref": work.get("source_ref"),
                        "feature_ref": work.get("feature_ref"),
                        "definition_hash": work.get("definition_hash"),
                        "item_ids": list(work.get("item_ids", [])),
                        "scope": work.get("partition_scope", {}),
                        "state": stage.get("status", work.get("state", "PENDING")),
                        "attempt": stage.get("attempt", 0),
                        "attempts": [{"state": event["state"], "attempt": event["attempt"],
                                      "created_at": event["created_at"], "fields": event["fields"]}
                                     for event in events],
                        "requested": work.get("role") == "requested",
                        "selected_item_ids": list(plan.get("selected_item_ids", [])),
                    })
            offset += len(runs)
            if not has_more:
                break
            if offset >= 20_000:
                raise ResearchValidationError(
                    "coverage Run history exceeds its safety bound",
                    details={"reason": "QUERY_RESOURCE_LIMIT", "max_runs": 20_000},
                )
        return work_records, selected_runs

    def _candidate_results(self, *, source_ref: str, feature_ref: str,
                           definition_hash: str,
                           requested_intervals: Sequence[Mapping[str, Any]],
                           linked_result_ids: set[str],
                           deadline: float | None = None) -> list[dict[str, Any]]:
        filters_base = {
            "source_ref": source_ref,
            "feature_ref": feature_ref,
            "definition_hash": definition_hash,
        }
        instruments = sorted({item["instrument_id"] for item in requested_intervals})
        filters_by_instrument = []
        for instrument in instruments:
            scoped = [item for item in requested_intervals if item["instrument_id"] == instrument]
            filters_by_instrument.append({
                **filters_base,
                "instrument_id": instrument,
                "start": min(item["start"] for item in scoped),
                "end": max(item["end"] for item in scoped),
            })
        if not filters_by_instrument:
            # Without exact session intervals there is no safe date/time target to
            # claim. Keep a small identity-filtered window for results already linked
            # to this Study so the caller can still explain the missing policy.
            filters_by_instrument = [filters_base]
        results: dict[str, dict[str, Any]] = {}
        for filters in filters_by_instrument:
            offset = 0
            while True:
                if deadline is not None and time.perf_counter() > deadline:
                    raise ResearchValidationError(
                        "coverage result lookup exceeded its time budget",
                        details={"reason": "QUERY_RESOURCE_LIMIT", "limit_seconds": _QUERY_TIMEOUT_S},
                    )
                page, has_more = self._registry.query_feature_results(
                    filters, offset=offset, limit=501,
                )
                for result in page:
                    if result["result_id"] in linked_result_ids:
                        results[result["result_id"]] = result
                offset += len(page)
                if not has_more:
                    break
                if offset >= 20_000:
                    raise ResearchValidationError(
                        "coverage result candidate window exceeds its safety bound",
                        details={"reason": "QUERY_RESOURCE_LIMIT", "max_results": 20_000},
                    )
        return list(results.values())

    def _stored_result_stats(self, result: dict[str, Any],
                             scope: Mapping[str, Any]) -> dict[str, Any]:
        result_scope = result.get("processed_scope", {})
        exact = (
            result_scope.get("instrument_id") in scope.get("instruments", [])
            and result_scope.get("trade_date") in scope.get("trade_dates", [])
            and result_scope.get("session_id") in scope.get("session_filter", [])
            and result_scope.get("start") == scope.get("start")
            and result_scope.get("end") == scope.get("end")
        )
        quality = result.get("quality", {})
        total, valid = quality.get("n_total"), quality.get("n_valid")
        if exact and type(total) is int and type(valid) is int:
            return {"observed_points": total, "valid_points": valid,
                    "na_points": total - valid, "counts_unverified": True}
        return {"observed_points": None, "valid_points": None,
                "na_points": None, "counts_unverified": True}

    def _result_stats(self, result: dict[str, Any], scope: Mapping[str, Any], *,
                      deadline: float | None = None) -> dict[str, int]:
        total = valid = 0
        deadline = deadline if deadline is not None else time.perf_counter() + _QUERY_TIMEOUT_S
        for descriptor in result.get("artifacts", []):
            count, good = self._scan_artifact(
                result, descriptor,
                data_columns=["event_time", "instrument_id", "trade_date", "session_id", "value"],
                scope=scope, deadline=deadline,
            )
            total += count
            valid += good
        return {"observed_points": total, "valid_points": valid, "na_points": total - valid}

    def study_coverage(self, study_id: str, *, revision: int | None = None,
                       source_refs: Sequence[str] | None = None,
                       scope: Mapping[str, Any] | None = None,
                       feature_set_ref: str | None = None,
                       item_ids: Sequence[str] | None = None,
                       verify: bool = True) -> dict[str, Any]:
        if not isinstance(verify, bool):
            raise ResearchValidationError("verify must be a boolean")
        if type(revision) is not int and revision is not None:
            raise ResearchValidationError("revision must be an integer or null")
        if revision is not None and revision < 1:
            raise ResearchValidationError("revision must be >= 1")
        deadline = time.perf_counter() + _QUERY_TIMEOUT_S
        study = self._repository.get_study(study_id, revision)
        if study is None:
            raise UnknownReferenceError(f"unknown Study revision: {study_id}@{revision or 'current'}")
        selected_sources = study["source_refs"] if source_refs is None else _strings(source_refs, "source_refs")
        if not set(selected_sources).issubset(study["source_refs"]):
            raise SourceScopeMissingError("coverage source_refs exceed Study source_refs")
        normalized_scope = _normalize_scope(scope)
        if not normalized_scope and "default_scope" in study:
            normalized_scope = _normalize_scope(study["default_scope"])
        sources = [self._source(ref) for ref in selected_sources]
        requested_intervals, source_gaps, diagnostics = self._requested_intervals(
            study, sources, normalized_scope,
        )
        targets = self._coverage_targets(
            study, feature_set_ref=feature_set_ref, item_ids=item_ids,
        )
        work_records, selected_runs = self._work_records(
            self._registry, study, requested_intervals, deadline=deadline,
        )
        linked_result_ids = {
            link["result_id"]
            for run in selected_runs
            for link in self._registry.list_run_result_links(run["run_id"])
        }
        active_result_ids = {
            result_id for run in selected_runs for result_id in run["active_result_ids"]
        }
        result_checks: dict[str, dict[str, Any] | None] = {}
        target_responses = []
        summary = {
            "target_count": 0,
            "requested_intervals": 0,
            "processed_intervals": 0,
            "available_results": 0,
            "observed_points": 0,
            "valid_points": 0,
            "unprocessed_intervals": 0,
            "source_gaps": len(source_gaps),
            "needs_repair": 0,
            "reason_counts": {},
        }
        reasons = Counter()
        for definition in targets:
            for source_ref in selected_sources:
                if time.perf_counter() > deadline:
                    raise ResearchValidationError(
                        "coverage query exceeded its time budget",
                        details={"reason": "QUERY_RESOURCE_LIMIT", "limit_seconds": _QUERY_TIMEOUT_S},
                    )
                related_intervals = [item for item in requested_intervals
                                     if item["source_ref"] == source_ref]
                related_gaps = [item for item in source_gaps if item["source_ref"] == source_ref]
                matching_work = [record for record in work_records
                                 if record["source_ref"] == source_ref
                                 and record["feature_ref"] == definition["feature_ref"]
                                 and record["definition_hash"] == definition["definition_hash"]
                                 and set(record["item_ids"]) & set(definition["item_ids"])
                                 and any(_scope_overlaps_interval(record["scope"], interval)
                                         is not None for interval in related_intervals)]
                candidate_results = self._candidate_results(
                    source_ref=source_ref,
                    feature_ref=definition["feature_ref"],
                    definition_hash=definition["definition_hash"],
                    requested_intervals=related_intervals,
                    linked_result_ids=linked_result_ids,
                    deadline=deadline,
                )
                for candidate in candidate_results:
                    if candidate.get("result_id") not in result_checks:
                        result_checks[candidate["result_id"]] = self._verify(
                            candidate, verify, deadline,
                        )
                matching_results = []
                for result in candidate_results:
                    result_scope = result.get("processed_scope", {})
                    if not isinstance(result_scope, dict):
                        continue
                    for requested in related_intervals:
                        if (result_scope.get("instrument_id") != requested["instrument_id"]
                                or result_scope.get("trade_date") != requested["trade_date"]
                                or result_scope.get("session_id") != requested["session_id"]
                                or (requested.get("exchange") is not None
                                    and result_scope.get("exchange") != requested.get("exchange"))):
                            continue
                        overlap = _intersect(result_scope, requested)
                        if overlap is not None:
                            matching_results.append((result, overlap))
                matching_results.sort(
                    key=lambda pair: (pair[0].get("created_at", ""), pair[0]["result_id"]),
                    reverse=True,
                )
                matching_results.sort(key=lambda pair:
                                      pair[0]["result_id"] not in active_result_ids)
                matching_results.sort(key=lambda pair: (
                    result_checks[pair[0]["result_id"]] is not None
                    and result_checks[pair[0]["result_id"]]["status"] != "available"
                ))
                processed_by_key: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
                available_by_key: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
                unverified_counted_by_key: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
                processed_fragments = []
                observed_points = valid_points = 0
                result_ids: list[str] = []
                producers = set()
                reason_counts = Counter()
                canonical_hashes = set()
                health_history = []
                counts_unverified = False
                seen_fragments = set()
                for result, overlap in matching_results:
                    result_id = result["result_id"]
                    if result_id not in result_ids:
                        result_ids.append(result_id)
                        producers.add(result.get("producer_run_id"))
                        check = result_checks[result_id]
                        if check is None:
                            previous = result.get("health_checks", [])
                            last = previous[-1] if previous else None
                            health_history.append({
                                "result_id": result_id,
                                "status": "unverified",
                                "checked_at": None if last is None else last.get("checked_at"),
                                "last_status": None if last is None else last.get("status"),
                                "health_unverified_since_check": True,
                            })
                        else:
                            health_history.append({**check,
                                                   "health_unverified_since_check": False})
                    key_interval = {
                        **overlap, "source_ref": source_ref,
                        "definition_hash": definition["definition_hash"],
                    }
                    key = _group_key(key_interval)
                    fragment_key = (result_id, key, overlap["start"], overlap["end"])
                    if fragment_key in seen_fragments:
                        continue
                    seen_fragments.add(fragment_key)
                    processed_by_key[key].append(key_interval)
                    processed_fragments.append(key_interval)
                    health = result_checks[result_id]
                    if health is not None and health["status"] != "available":
                        continue
                    descriptors = result.get("artifacts", [])
                    destination = available_by_key[key] if verify else unverified_counted_by_key[key]
                    remaining = _subtract(key_interval, destination)
                    if verify:
                        destination.append(key_interval)
                    else:
                        destination.append(key_interval)
                    for piece in remaining:
                        stats_scope = {
                            "instruments": [piece["instrument_id"]],
                            "start": piece["start"], "end": piece["end"],
                            "trade_dates": [piece["trade_date"]],
                            "session_filter": [piece["session_id"]],
                        }
                        if verify:
                            stats = self._result_stats(result, stats_scope, deadline=deadline)
                        else:
                            result_scope = result.get("processed_scope", {})
                            exact = all(result_scope.get(key_name) == value
                                        for key_name, value in (
                                            ("instrument_id", piece["instrument_id"]),
                                            ("trade_date", piece["trade_date"]),
                                            ("session_id", piece["session_id"]),
                                            ("start", piece["start"]),
                                            ("end", piece["end"]),
                                        ))
                            stats = self._stored_result_stats(result, stats_scope) if exact else {
                                "observed_points": None, "valid_points": None,
                                "na_points": None, "counts_unverified": True,
                            }
                        if stats["observed_points"] is not None:
                            observed_points += stats["observed_points"]
                            valid_points += stats["valid_points"]
                            if stats["na_points"]:
                                reason_counts["UNCLASSIFIED_NA"] += stats["na_points"]
                        else:
                            counts_unverified = True
                    if verify:
                        canonical_hashes.update(
                            descriptor.get("hash") for descriptor in descriptors
                            if isinstance(descriptor.get("hash"), str)
                        )
                processed_intervals = [
                    _interval_doc(item) | {"source_ref": source_ref}
                    for item in _merge_intervals(processed_fragments)
                ]
                needs_repair = bool(verify) and any(
                    _subtract(item, available_by_key[key])
                    for key, fragments in processed_by_key.items()
                    for item in _merge_intervals(fragments)
                )
                if needs_repair:
                    reasons["NEEDS_REPAIR"] += 1
                    reason_counts["NEEDS_REPAIR"] += 1
                # Preserve planned failure/blocked attempts, even if a later result
                # currently satisfies the interval.
                states = [record["state"] for record in matching_work]
                attempts = [attempt for record in matching_work for attempt in record["attempts"]]
                expected_sets = ({feature_set_ref} if feature_set_ref is not None
                                 else set(definition["feature_set_refs"]))

                def run_applies(run, expected_sets=expected_sets, source_ref=source_ref,
                                related_intervals=related_intervals):
                    request = run["request"]
                    plan = run["plan"]
                    set_ref = request.get("feature_set_ref")
                    if set_ref not in expected_sets or source_ref not in request.get("source_refs", []):
                        return False
                    run_scope = plan.get("scope", {})
                    return any(_scope_overlaps_interval(run_scope, interval) is not None
                               for interval in related_intervals)

                item_was_omitted = any(
                    run_applies(run)
                    and not (set(definition["item_ids"])
                             & set(run["request"].get("selected_item_ids", [])))
                    for run in selected_runs
                )
                unselected = definition["backlog_state"] != "active" or (
                    not matching_work and not matching_results and item_was_omitted
                )
                requested_for_target = related_intervals
                target_gaps = [gap for gap in related_gaps
                               if any(gap["instrument_id"] == interval["instrument_id"]
                                      and gap["trade_date"] == interval["trade_date"]
                                      and gap["session_id"] == interval["session_id"]
                                      for interval in requested_for_target)]
                target_processed = [item for item in processed_intervals
                                    if any(item["instrument_id"] == requested["instrument_id"]
                                           and item["exchange"] == requested["exchange"]
                                           and item["trade_date"] == requested["trade_date"]
                                           and item["session_id"] == requested["session_id"]
                                           for requested in requested_for_target)]
                unprocessed = []
                for requested in requested_for_target:
                    relevant_processed = [item for item in target_processed
                                          if item["instrument_id"] == requested["instrument_id"]
                                          and item["exchange"] == requested["exchange"]
                                          and item["trade_date"] == requested["trade_date"]
                                          and item["session_id"] == requested["session_id"]]
                    unprocessed.extend(_subtract(requested, relevant_processed))
                total_requested_duration = sum(
                    (_bounds(item)[1] - _bounds(item)[0]).total_seconds()
                    for item in requested_for_target
                )
                total_processed_duration = sum(
                    (_bounds(item)[1] - _bounds(item)[0]).total_seconds()
                    for item in target_processed
                )
                all_processed = total_requested_duration > 0 and total_processed_duration >= total_requested_duration
                if unselected:
                    status = "NOT_SELECTED"
                    reasons["NOT_SELECTED"] += 1
                elif needs_repair:
                    status = "NEEDS_REPAIR"
                    reasons["NEEDS_REPAIR"] += 1
                elif all_processed:
                    status = "COMPLETE"
                elif target_processed:
                    status = "PARTIAL"
                elif "FAILED" in states:
                    status = "FAILED"
                elif "BLOCKED" in states or target_gaps:
                    status = "BLOCKED"
                elif "RUNNING" in states:
                    status = "IN_PROGRESS"
                elif matching_work:
                    status = "NOT_STARTED"
                else:
                    status = "NOT_STARTED"
                available = bool(matching_results) and not needs_repair
                if not verify and status not in {"NOT_SELECTED", "NOT_STARTED", "FAILED", "BLOCKED"}:
                    execution_status = status
                    status = "UNVERIFIED"
                else:
                    execution_status = status
                if verify:
                    available = bool(processed_intervals) and not needs_repair
                else:
                    available = None
                checked_times = [item.get("checked_at") for item in health_history
                                 if item.get("checked_at")]
                last_health = max(health_history,
                                  key=lambda item: item.get("checked_at") or "",
                                  default=None)
                if not unselected and not matching_results and target_gaps and not matching_work:
                    status = "BLOCKED"
                    execution_status = "BLOCKED"
                execute_intervals = []
                for requested in requested_for_target:
                    covered = [item for item in target_processed
                               if item["instrument_id"] == requested["instrument_id"]
                               and item["exchange"] == requested["exchange"]
                               and item["trade_date"] == requested["trade_date"]
                               and item["session_id"] == requested["session_id"]]
                    gaps_for_interval = [item for item in target_gaps
                                         if item["instrument_id"] == requested["instrument_id"]
                                         and item["exchange"] == requested["exchange"]
                                         and item["trade_date"] == requested["trade_date"]
                                         and item["session_id"] == requested["session_id"]]
                    execution_residual = _subtract(requested, [*covered, *gaps_for_interval])
                    execute_intervals.extend(execution_residual)
                if unselected or (all_processed and not needs_repair):
                    suggested_work = []
                else:
                    suggested_work = []
                    if needs_repair:
                        suggested_work.append({"action": "repair", "source_ref": source_ref,
                                               "feature_ref": definition["feature_ref"],
                                               "definition_hash": definition["definition_hash"],
                                               "unprocessed_intervals": [_interval_doc(item)
                                                                         for item in unprocessed]})
                    if execute_intervals:
                        suggested_work.append({"action": "execute", "source_ref": source_ref,
                                               "feature_ref": definition["feature_ref"],
                                               "definition_hash": definition["definition_hash"],
                                               "unprocessed_intervals": [_interval_doc(item)
                                                                         for item in execute_intervals]})
                    if target_gaps:
                        suggested_work.append({"action": "resolve_source_gap",
                                               "source_ref": source_ref,
                                               "source_gaps": [_interval_doc(item)
                                                               for item in target_gaps]})
                result_ids.sort(key=lambda result_id: (
                    next((item.get("created_at", "") for item in candidate_results
                          if item["result_id"] == result_id), ""), result_id,
                ))
                health_history.sort(key=lambda item: result_ids.index(item["result_id"]))
                target = {
                    "feature_ref": definition["feature_ref"],
                    "definition_hash": definition["definition_hash"],
                    "source_ref": source_ref,
                    "item_ids": definition["item_ids"],
                    "feature_set_refs": definition["feature_set_refs"],
                    "kind": definition["kind"],
                    "backlog_state": definition["backlog_state"],
                    "status": status,
                    "execution_status": execution_status,
                    "available": available,
                    "health_verification": {
                        "status": "verified" if verify else "unverified",
                        "checked_at": max(checked_times) if checked_times else None,
                        "last_status": (None if last_health is None else
                                        last_health.get("status") if verify else
                                        last_health.get("last_status")),
                        "health_unverified_since_check": not verify,
                    },
                    "counts_unverified": counts_unverified or not verify,
                    "needs_repair": needs_repair,
                    "requested_intervals": [_interval_doc(item) for item in requested_for_target],
                    "processed_intervals": target_processed,
                    "unprocessed_intervals": [_interval_doc(item) for item in unprocessed],
                    "source_gaps": [_interval_doc(item) | {"reason": item["reason"]}
                                    for item in target_gaps],
                    "observed_points": None if counts_unverified else observed_points,
                    "valid_points": None if counts_unverified else valid_points,
                    "na_points": (None if counts_unverified else
                                  max(0, observed_points - valid_points)),
                    "reason_counts": dict(sorted(reason_counts.items())),
                    "result_refs": result_ids,
                    "active_result_refs": [item for item in result_ids if item in active_result_ids],
                    "available_result_refs": [
                        item for item in result_ids
                        if item in active_result_ids
                        and result_checks[item] is not None
                        and result_checks[item]["status"] == "available"
                    ] if verify else [],
                    "health_history": health_history,
                    "producer_run_ids": sorted(item for item in producers if item),
                    "attempts": attempts,
                    "suggested_work": [] if all_processed and not needs_repair else
                        suggested_work,
                    "diagnostics": [reason for reason in diagnostics
                                    if reason.startswith(source_ref + ":")],
                    "overlap_content_hashes": sorted(canonical_hashes),
                }
                target_responses.append(target)
                summary["target_count"] += 1
                summary["requested_intervals"] += len(target["requested_intervals"])
                summary["processed_intervals"] += len(target["processed_intervals"])
                summary["available_results"] += len(target["available_result_refs"])
                if not counts_unverified:
                    summary["observed_points"] += observed_points
                    summary["valid_points"] += valid_points
                summary["unprocessed_intervals"] += len(target["unprocessed_intervals"])
                summary["needs_repair"] += int(needs_repair)
                reasons.update(reason_counts)
        summary["reason_counts"] = dict(sorted(reasons.items()))
        stored_check_times = [target["health_verification"]["checked_at"]
                              for target in target_responses
                              if target["health_verification"].get("checked_at")]
        return {
            "schema_version": "study_coverage_v1",
            "study_id": study_id,
            "revision": study["revision"],
            "source_refs": list(selected_sources),
            "scope": normalized_scope,
            "targets": target_responses,
            "summary": summary,
            "health_verification": "verified" if verify else "unverified",
            "health_unverified_since_check": not verify,
            "checked_at": utc_now() if verify else
                (max(stored_check_times) if stored_check_times else None),
        }

    def add_review(self, document: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(document, Mapping):
            raise ResearchValidationError("Review must be an object")
        raw = dict(document)
        required = {"review_id", "study_id", "actor", "run_refs", "result_refs", "scope",
                    "observations", "decision", "rationale", "limitations"}
        optional = {"schema_version", "target_item_refs", "scores", "next_action",
                    "supersedes_review_id"}
        missing, unknown = required - raw.keys(), raw.keys() - required - optional
        if missing:
            raise ResearchValidationError("Review missing fields: " + ", ".join(sorted(missing)))
        if unknown:
            raise ResearchValidationError("Review has unknown fields: " + ", ".join(sorted(unknown)))
        if raw.get("schema_version", "review_v1") != "review_v1":
            raise ResearchValidationError("Review schema_version must be review_v1")
        review_id = _text(raw["review_id"], "review.review_id")
        if not _REVIEW_ID.fullmatch(review_id):
            raise ResearchValidationError("review.review_id must be a stable identifier")
        study_id = _text(raw["study_id"], "review.study_id")
        study = self._repository.get_study(study_id)
        if study is None:
            raise UnknownReferenceError(f"unknown Study: {study_id}")
        actor = raw["actor"]
        if not isinstance(actor, Mapping) or set(actor) != {"kind", "id"}:
            raise ResearchValidationError("review.actor must contain exactly kind and id")
        if actor["kind"] not in {"human", "agent"}:
            raise ResearchValidationError("review.actor.kind must be human or agent")
        actor = {"kind": actor["kind"], "id": _text(actor["id"], "review.actor.id")}
        run_refs = _strings(raw["run_refs"], "review.run_refs")
        result_refs = _strings(raw["result_refs"], "review.result_refs", nonempty=False)
        if len(run_refs) > _MAX_REVIEW_REFS or len(result_refs) > _MAX_REVIEW_REFS:
            raise ResearchValidationError(
                f"Review run_refs and result_refs may contain at most {_MAX_REVIEW_REFS} refs each"
            )
        scope = _normalize_scope(raw["scope"], required=True)
        referenced_runs = []
        source_refs_for_runs = set()
        for run_id in run_refs:
            run = self._registry.get_run(run_id)
            if run is None or run.get("research_study_id") is None:
                raise UnknownReferenceError(f"unknown research Run: {run_id}")
            if run["research_study_id"] != study_id:
                raise ResearchValidationError(f"Run {run_id} does not belong to Study {study_id}")
            run_scope = run.get("research_plan", {}).get("scope")
            run_request = run.get("research_request", {})
            if not isinstance(run_scope, dict):
                raise ResearchValidationError(f"Run {run_id} has no frozen scope")
            referenced_runs.append({
                "run_id": run_id,
                "scope": run_scope,
                "source_refs": list(run_request.get("source_refs", [])),
            })
            source_refs_for_runs.update(run_request.get("source_refs", []))
        sources = [self._source(ref) for ref in sorted(source_refs_for_runs)]
        review_intervals, _, _ = self._requested_intervals(study, sources, scope)
        if review_intervals:
            for interval in review_intervals:
                allowed_by_runs = []
                for run in referenced_runs:
                    run_scope = run["scope"]
                    if interval["source_ref"] not in run["source_refs"]:
                        continue
                    if interval["instrument_id"] not in run_scope.get("instruments", []):
                        continue
                    sessions = set(run_scope.get("session_filter", []))
                    if sessions and interval["session_id"] not in sessions:
                        continue
                    allowed_by_runs.append({**interval,
                                            "start": max(interval["start"], run_scope["start"]),
                                            "end": min(interval["end"], run_scope["end"])})
                allowed_by_runs = [item for item in allowed_by_runs
                                   if item["start"] < item["end"]]
                if _subtract(interval, allowed_by_runs):
                    raise SourceScopeMissingError(
                        "Review scope exceeds the union of referenced Run scopes",
                        details={"source_ref": interval["source_ref"],
                                 "trade_date": interval["trade_date"],
                                 "session_id": interval["session_id"]},
                    )
        elif not any(
            run["scope"].get("start") <= scope["start"]
            and scope["end"] <= run["scope"].get("end")
            and set(scope["instruments"]).issubset(run["scope"].get("instruments", []))
            for run in referenced_runs
        ):
            raise SourceScopeMissingError("Review scope exceeds referenced Run scopes")
        cited_results = []
        for result_id in result_refs:
            result = self._get_result(result_id)
            if not any(
                link["result_id"] == result_id
                for run_id in run_refs
                for link in self._registry.list_run_result_links(run_id)
            ):
                raise ResearchValidationError(
                    f"Review FeatureResult {result_id} is not linked to a referenced Run",
                    details={"result_id": result_id},
                )
            cited_results.append(result)
        if result_refs:
            if not review_intervals:
                raise SourceScopeMissingError(
                    "Review result scope cannot be checked without exact source session intervals"
                )
            cited_by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for result in cited_results:
                result_scope = result.get("processed_scope") or result.get("scope")
                if not isinstance(result_scope, dict):
                    raise ResearchValidationError(
                        f"FeatureResult {result['result_id']} has no processed scope"
                    )
                cited_by_source.setdefault(result["source_ref"], []).append(result_scope)
            for interval in review_intervals:
                evidence_intervals = []
                for result_scope in cited_by_source.get(interval["source_ref"], []):
                    if (result_scope.get("instrument_id") != interval["instrument_id"]
                            or result_scope.get("trade_date") != interval["trade_date"]
                            or result_scope.get("session_id") != interval["session_id"]
                            or (interval.get("exchange") is not None
                                and result_scope.get("exchange") != interval.get("exchange"))):
                        continue
                    overlap = _intersect(result_scope, interval)
                    if overlap is not None:
                        evidence_intervals.append(overlap)
                if _subtract(interval, evidence_intervals):
                    raise SourceScopeMissingError(
                        "cited FeatureResult scopes do not cover Review scope",
                        details={"source_ref": interval["source_ref"],
                                 "trade_date": interval["trade_date"],
                                 "session_id": interval["session_id"]},
                    )
        observations = raw["observations"]
        if isinstance(observations, str):
            _text(observations, "review.observations")
        elif (not isinstance(observations, list) or not observations
              or any(not isinstance(item, Mapping) for item in observations)):
            raise ResearchValidationError("review.observations must be non-empty text or objects")
        if raw["decision"] not in {"keep", "investigate", "park"}:
            raise ResearchValidationError("review.decision must be keep, investigate, or park")
        rationale = _text(raw["rationale"], "review.rationale")
        limitations = _text(raw["limitations"], "review.limitations")
        normalized = {
            **raw,
            "schema_version": "review_v1",
            "review_id": review_id,
            "study_id": study_id,
            "actor": actor,
            "run_refs": run_refs,
            "result_refs": result_refs,
            "scope": scope,
            "rationale": rationale,
            "limitations": limitations,
        }
        if "target_item_refs" in raw:
            target_refs = _strings(raw["target_item_refs"], "review.target_item_refs", nonempty=False)
            for target_ref in target_refs:
                match = re.fullmatch(r"([^/#]+@[1-9][0-9]*)/([^/#]+)(?:/([^/#]+))?", target_ref)
                if match is None:
                    raise ResearchValidationError(
                        "target_item_refs must be feature_set_ref/item_id[/output_id]"
                    )
                set_ref, item_id, output_id = match.groups()
                if set_ref not in study["feature_set_refs"]:
                    raise UnknownReferenceError(f"FeatureSet is not registered in Study: {set_ref}")
                feature_set = self._catalog.get_feature_set(set_ref)
                item = next((entry for entry in feature_set["resolved_items"]
                             if entry["item_id"] == item_id), None)
                if item is None or (output_id is not None
                                    and output_id not in {output["output_id"] for output in item["outputs"]}):
                    raise UnknownReferenceError(f"unknown Review target item: {target_ref}")
            normalized["target_item_refs"] = target_refs
        if "scores" in raw:
            if not isinstance(raw["scores"], list) or any(not isinstance(item, Mapping)
                                                           for item in raw["scores"]):
                raise ResearchValidationError("review.scores must be an array of objects")
            if len(raw["scores"]) > _MAX_REVIEW_REFS:
                raise ResearchValidationError(f"review.scores may contain at most {_MAX_REVIEW_REFS} entries")
            score_documents = []
            result_by_id = {result["result_id"]: result for result in cited_results}
            artifact_to_result = {
                descriptor["artifact_ref"]["artifact_id"]: result["result_id"]
                for result in cited_results
                for descriptor in result.get("artifacts", [])
                if isinstance(descriptor, dict)
                and isinstance(descriptor.get("artifact_ref"), dict)
                and isinstance(descriptor["artifact_ref"].get("artifact_id"), str)
            }
            for index, score in enumerate(raw["scores"]):
                allowed_score = {"metric_ref", "metric_version", "value", "scope", "evidence_refs"}
                if set(score) - allowed_score or not {"metric_ref", "value", "scope", "evidence_refs"}.issubset(score):
                    raise ResearchValidationError(f"review.scores[{index}] has invalid fields")
                metric_ref = _text(score["metric_ref"], f"review.scores[{index}].metric_ref")
                metric = self._catalog.get_metric(metric_ref)
                if ("metric_version" in score
                        and str(score["metric_version"]) != str(metric.get("version"))):
                    raise ResearchValidationError(
                        f"review.scores[{index}].metric_version does not match metric_ref"
                    )
                if (isinstance(score["value"], bool) or not isinstance(score["value"], (int, float))
                        or not math.isfinite(score["value"])):
                    raise ResearchValidationError(f"review.scores[{index}].value must be finite numeric")
                score_scope = _normalize_scope(score["scope"], required=True)
                if (not scope_is_within(score_scope, scope)
                        or (scope.get("trade_dates") is not None
                            and (score_scope.get("trade_dates") is None
                                 or not set(score_scope["trade_dates"]).issubset(
                                     scope["trade_dates"])))):
                    raise SourceScopeMissingError(
                        f"review.scores[{index}].scope exceeds Review scope"
                    )
                evidence_refs = _strings(score["evidence_refs"],
                                         f"review.scores[{index}].evidence_refs")
                evidence_results = set()
                evidence_runs = set()
                for evidence_ref in evidence_refs:
                    if evidence_ref in run_refs:
                        evidence_runs.add(evidence_ref)
                        continue
                    if evidence_ref in result_refs:
                        evidence_results.add(evidence_ref)
                        continue
                    if evidence_ref in artifact_to_result:
                        evidence_results.add(artifact_to_result[evidence_ref])
                        continue
                    raise UnknownReferenceError(
                        f"unknown Review score evidence ref: {evidence_ref}",
                        details={"evidence_ref": evidence_ref},
                    )
                if evidence_results:
                    score_intervals, _, _ = self._requested_intervals(
                        study, sources, score_scope,
                    )
                    for interval in score_intervals:
                        evidence_intervals = []
                        for result_id in evidence_results:
                            result = result_by_id[result_id]
                            result_scope = result.get("processed_scope", {})
                            if (result.get("source_ref") != interval["source_ref"]
                                    or result_scope.get("instrument_id") != interval["instrument_id"]
                                    or result_scope.get("trade_date") != interval["trade_date"]
                                    or result_scope.get("session_id") != interval["session_id"]
                                    or (interval.get("exchange") is not None
                                        and result_scope.get("exchange") != interval["exchange"])):
                                continue
                            overlap = _intersect(result_scope, interval)
                            if overlap is not None:
                                evidence_intervals.append(overlap)
                        if _subtract(interval, evidence_intervals):
                            raise SourceScopeMissingError(
                                f"review.scores[{index}] evidence does not cover its scope"
                            )
                elif evidence_runs:
                    score_intervals, _, _ = self._requested_intervals(
                        study, sources, score_scope,
                    )
                    for interval in score_intervals:
                        run_intervals = []
                        for run in referenced_runs:
                            if run["run_id"] not in evidence_runs:
                                continue
                            run_scope = run["scope"]
                            if interval["source_ref"] not in run["source_refs"]:
                                continue
                            if interval["instrument_id"] not in run_scope.get("instruments", []):
                                continue
                            sessions = set(run_scope.get("session_filter") or [])
                            if sessions and interval["session_id"] not in sessions:
                                continue
                            overlap = _intersect(interval, {
                                "start": run_scope["start"], "end": run_scope["end"],
                            })
                            if overlap is not None:
                                run_intervals.append(overlap)
                        if _subtract(interval, run_intervals):
                            raise SourceScopeMissingError(
                                f"review.scores[{index}] Run evidence does not cover its scope"
                            )
                score_doc = {**score, "metric_ref": metric_ref, "scope": score_scope,
                             "evidence_refs": evidence_refs}
                if "metric_version" in score:
                    score_doc["metric_version"] = str(metric.get("version"))
                score_documents.append(score_doc)
            normalized["scores"] = score_documents
        if "next_action" in raw and not isinstance(raw["next_action"], Mapping):
            raise ResearchValidationError("review.next_action must be an object")
        if "supersedes_review_id" in raw:
            normalized["supersedes_review_id"] = _text(raw["supersedes_review_id"],
                                                       "review.supersedes_review_id")
        return self._repository.add_review(normalized)

    def list_reviews(self, study_id: str, *, run_id: str | None = None,
                     result_ref: str | None = None, limit: int = 100,
                     cursor: str | None = None) -> dict[str, Any]:
        study_id = _text(study_id, "study_id")
        if self._repository.get_study(study_id) is None:
            raise UnknownReferenceError(f"unknown Study: {study_id}")
        if run_id is not None:
            run_id = _text(run_id, "run_id")
            run = self._registry.get_run(run_id)
            if run is None or run.get("research_study_id") != study_id:
                raise UnknownReferenceError(f"unknown Run in Study {study_id}: {run_id}")
        if result_ref is not None:
            result_ref = _text(result_ref, "result_ref")
            self._get_result(result_ref)
        offset, page_limit, signature = _page_args(
            limit, cursor, maximum=_MAX_LIST_LIMIT,
            query={"study_id": study_id, "run_id": run_id, "result_ref": result_ref},
        )
        page, has_more = self._repository.list_reviews_page(
            study_id, run_id=run_id, result_ref=result_ref,
            offset=offset, limit=page_limit + 1,
        )
        items = page[:page_limit]
        truncated = has_more or len(page) > page_limit
        return {
            "schema_version": "review_page_v1",
            "study_id": study_id,
            "items": items,
            "next_cursor": _next_cursor(signature, offset + len(items)) if truncated else None,
            "truncated": truncated,
        }

    def _run_results(self, run_id: str, *, verify: bool,
                    deadline: float | None = None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        run = self._registry.get_run(run_id)
        if run is None or run.get("research_study_id") is None:
            raise UnknownReferenceError(f"unknown research Run: {run_id}")
        from .execution_view import active_linked_result

        links = self._registry.list_run_result_links(run_id)
        links_by_work: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for link in links:
            links_by_work[link["work_key"]].append(link)
        history = self._registry.get_run_work_history([run_id])
        if deadline is not None and time.perf_counter() > deadline:
            raise ResearchValidationError(
                "Run comparison lookup exceeded its time budget",
                details={"reason": "QUERY_RESOURCE_LIMIT", "limit_seconds": _QUERY_TIMEOUT_S},
            )
        stages = {stage.get("work_key"): stage for stage in
                  history["stages_by_run"].get(run_id, [])}
        results = []
        for work in run.get("research_plan", {}).get("work", []):
            stage = stages.get(work.get("work_key"), {})
            stage_id = stage.get("stage_run_id")
            events = history["events_by_stage"].get(stage_id, [])
            active = active_linked_result(
                links_by_work.get(work.get("work_key", ""), []), events,
            )
            if active is not None:
                results.append(active)
        if verify:
            for result in results:
                result["query_health"] = self._verify(result, True, deadline)
        if deadline is not None and time.perf_counter() > deadline:
            raise ResearchValidationError(
                "Run comparison exceeded its time budget",
                details={"reason": "QUERY_RESOURCE_LIMIT", "limit_seconds": _QUERY_TIMEOUT_S},
            )
        return run, results

    def compare_runs(self, left_run_id: str, right_run_id: str, *,
                     scope: Mapping[str, Any] | None = None,
                     verify: bool = True) -> dict[str, Any]:
        if not isinstance(verify, bool):
            raise ResearchValidationError("verify must be a boolean")
        deadline = time.perf_counter() + _QUERY_TIMEOUT_S
        left_run, left_results = self._run_results(left_run_id, verify=verify, deadline=deadline)
        right_run, right_results = self._run_results(right_run_id, verify=verify, deadline=deadline)
        if left_run["research_study_id"] != right_run["research_study_id"]:
            raise ResearchValidationError("runs.compare requires both Runs to belong to the same Study")
        left_run_scope = left_run.get("research_plan", {}).get("scope", {})
        right_run_scope = right_run.get("research_plan", {}).get("scope", {})
        if not left_run_scope or not right_run_scope:
            raise ResearchValidationError("Run comparison requires frozen scopes")
        supplied = _normalize_scope(scope)
        def side_scope(run_scope: Mapping[str, Any], label: str) -> dict[str, Any]:
            start = _timestamp(run_scope["start"], f"{label}.scope.start")
            end = _timestamp(run_scope["end"], f"{label}.scope.end")
            if "start" in supplied:
                start = max(start, _timestamp(supplied["start"], "scope.start"))
                end = min(end, _timestamp(supplied["end"], "scope.end"))
            instruments = set(run_scope.get("instruments", []))
            if "instruments" in supplied:
                instruments &= set(supplied["instruments"])
            if start >= end:
                end = start
                instruments = set()
            run_sessions = run_scope.get("session_filter")
            sessions = None if run_sessions is None else set(run_sessions)
            if "session_filter" in supplied:
                sessions = (sessions & set(supplied["session_filter"])
                            if sessions is not None else set(supplied["session_filter"]))
            result = {"instruments": sorted(instruments), "start": _time_text(start),
                      "end": _time_text(end)}
            if sessions is not None:
                result["session_filter"] = sorted(sessions)
            if "trade_dates" in supplied:
                result["trade_dates"] = supplied["trade_dates"]
            return result

        left_scope = side_scope(left_run_scope, "left_run")
        right_scope = side_scope(right_run_scope, "right_run")
        common_start = max(_timestamp(left_scope["start"], "left_scope.start"),
                           _timestamp(right_scope["start"], "right_scope.start"))
        common_end = min(_timestamp(left_scope["end"], "left_scope.end"),
                         _timestamp(right_scope["end"], "right_scope.end"))
        common_instruments = sorted(set(left_scope["instruments"]) & set(right_scope["instruments"]))
        if common_start >= common_end:
            common_end = common_start
            common_instruments = []
        left_sessions = (None if "session_filter" not in left_scope
                         else set(left_scope["session_filter"]))
        right_sessions = (None if "session_filter" not in right_scope
                          else set(right_scope["session_filter"]))
        if left_sessions is not None and right_sessions is not None:
            common_sessions = sorted(left_sessions & right_sessions)
        else:
            common_sessions = sorted(left_sessions if left_sessions is not None else
                                     right_sessions if right_sessions is not None else [])
        common_scope = {
            "instruments": common_instruments,
            "start": _time_text(common_start), "end": _time_text(common_end),
        }
        if left_sessions is not None or right_sessions is not None:
            common_scope["session_filter"] = common_sessions
        if "trade_dates" in supplied:
            common_scope["trade_dates"] = supplied["trade_dates"]
        if "trade_dates" in left_scope and "trade_dates" in right_scope:
            common_scope["trade_dates"] = sorted(
                set(left_scope["trade_dates"]) & set(right_scope["trade_dates"])
            )

        def filtered(results, applied_scope):
            output = []
            for result in results:
                if result.get("query_health", {}).get("status") not in {None, "available"}:
                    continue
                identity = (result.get("feature_ref"), result.get("definition_hash"),
                            result.get("source_ref"), result.get("split_ref", {}).get("split_id"))
                result_scope = result.get("processed_scope", {})
                if result_scope.get("instrument_id") not in applied_scope["instruments"]:
                    continue
                if applied_scope.get("trade_dates") and result_scope.get("trade_date") not in applied_scope["trade_dates"]:
                    continue
                if ("session_filter" in applied_scope
                        and result_scope.get("session_id") not in applied_scope["session_filter"]):
                    continue
                overlap = _intersect(result_scope, {
                    "start": applied_scope["start"], "end": applied_scope["end"],
                })
                if overlap is not None:
                    output.append((identity, result, overlap))
            return output

        left = filtered(left_results, left_scope)
        right = filtered(right_results, right_scope)
        left_def = {item[0] for item in left}
        right_def = {item[0] for item in right}
        common_def = sorted(left_def & right_def)
        added_left, added_right = sorted(left_def - right_def), sorted(right_def - left_def)
        date_key = lambda item: (item[1].get("source_ref"),
                                 item[2].get("instrument_id"),
                                 item[2].get("exchange"),
                                 item[2].get("trade_date"),
                                 item[2].get("session_id"))
        left_dates = {date_key(item) for item in left}
        right_dates = {date_key(item) for item in right}
        encode_date = lambda value: {
            "source_ref": value[0], "instrument_id": value[1], "exchange": value[2],
            "trade_date": value[3], "session_id": value[4],
        }
        common_dates = sorted(left_dates & right_dates)
        added_dates_left, added_dates_right = sorted(left_dates - right_dates), sorted(right_dates - left_dates)
        common_definition_docs = [{"feature_ref": key[0], "definition_hash": key[1],
                                   "source_ref": key[2], "split_id": key[3]}
                                  for key in common_def]
        quality_differences = []
        right_by_key = {(item[0], date_key(item)): item for item in right}
        for left_item in left:
            key = (left_item[0], date_key(left_item))
            right_item = right_by_key.get(key)
            if right_item is None:
                continue
            common = _intersect(left_item[2], right_item[2])
            if common is None:
                continue
            if common_scope["instruments"] and common["instrument_id"] not in common_scope["instruments"]:
                continue
            common = _intersect(common, common_scope)
            if common is None:
                continue
            if common_scope.get("trade_dates") and common.get("trade_date") not in common_scope["trade_dates"]:
                continue
            if ("session_filter" in common_scope
                    and common.get("session_id") not in common_scope["session_filter"]):
                continue
            stats_scope = {
                "instruments": [common["instrument_id"]],
                "start": common["start"], "end": common["end"],
                "trade_dates": [common["trade_date"]],
                "session_filter": [common["session_id"]],
            }
            left_stats = (self._result_stats(left_item[1], stats_scope, deadline=deadline) if verify
                          else self._stored_result_stats(left_item[1], stats_scope))
            right_stats = (self._result_stats(right_item[1], stats_scope, deadline=deadline) if verify
                           else self._stored_result_stats(right_item[1], stats_scope))
            quality_differences.append({
                "definition": {"feature_ref": key[0][0], "definition_hash": key[0][1],
                               "source_ref": key[0][2], "split_id": key[0][3]},
                "date": encode_date(date_key(left_item)),
                "scope": _interval_doc(common),
                "left": left_stats,
                "right": right_stats,
                "delta": {field: right_stats[field] - left_stats[field]
                          if type(left_stats[field]) is int and type(right_stats[field]) is int
                          else None
                          for field in ("observed_points", "valid_points", "na_points")},
            })
        def definitions(keys):
            return [{"feature_ref": key[0], "definition_hash": key[1],
                     "source_ref": key[2], "split_id": key[3]} for key in keys]
        return {
            "schema_version": "run_comparison_v1",
            "left_run_id": left_run_id,
            "right_run_id": right_run_id,
            "scope": {"left": left_scope, "right": right_scope, "common": common_scope},
            "common_dates": [encode_date(item) for item in common_dates],
            "added_dates": {"left": [encode_date(item) for item in added_dates_left],
                             "right": [encode_date(item) for item in added_dates_right]},
            "common_definitions": common_definition_docs,
            "added_definitions": {"left": definitions(added_left),
                                   "right": definitions(added_right)},
            "quality_differences": quality_differences,
            "unmatched_left": definitions(added_left),
            "unmatched_right": definitions(added_right),
            "health_verification": "verified" if verify else "unverified",
            "result_refs": {"left": sorted({item[1]["result_id"] for item in left}),
                            "right": sorted({item[1]["result_id"] for item in right})},
            "historical_result_refs": {
                "left": list(dict.fromkeys(
                    link["result_id"] for link in self._registry.list_run_result_links(left_run_id)
                )),
                "right": list(dict.fromkeys(
                    link["result_id"] for link in self._registry.list_run_result_links(right_run_id)
                )),
            },
            "checked_at": utc_now() if verify else None,
        }
