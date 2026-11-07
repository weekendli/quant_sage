"""Range-aware, side-effect-free research planning and atomic Run registration."""

from __future__ import annotations

import hashlib
import platform
import re
import subprocess
import sys
import tomllib
import uuid
from datetime import UTC, date, datetime, timedelta
from importlib import metadata
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from quantsage_discovery.core.registry import Registry
from quantsage_discovery.core.types import StageStatus
from quantsage_discovery.core.utils import canonical_json

from .catalog import FeatureCatalog
from .errors import (
    BudgetExceededError,
    IdempotencyConflictError,
    PlanChangedError,
    ResearchValidationError,
    RevisionConflictError,
    SourceScopeMissingError,
    StudyNotActiveError,
    UnknownReferenceError,
)
from .feature_schemas import parse_feature_ref
from .repository import ResearchRepository
from .schemas import (
    _actor,
    _read_json_ref,
    _scope,
    _timestamp,
    parse_versioned_ref,
    source_access_policy,
)

_REQUEST_SCHEMA = "run_request_v1"
_PLAN_SCHEMA = "run_plan_v1"
_BUDGET_KEYS = {
    "max_work_units", "max_partitions", "max_cpu_seconds", "max_memory_bytes",
    "max_concurrent_jobs",
}
_SPLIT_ROLES = {
    "train", "development", "validation", "test", "holdout", "final_holdout",
    "research", "research_only", "unassigned_research_only", "discovery",
}
_UNSAFE_SPLIT_ROLES = {"test", "holdout", "final_holdout"}
_DURATION_RE = re.compile(r"^(\d+)ns$")


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _utc_timestamp(value: Any, path: str) -> datetime:
    parsed = _timestamp(value, path)
    return parsed.astimezone(UTC)


def _time_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _duration(value: Any, path: str) -> timedelta:
    if value is None:
        return timedelta(0)
    if not isinstance(value, str):
        raise ResearchValidationError(f"{path} must be a normalized duration string")
    match = _DURATION_RE.fullmatch(value)
    if match is None:
        raise ResearchValidationError(f"{path} must be a normalized duration string")
    nanoseconds = int(match.group(1))
    # datetime has microsecond precision. Round upward so a sub-microsecond tail is
    # included in the planned historical range.
    microseconds = (nanoseconds + 999) // 1_000
    return timedelta(microseconds=microseconds)


def _runtime_snapshot() -> tuple[str, str, dict[str, Any]]:
    package_root = Path(__file__).resolve().parents[2]
    source_files = sorted((package_root / "quantsage_discovery").rglob("*.py"))
    tree = hashlib.sha256()
    for path in source_files:
        tree.update(path.relative_to(package_root).as_posix().encode("utf-8"))
        tree.update(b"\0")
        tree.update(path.read_bytes())
        tree.update(b"\0")
    tree_hash = "sha256:" + tree.hexdigest()
    try:
        git_revision = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=package_root, check=True,
            capture_output=True, text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        git_revision = None
    code_revision = f"git:{git_revision}+tree:{tree_hash}" if git_revision else f"tree:{tree_hash}"
    try:
        package_version = metadata.version("quantsage-discovery")
    except metadata.PackageNotFoundError:
        project = tomllib.loads((package_root / "pyproject.toml").read_text(encoding="utf-8"))
        package_version = project["project"]["version"]
    dependencies = {}
    for distribution in ("duckdb", "pyarrow", "PyYAML"):
        try:
            dependencies[distribution] = metadata.version(distribution)
        except metadata.PackageNotFoundError:
            dependencies[distribution] = None
    environment = {
        "schema_version": "runtime_environment_v1",
        "package": "quantsage-discovery",
        "package_version": package_version,
        "python_version": sys.version,
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "dependencies": dependencies,
        "code_revision": code_revision,
        "source_tree_hash": tree_hash,
    }
    return code_revision, _hash(environment), environment


def _validated_scope(value: Any) -> dict[str, Any]:
    scope = _scope(value, "request.scope")
    scope["instruments"] = sorted(scope["instruments"])
    if "session_filter" in scope:
        scope["session_filter"] = sorted(scope["session_filter"])
    scope["start"] = _time_text(_utc_timestamp(scope["start"], "request.scope.start"))
    scope["end"] = _time_text(_utc_timestamp(scope["end"], "request.scope.end"))
    return scope


def _validate_request_shape(request: Any) -> dict[str, Any]:
    required = {
        "schema_version", "study_id", "study_revision", "feature_set_ref", "source_refs",
        "split_ref", "scope", "allowed_steps", "missing_dependencies", "reuse_policy",
        "budget", "created_by",
    }
    optional = {"selected_item_ids", "parent_run_id", "adopted_review_refs", "seed"}
    if not isinstance(request, dict):
        raise ResearchValidationError("request must be an object")
    if any(not isinstance(key, str) for key in request):
        raise ResearchValidationError("request object keys must be strings")
    missing, unknown = required - request.keys(), request.keys() - required - optional
    if missing:
        raise ResearchValidationError(f"request missing fields: {', '.join(sorted(missing))}")
    if unknown:
        raise ResearchValidationError(f"request has unknown fields: {', '.join(sorted(unknown))}")
    if request["schema_version"] != _REQUEST_SCHEMA:
        raise ResearchValidationError(f"request.schema_version must be {_REQUEST_SCHEMA}")
    study_id = request["study_id"]
    if not isinstance(study_id, str) or not study_id.strip() or study_id != study_id.strip():
        raise ResearchValidationError("request.study_id must be a non-empty identifier")
    revision = request["study_revision"]
    if type(revision) is not int or revision < 1:
        raise ResearchValidationError("request.study_revision must be an integer >= 1")
    feature_set_ref = request["feature_set_ref"]
    parse_versioned_ref(feature_set_ref, "request.feature_set_ref")
    if not isinstance(request["source_refs"], list) or not request["source_refs"]:
        raise ResearchValidationError("request.source_refs must be a non-empty array")
    source_refs = []
    for ref in request["source_refs"]:
        parse_versioned_ref(ref, "request.source_refs[]")
        if ref in source_refs:
            raise ResearchValidationError("request.source_refs must be unique")
        source_refs.append(ref)
    source_refs.sort()
    split_ref = request["split_ref"]
    if (not isinstance(split_ref, dict) or set(split_ref) != {"uri", "content_hash"}
            or not isinstance(split_ref["uri"], str)
            or not isinstance(split_ref["content_hash"], str)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", split_ref["content_hash"])):
        raise ResearchValidationError("request.split_ref must be a hashed local artifact ref")
    parsed_split = urlparse(split_ref["uri"])
    if parsed_split.scheme != "file" or parsed_split.netloc not in {"", "localhost"}:
        raise ResearchValidationError("request.split_ref.uri must use a local file:// URI")
    scope = _validated_scope(request["scope"])
    allowed_steps = request["allowed_steps"]
    if (not isinstance(allowed_steps, list) or any(type(step) is not int for step in allowed_steps)
            or any(step not in {3, 4, 5} for step in allowed_steps)
            or len(set(allowed_steps)) != len(allowed_steps)):
        raise ResearchValidationError("request.allowed_steps must contain unique steps from 3, 4, 5")
    allowed_steps = sorted(allowed_steps)
    if (not isinstance(request["missing_dependencies"], str)
            or request["missing_dependencies"] not in {"block", "compute_within_allowed_steps"}):
        raise ResearchValidationError(
            "request.missing_dependencies must be block or compute_within_allowed_steps"
        )
    if request["reuse_policy"] != "verified_only":
        raise ResearchValidationError("request.reuse_policy must be verified_only")
    budget = request["budget"]
    if (not isinstance(budget, dict) or not budget
            or any(not isinstance(key, str) for key in budget)
            or set(budget) - _BUDGET_KEYS):
        raise ResearchValidationError("request.budget must contain supported positive resource limits")
    if any(type(limit) is not int or limit < 1 for limit in budget.values()):
        raise ResearchValidationError("request.budget values must be positive integers")
    selected = request.get("selected_item_ids")
    if "selected_item_ids" in request:
        if not isinstance(selected, list) or any(not isinstance(item, str) or not item.strip()
                                                for item in selected):
            raise ResearchValidationError("request.selected_item_ids must be an array of item ids")
        if len(set(selected)) != len(selected):
            raise ResearchValidationError("request.selected_item_ids must be unique")
        selected = sorted(selected)
    actor = _actor(request["created_by"], "request.created_by")
    normalized = {
        "schema_version": _REQUEST_SCHEMA,
        "study_id": study_id,
        "study_revision": revision,
        "feature_set_ref": feature_set_ref,
        "source_refs": source_refs,
        "split_ref": {"uri": split_ref["uri"], "content_hash": split_ref["content_hash"]},
        "scope": scope,
        "allowed_steps": allowed_steps,
        "missing_dependencies": request["missing_dependencies"],
        "reuse_policy": "verified_only",
        "budget": dict(sorted(budget.items())),
        "created_by": actor,
    }
    if selected is not None:
        normalized["selected_item_ids"] = selected
    if "parent_run_id" in request:
        parent = request["parent_run_id"]
        if not isinstance(parent, str) or not parent.strip():
            raise ResearchValidationError("request.parent_run_id must be a non-empty string")
        normalized["parent_run_id"] = parent
    if "adopted_review_refs" in request:
        refs = request["adopted_review_refs"]
        if not isinstance(refs, list) or any(not isinstance(ref, str) or not ref.strip() for ref in refs):
            raise ResearchValidationError("request.adopted_review_refs must be an array of refs")
        if len(set(refs)) != len(refs):
            raise ResearchValidationError("request.adopted_review_refs must be unique")
        normalized["adopted_review_refs"] = sorted(refs)
    if "seed" in request:
        if type(request["seed"]) is not int or request["seed"] < 0:
            raise ResearchValidationError("request.seed must be a non-negative integer")
        normalized["seed"] = request["seed"]
    return normalized


def _range_contains(allowed: dict[str, Any], instrument: str, start: datetime,
                    end: datetime, sessions: set[str] | None) -> bool:
    if instrument not in allowed["instruments"]:
        return False
    if start < _utc_timestamp(allowed["start"], "allowed_scope.start"):
        return False
    if end > _utc_timestamp(allowed["end"], "allowed_scope.end"):
        return False
    allowed_sessions = allowed.get("session_filter")
    return allowed_sessions is None or (sessions is not None and sessions.issubset(allowed_sessions))


def _contained_by_ranges(ranges: list[dict[str, Any]], instrument: str,
                         start: datetime, end: datetime, session: str | None = None) -> bool:
    sessions = None if session is None else {session}
    return any(_range_contains(item, instrument, start, end, sessions) for item in ranges)


def _scope_authorized(ranges: list[dict[str, Any]], instrument: str, start: datetime,
                      end: datetime, sessions: set[str] | None) -> bool:
    if sessions is None:
        return _contained_by_ranges(ranges, instrument, start, end)
    return all(_contained_by_ranges(ranges, instrument, start, end, session)
               for session in sessions)


def _read_split(split_ref: dict[str, str]) -> dict[str, Any]:
    document = _read_json_ref(split_ref, "request.split_ref")
    if document.get("schema_version") != "split_manifest_v1":
        raise ResearchValidationError("split_ref must contain split_manifest_v1")
    if not isinstance(document.get("split_id"), str) or not document["split_id"].strip():
        raise ResearchValidationError("split manifest must declare split_id")
    role = document.get("role")
    if role is not None and (not isinstance(role, str) or role not in _SPLIT_ROLES):
        raise ResearchValidationError("split manifest role is not recognized")
    if role in _UNSAFE_SPLIT_ROLES:
        raise SourceScopeMissingError(
            f"split role {role!r} is not available for exploratory Run planning",
            details={"role": role},
        )
    read_ranges = document.get("read_ranges")
    if read_ranges is None and "scope" in document:
        raw_scope = document["scope"]
        if isinstance(raw_scope, dict) and "instruments" in raw_scope:
            normalized = _scope(raw_scope, "split.scope")
            read_ranges = [{"instruments": normalized["instruments"],
                            "start": normalized["start"], "end": normalized["end"],
                            **({"session_filter": normalized["session_filter"]}
                               if "session_filter" in normalized else {})}]
    if not isinstance(read_ranges, list) or not read_ranges:
        raise SourceScopeMissingError(
            "split manifest must declare read_ranges; role alone does not authorize dates"
        )
    normalized_ranges = []
    for index, raw in enumerate(read_ranges):
        path = f"split.read_ranges[{index}]"
        if not isinstance(raw, dict):
            raise ResearchValidationError(f"{path} must be an object")
        if "instrument_id" in raw:
            required = {"instrument_id", "start", "end"}
            allowed = required | {"session_filter"}
            if set(raw) - allowed or required - raw.keys():
                raise ResearchValidationError(f"{path} has invalid fields")
            range_doc = {"instruments": [raw["instrument_id"]], "start": raw["start"],
                         "end": raw["end"]}
            if "session_filter" in raw:
                range_doc["session_filter"] = raw["session_filter"]
        else:
            range_doc = raw
        normalized = _validated_scope(range_doc)
        normalized_ranges.append(normalized)
    return {**document, "read_ranges": normalized_ranges}


def _source_time_metadata(source: dict[str, Any]) -> dict[str, Any]:
    calendar = _read_json_ref(source["calendar_ref"], "source.calendar_ref")
    session_policy = _read_json_ref(source["session_policy_ref"], "source.session_policy_ref")
    return {
        "calendar": calendar,
        "sessions": session_policy,
        "timezone": source["time_rules"]["timezone"],
        "partitions": {
            (item.get("exchange"), item["instrument_id"], item["trade_date"]): item
            for item in source["coverage"]["partitions"]
        },
        "partitions_by_instrument_date": _partition_index(source),
    }


def _partition_index(source: dict[str, Any]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    index: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in source["coverage"]["partitions"]:
        index.setdefault((item["instrument_id"], item["trade_date"]), []).append(item)
    return index


def _scope_segments(scope: dict[str, Any], sources: list[dict[str, Any]],
                    metadata_by_ref: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    start = _utc_timestamp(scope["start"], "request.scope.start")
    end = _utc_timestamp(scope["end"], "request.scope.end")
    selected_sessions = set(scope.get("session_filter", [])) or None
    intervals: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    missing_interval_partitions: list[dict[str, Any]] = []
    for source in sources:
        ref = f"{source['source_id']}@{source['version']}"
        metadata_doc = metadata_by_ref[ref]
        raw_intervals = metadata_doc["sessions"].get("intervals")
        if not isinstance(raw_intervals, list):
            # No exact session grid means there is no safe way to turn a UTC range
            # into exchange trade-date partitions.
            for partition in source["coverage"]["partitions"]:
                if partition["instrument_id"] in scope["instruments"]:
                    missing_interval_partitions.append({
                        "source_ref": ref,
                        "instrument_id": partition["instrument_id"],
                        "trade_date": partition["trade_date"],
                        "session_ids": list(partition["session_ids"]),
                        "reason": "SESSION_INTERVALS_MISSING",
                    })
            continue
        for raw in raw_intervals:
            if raw["instrument_id"] not in scope["instruments"]:
                continue
            if selected_sessions is not None and raw["session_id"] not in selected_sessions:
                continue
            session_start = _utc_timestamp(raw["start"], "session_policy.interval.start")
            session_end = _utc_timestamp(raw["end"], "session_policy.interval.end")
            out_start, out_end = max(start, session_start), min(end, session_end)
            if out_start >= out_end:
                continue
            key = (raw["instrument_id"], raw["trade_date"], raw["session_id"])
            intervals.setdefault(key, []).append({
                "source_ref": ref,
                "exchange": raw.get("exchange"),
                "session_start": session_start,
                "session_end": session_end,
                "start": out_start,
                "end": out_end,
            })

    if not intervals and not missing_interval_partitions:
        diagnostics = [{
            "instrument_id": instrument,
            "start": _time_text(start),
            "end": _time_text(end),
            "reason": "NO_SCHEDULED_SESSION",
        } for instrument in scope["instruments"]]
    else:
        diagnostics = list(missing_interval_partitions)

    segments: list[dict[str, Any]] = []
    source_gaps: list[dict[str, Any]] = []
    for (instrument, trade_date, session_id), candidates in sorted(intervals.items()):
        declared = []
        for candidate in candidates:
            source = next(item for item in sources
                          if f"{item['source_id']}@{item['version']}" == candidate["source_ref"])
            rows = metadata_by_ref[candidate["source_ref"]]["partitions_by_instrument_date"].get(
                (instrument, trade_date), [])
            matching_rows = [row for row in rows if session_id in row["session_ids"]]
            for row in matching_rows:
                declared.append((candidate, row))
        if len(declared) > 1:
            diagnostics.append({
                "source_refs": sorted({entry[0]["source_ref"] for entry in declared}),
                "instrument_id": instrument, "trade_date": trade_date,
                "session_id": session_id, "reason": "SOURCE_PARTITION_AMBIGUOUS",
            })
            continue
        if not declared:
            # A scheduled session with no source coverage row is an evidenced source
            # gap. It is surfaced as NA provenance and never as a missing artifact.
            candidates_by_source = {candidate["source_ref"]: candidate for candidate in candidates}
            for candidate in candidates_by_source.values():
                source = next(item for item in sources
                              if f"{item['source_id']}@{item['version']}" == candidate["source_ref"])
                if instrument not in source["instruments"]:
                    continue
                known_exchanges = {
                    item.get("exchange")
                    for item in source["coverage"]["partitions"]
                    if item["instrument_id"] == instrument and item.get("exchange") is not None
                }
                exchange = candidate.get("exchange")
                if exchange is None and len(known_exchanges) == 1:
                    exchange = next(iter(known_exchanges))
                blocked_reason = None if exchange is not None else "EXCHANGE_METADATA_MISSING"
                source_gaps.append({
                    "source_ref": candidate["source_ref"],
                    "instrument_id": instrument,
                    "trade_date": trade_date,
                    "session_id": session_id,
                    "exchange": exchange,
                    "reason": "SOURCE_PARTITION_MISSING",
                    "na_reason": "SOURCE_PARTITION_MISSING",
                })
                segments.append({
                    "source_ref": candidate["source_ref"],
                    "exchange": exchange,
                    "instrument_id": instrument,
                    "trade_date": trade_date,
                    "session_id": session_id,
                    "session_start": candidate["session_start"],
                    "session_end": candidate["session_end"],
                    "start": candidate["start"],
                    "end": candidate["end"],
                    "blocked_reason": blocked_reason,
                    "source_gap_reason": "SOURCE_PARTITION_MISSING",
                })
            continue
        candidate, row = declared[0]
        exchange = row.get("exchange")
        interval_exchange = candidate.get("exchange")
        blocked_reason = None
        if exchange is None:
            blocked_reason = "EXCHANGE_METADATA_MISSING"
        elif interval_exchange is not None and exchange != interval_exchange:
            blocked_reason = "EXCHANGE_METADATA_CONFLICT"
        segments.append({
            "source_ref": candidate["source_ref"],
            "exchange": exchange,
            "instrument_id": instrument,
            "trade_date": trade_date,
            "session_id": session_id,
            "session_start": candidate["session_start"],
            "session_end": candidate["session_end"],
            "start": candidate["start"],
            "end": candidate["end"],
            "blocked_reason": blocked_reason,
            "source_gap_reason": None,
        })
    source_gaps.sort(key=lambda item: (item["source_ref"], item["instrument_id"],
                                       item["trade_date"], item["session_id"]))
    diagnostics.sort(key=canonical_json)
    return segments, source_gaps, diagnostics


def _feature_extension(feature: dict[str, Any]) -> timedelta:
    policy = feature.get("time_policy", {})
    values = []
    for key in ("lookback", "profile_window"):
        if key in policy:
            values.append(_duration(policy[key], f"Feature {key}"))
    base = max(values, default=timedelta(0))
    for key in ("max_endpoint_age", "max_quote_age", "max_feature_age"):
        if key in policy:
            base += _duration(policy[key], f"Feature {key}")
    sampling = feature.get("sampling", {})
    for key in ("max_quote_age", "max_feature_age"):
        if key in sampling:
            base += _duration(sampling[key], f"Feature sampling.{key}")
    return base


def _needs_tick_size(feature: dict[str, Any]) -> bool:
    requirements = feature.get("source_requirements", {})
    if not isinstance(requirements, dict):
        return False
    return (requirements.get("tick_size") is True
            or "tick_size" in requirements.get("required_metadata", [])
            or "tick_size" in requirements.get("metadata", []))


def _tick_metadata_block(feature: dict[str, Any], source: dict[str, Any],
                         instrument: str, trade_date: str) -> str | None:
    if not _needs_tick_size(feature):
        return None
    metadata_ref = source.get("market_metadata_ref")
    if metadata_ref is None:
        return "MARKET_METADATA_MISSING"
    metadata_doc = _read_json_ref(metadata_ref, "source.market_metadata_ref")
    product = next((candidate for candidate in ("TL", "TF", "TS", "T")
                    if instrument.startswith(candidate)), None)
    if product is None:
        return "TICK_SIZE_RULE_MISSING"
    day = date.fromisoformat(trade_date)
    for rule in metadata_doc.get("tick_rules", []):
        if rule.get("product") != product:
            continue
        valid_from = date.fromisoformat(rule["valid_from"])
        valid_to = None if rule["valid_to"] is None else date.fromisoformat(rule["valid_to"])
        if valid_from <= day and (valid_to is None or day < valid_to):
            return None
    return "TICK_SIZE_RULE_MISSING"


class RunPlanner:
    """Plan frozen FeatureSet work and register it without dispatching workers."""

    def __init__(self, registry: Registry, repository: ResearchRepository,
                 catalog: FeatureCatalog):
        self._registry = registry
        self._repository = repository
        self._catalog = catalog

    def plan_run(self, request: dict[str, Any]) -> dict[str, Any]:
        normalized = _validate_request_shape(request)
        # Resolving the immutable FeatureSet makes omitted selection semantically
        # identical to an explicit selection of every item.
        feature_set = self._catalog.get_feature_set(normalized["feature_set_ref"])
        if feature_set.get("status") != "frozen":
            raise ResearchValidationError("request.feature_set_ref must reference a frozen FeatureSet")
        item_ids = [item["item_id"] for item in feature_set["resolved_items"]]
        if "selected_item_ids" not in normalized:
            normalized["selected_item_ids"] = sorted(item_ids)
        unknown = set(normalized["selected_item_ids"]) - set(item_ids)
        if unknown:
            raise UnknownReferenceError(
                "selected item ids are not in the frozen FeatureSet",
                details={"unknown_item_ids": sorted(unknown)},
            )
        return self._build_plan(normalized, feature_set)

    def submit_run(self, request: dict[str, Any], request_key: str,
                   expected_plan_hash: str | None = None) -> dict[str, Any]:
        if not isinstance(request_key, str) or not request_key.strip() or request_key != request_key.strip():
            raise ResearchValidationError("request_key must be a non-empty string without surrounding whitespace")
        normalized = _validate_request_shape(request)
        feature_set = self._catalog.get_feature_set(normalized["feature_set_ref"])
        if feature_set.get("status") != "frozen":
            raise ResearchValidationError("request.feature_set_ref must reference a frozen FeatureSet")
        item_ids = [item["item_id"] for item in feature_set["resolved_items"]]
        if "selected_item_ids" not in normalized:
            normalized["selected_item_ids"] = sorted(item_ids)
        unknown = set(normalized["selected_item_ids"]) - set(item_ids)
        if unknown:
            raise UnknownReferenceError(
                "selected item ids are not in the frozen FeatureSet",
                details={"unknown_item_ids": sorted(unknown)},
            )
        request_hash = _hash(normalized)

        # Idempotent retries return before current Study state or health is inspected.
        existing = self._registry.find_research_run(normalized["study_id"], request_key)
        if existing is not None:
            if existing.get("request_hash") != request_hash:
                raise IdempotencyConflictError(
                    "request_key was already used for different Run content",
                    details={"study_id": normalized["study_id"], "request_key": request_key,
                             "existing_request_hash": existing.get("request_hash"),
                             "request_hash": request_hash},
                )
            return self.get_run(existing["run_id"])

        plan = self._build_plan(normalized, feature_set)
        if expected_plan_hash is not None and expected_plan_hash != plan["plan_hash"]:
            raise PlanChangedError(
                "the submitted plan no longer matches expected_plan_hash",
                details={"expected_plan_hash": expected_plan_hash,
                         "current_plan_hash": plan["plan_hash"]},
            )
        runtime = plan["runtime_environment"]
        code_revision = plan["code_revision"]
        run_id = "run_" + uuid.uuid4().hex
        work_items = []
        for work in plan["work"]:
            stage_name = f"research_step_{work['step']}"
            implementation_version = work.get("implementation_version") or str(work["step"])
            work_items.append({
                **work,
                "stage_run_id": "stage_" + uuid.uuid4().hex,
                "stage_name": stage_name,
                "stage_version": implementation_version,
            })
        registration = self._registry.register_research_run(
            run_id=run_id, study_id=normalized["study_id"],
            study_revision=normalized["study_revision"], request_key=request_key,
            request_hash=request_hash, plan_hash=plan["plan_hash"], request=normalized,
            config_snapshot=plan["config_snapshot"], config_hash=plan["config_hash"],
            plan=plan, summary=plan["summary"], created_by=normalized["created_by"],
            split_id=plan["split_id"], code_revision=code_revision,
            environment=runtime, work_items=work_items,
        )
        outcome = registration["outcome"]
        if outcome == "idempotency_conflict":
            raise IdempotencyConflictError(
                "request_key was concurrently registered with different Run content",
                details={"study_id": normalized["study_id"], "request_key": request_key},
            )
        if outcome == "revision_conflict":
            raise RevisionConflictError(
                "Study revision changed before Run registration",
                details={"study_id": normalized["study_id"],
                         "expected_revision": normalized["study_revision"],
                         "current_revision": registration["current_revision"]},
            )
        if outcome == "study_not_active":
            raise StudyNotActiveError(
                f"Study {normalized['study_id']} is {registration['status']} and cannot accept a Run",
                details={"study_id": normalized["study_id"], "status": registration["status"]},
            )
        if outcome == "unknown_study":
            raise UnknownReferenceError(f"unknown Study: {normalized['study_id']}")
        return self.get_run(registration["run_id"])

    def get_run(self, run_id: str) -> dict[str, Any]:
        stored = self._registry.get_run(run_id)
        if stored is None or stored.get("research_study_id") is None:
            raise UnknownReferenceError(f"unknown research Run: {run_id}", details={"run_id": run_id})
        plan = stored.get("research_plan") or {}
        summary = stored.get("research_summary") or {}
        executions = []
        stages = self._registry.get_run_stages(run_id)
        by_work_key = {stage.get("work_key"): stage for stage in stages if stage.get("work_key")}
        for work in plan.get("work", []):
            stage = by_work_key.get(work["work_key"], {})
            executions.append({
                **work,
                "stage_run_id": stage.get("stage_run_id"),
                "state": stage.get("status", work.get("state")),
                "status": stage.get("status", work.get("state")),
                "attempt": stage.get("attempt", 0),
                "blocked_reason": stage.get("blocked_reason") or work.get("blocked_reason"),
            })
        request = stored.get("research_request", stored.get("config", {}))
        config_snapshot = stored.get("config", {})
        view = {
            "schema_version": "run_v1",
            "run_id": stored["run_id"],
            "study_id": stored["research_study_id"],
            "study_revision": request.get("study_revision"),
            "feature_set_ref": plan.get("feature_set_ref"),
            "feature_set_hash": plan.get("feature_set_hash"),
            "selected_item_ids": plan.get("selected_item_ids", []),
            "source_refs": plan.get("source_refs", []),
            "split_ref": plan.get("split_ref"),
            "scope": plan.get("scope"),
            "allowed_steps": plan.get("allowed_steps", []),
            "budget": plan.get("budget", {}),
            "requested_budget": plan.get("requested_budget", {}),
            "config_snapshot": config_snapshot,
            "config_hash": stored.get("config_hash"),
            "request_key": stored.get("request_key"),
            "request": request,
            "request_hash": stored.get("request_hash"),
            "plan": plan,
            "plan_hash": stored.get("plan_hash"),
            "summary": summary,
            "status": stored["status"],
            "created_by": stored.get("created_by"),
            "parent_run_id": request.get("parent_run_id"),
            "adopted_review_refs": request.get("adopted_review_refs", []),
            "seed": request.get("seed"),
            "code_revision": stored["code_revision"],
            "environment_ref": _hash(stored.get("environment", {})),
            "environment": stored.get("environment", {}),
            "item_executions": executions,
            "created_at": stored["created_at"],
            "updated_at": stored["updated_at"],
        }
        from .execution_view import enrich_run_view

        return enrich_run_view(self._registry, self._catalog, view)

    def _build_plan(self, request: dict[str, Any], feature_set: dict[str, Any]) -> dict[str, Any]:
        study = self._repository.get_study(request["study_id"], request["study_revision"])
        if study is None:
            raise UnknownReferenceError(
                f"unknown Study revision: {request['study_id']}@{request['study_revision']}"
            )
        if request["feature_set_ref"] not in study["feature_set_refs"]:
            raise ResearchValidationError("FeatureSet is not registered in the requested Study revision")
        if not set(request["source_refs"]).issubset(study["source_refs"]):
            raise ResearchValidationError("Run source_refs must be a subset of Study source_refs")
        sources = [self._source(ref) for ref in request["source_refs"]]
        split = _read_split(request["split_ref"])
        for source in sources:
            access = source_access_policy(source)
            if "research" not in access["allowed_uses"]:
                raise ResearchValidationError(f"DataSource {source['source_id']} does not permit research")
            if request["split_ref"] not in access["split_refs"]:
                raise SourceScopeMissingError(
                    "split_ref is not authorized by every bound DataSource",
                    details={"source_ref": f"{source['source_id']}@{source['version']}",
                             "split_ref": request["split_ref"]},
                )

        requested_start = _utc_timestamp(request["scope"]["start"], "request.scope.start")
        requested_end = _utc_timestamp(request["scope"]["end"], "request.scope.end")
        selected_sessions = set(request["scope"].get("session_filter", [])) or None
        study_ranges = study["limits"]["allowed_scope"]
        split_ranges = split["read_ranges"]
        for instrument in request["scope"]["instruments"]:
            if not _scope_authorized(study_ranges, instrument, requested_start, requested_end,
                                     selected_sessions):
                raise SourceScopeMissingError(
                    "requested output scope exceeds Study allowed_scope",
                    details={"instrument_id": instrument},
                )
            if not _scope_authorized(split_ranges, instrument, requested_start, requested_end,
                                     selected_sessions):
                raise SourceScopeMissingError(
                    "requested output scope exceeds split read_ranges",
                    details={"instrument_id": instrument, "split_id": split["split_id"]},
                )

        effective_steps = sorted(set(request["allowed_steps"]) & set(study["limits"]["allowed_steps"]))
        effective_budget = self._effective_budget(request["budget"], study["limits"]["budget"])
        source_metadata = {
            f"{source['source_id']}@{source['version']}": _source_time_metadata(source)
            for source in sources
        }
        segments, source_gaps, diagnostics = _scope_segments(request["scope"], sources, source_metadata)

        all_items = {item["item_id"]: item for item in feature_set["resolved_items"]}
        item_ids_by_feature: dict[str, set[str]] = {}
        for item_id, item in all_items.items():
            for output in item["outputs"]:
                item_ids_by_feature.setdefault(output["feature_ref"], set()).add(item_id)
        selected_items = set(request["selected_item_ids"])
        outputs_by_item = {
            item_id: [output["feature_ref"] for output in item["outputs"]]
            for item_id, item in all_items.items() if item_id in selected_items
        }
        selected_feature_refs = {ref for refs in outputs_by_item.values() for ref in refs}
        source_by_ref = {f"{source['source_id']}@{source['version']}": source for source in sources}
        nodes: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
        edges: set[tuple[tuple[str, str, str, str, str], tuple[str, str, str, str, str]]] = set()
        processing: set[tuple[str, str, str, str, str]] = set()

        def node_key(feature_ref: str, segment: dict[str, Any]) -> tuple[str, str, str, str, str]:
            return (feature_ref, segment["source_ref"], segment["instrument_id"],
                    segment["trade_date"], segment["session_id"])

        def ensure_node(feature_ref: str, segment: dict[str, Any], out_start: datetime,
                        out_end: datetime, *, requested_item: str | None = None,
                        associated_items: set[str] | None = None,
                        cache_only: bool = False) -> tuple[str, str, str, str, str]:
            key = node_key(feature_ref, segment)
            feature = self._catalog.get_feature(feature_ref)
            node = nodes.get(key)
            if node is None:
                node = {
                    "key": key, "feature": feature, "segment": segment,
                    "output_start": out_start, "output_end": out_end,
                    "item_ids": set(), "role": "dependency_only",
                    "blocked_reason": segment.get("blocked_reason"),
                    "cache_only": cache_only,
                    "children": set(), "processed_interval": None,
                }
                nodes[key] = node
            else:
                node["output_start"] = min(node["output_start"], out_start)
                node["output_end"] = max(node["output_end"], out_end)
            if requested_item is not None:
                node["role"] = "requested"
                node["item_ids"].add(requested_item)
                node["cache_only"] = False
            if associated_items:
                node["item_ids"].update(associated_items)
            if key in processing:
                return key
            interval_key = (node["output_start"], node["output_end"], tuple(sorted(node["item_ids"])))
            if node["processed_interval"] == interval_key:
                return key
            processing.add(key)
            try:
                step = feature["step"]
                if node["blocked_reason"] is None:
                    if feature.get("status") == "pending_semantics":
                        node["blocked_reason"] = "PENDING_SEMANTICS"
                    elif feature.get("status") != "ready":
                        node["blocked_reason"] = "FEATURE_NOT_READY"
                    elif step not in effective_steps:
                        node["blocked_reason"] = "STEP_NOT_ALLOWED"
                    elif node["cache_only"]:
                        node["blocked_reason"] = "DEPENDENCY_NOT_SELECTED"
                    else:
                        source = source_by_ref[segment["source_ref"]]
                        node["blocked_reason"] = _tick_metadata_block(
                            feature, source, segment["instrument_id"], segment["trade_date"]
                        )
                node["processed_interval"] = interval_key
                if node["blocked_reason"] not in {
                    None, "STEP_NOT_ALLOWED", "DEPENDENCY_NOT_SELECTED",
                }:
                    if node["blocked_reason"] == "PENDING_SEMANTICS":
                        extension = _feature_extension(feature)
                        dependency_start = max(
                            node["output_start"] - extension, segment["session_start"]
                        )
                        for dependency_ref in feature.get("input_feature_refs", []):
                            dependency = self._catalog.get_feature(dependency_ref)
                            if (dependency.get("status") != "pending_semantics"
                                    or (dependency_ref not in selected_feature_refs
                                        and request["missing_dependencies"] == "block")):
                                continue
                            child_key = ensure_node(
                                dependency_ref, segment, dependency_start, node["output_end"],
                                associated_items=item_ids_by_feature.get(dependency_ref, set()),
                            )
                            node["children"].add(child_key)
                            edges.add((child_key, key))
                    return key

                extension = _feature_extension(feature)
                raw_history_start = node["output_start"] - extension
                read_start = max(raw_history_start, segment["session_start"])
                node["raw_history_start"] = raw_history_start
                node["read_start"] = read_start
                node["history_shortfall"] = (
                    {"requested_start": _time_text(raw_history_start),
                     "available_start": _time_text(segment["session_start"]),
                     "reason": "SESSION_HISTORY_INSUFFICIENT"}
                    if raw_history_start < segment["session_start"] else None
                )
                # No session boundary can be crossed to satisfy the feature history.
                node["required_history"] = {
                    "start": _time_text(raw_history_start),
                    "end": _time_text(node["output_end"]),
                    "session_id": segment["session_id"],
                }
                node["read_scope"] = {
                    "start": _time_text(read_start), "end": _time_text(node["output_end"]),
                    "session_id": segment["session_id"],
                }
                if not _contained_by_ranges(study_ranges, segment["instrument_id"],
                                            read_start, node["output_end"], segment["session_id"]):
                    node["blocked_reason"] = "HISTORY_OUTSIDE_STUDY_SCOPE"
                    return key
                if not _contained_by_ranges(split_ranges, segment["instrument_id"],
                                            read_start, node["output_end"], segment["session_id"]):
                    node["blocked_reason"] = "HISTORY_OUTSIDE_SPLIT_SCOPE"
                    return key

                # These tasks may only bind a verified prior result. They never
                # grant permission to dispatch additional dependency computation.
                if node["blocked_reason"] in {"STEP_NOT_ALLOWED", "DEPENDENCY_NOT_SELECTED"}:
                    node["cache_only"] = True
                    return key

                input_start = read_start
                input_refs = feature.get("input_feature_refs", [])
                if not isinstance(input_refs, list):
                    node["blocked_reason"] = "INVALID_FEATURE_DEPENDENCIES"
                    return key
                for dependency_ref in input_refs:
                    parse_feature_ref(dependency_ref, "Feature.input_feature_refs[]")
                    dependency_cache_only = (
                        dependency_ref not in selected_feature_refs
                        and request["missing_dependencies"] == "block"
                    )
                    child_key = ensure_node(dependency_ref, segment, input_start,
                                            node["output_end"],
                                            associated_items=item_ids_by_feature.get(
                                                dependency_ref, set()),
                                            cache_only=dependency_cache_only)
                    node["children"].add(child_key)
                    edges.add((child_key, key))
            finally:
                processing.discard(key)
            return key

        # Empty selection deliberately produces no task graph or implicit validation work.
        if selected_items:
            for segment in segments:
                for item_id, refs in outputs_by_item.items():
                    for feature_ref in refs:
                        ensure_node(feature_ref, segment, segment["start"], segment["end"],
                                    requested_item=item_id)

        # Propagate blocking only along explicit dependency edges.
        changed = True
        while changed:
            changed = False
            for child_key, parent_key in sorted(edges):
                child, parent = nodes[child_key], nodes[parent_key]
                if child["blocked_reason"] is not None and parent["blocked_reason"] is None:
                    parent["blocked_reason"] = f"DEPENDENCY_BLOCKED:{child['feature']['feature_id']}"
                    changed = True

        def transitive_extension(feature: dict[str, Any], visiting: frozenset[str]) -> timedelta:
            ref = f"{feature['feature_id']}@{feature['version']}"
            if ref in visiting:
                raise ResearchValidationError("cyclic frozen Feature dependencies")
            children = [self._catalog.get_feature(child)
                        for child in feature.get("input_feature_refs", [])]
            return _feature_extension(feature) + max(
                (transitive_extension(child, visiting | {ref}) for child in children),
                default=timedelta(0),
            )

        for key, node in nodes.items():
            if (node["blocked_reason"] is None or node["cache_only"]
                    or str(node["blocked_reason"]).startswith("DEPENDENCY_BLOCKED:")):
                raw_start = node["output_start"] - transitive_extension(
                    node["feature"], frozenset(),
                )
                segment = node["segment"]
                available_start = max(raw_start, segment["session_start"])
                node["raw_history_start"] = raw_start
                node["history_shortfall"] = (
                    {"requested_start": _time_text(raw_start),
                     "available_start": _time_text(segment["session_start"]),
                     "reason": "SESSION_HISTORY_INSUFFICIENT"}
                    if raw_start < segment["session_start"] else None
                )
                node["required_history"] = {
                    "start": _time_text(raw_start), "end": _time_text(node["output_end"]),
                    "session_id": segment["session_id"],
                }
                node["raw_dependency_scope"] = {
                    "start": _time_text(available_start),
                    "end": _time_text(node["output_end"]),
                    "session_id": segment["session_id"],
                }
                if node["cache_only"]:
                    if not _contained_by_ranges(
                        study_ranges, segment["instrument_id"], available_start,
                        node["output_end"], segment["session_id"],
                    ):
                        node["blocked_reason"] = "HISTORY_OUTSIDE_STUDY_SCOPE"
                    elif not _contained_by_ranges(
                        split_ranges, segment["instrument_id"], available_start,
                        node["output_end"], segment["session_id"],
                    ):
                        node["blocked_reason"] = "HISTORY_OUTSIDE_SPLIT_SCOPE"

        code_revision, environment_ref, environment = _runtime_snapshot()
        source_hashes = {ref: source_by_ref[ref]["content_hash"] for ref in sorted(source_by_ref)}
        request_hash = _hash(request)
        study_semantics = {key: value for key, value in study.items()
                           if key not in {"created_at", "updated_at", "created_by", "updated_by",
                                          "change_reason"}}
        config_snapshot = {
            "schema_version": "run_config_v1",
            "study_id": request["study_id"],
            "study_revision": request["study_revision"],
            "study_snapshot_hash": _hash(study_semantics),
            "feature_set_ref": request["feature_set_ref"],
            "feature_set_hash": feature_set["config_hash"],
            "selected_item_ids": request["selected_item_ids"],
            "source_refs": request["source_refs"],
            "source_hashes": source_hashes,
            "split_ref": request["split_ref"],
            "split_id": split["split_id"],
            "scope": request["scope"],
            "allowed_steps": effective_steps,
            "requested_allowed_steps": request["allowed_steps"],
            "missing_dependencies": request["missing_dependencies"],
            "reuse_policy": request["reuse_policy"],
            "budget": effective_budget,
            "requested_budget": request["budget"],
            "created_by": request["created_by"],
            "code_revision": code_revision,
            "environment_ref": environment_ref,
        }
        for optional in ("parent_run_id", "adopted_review_refs", "seed"):
            if optional in request:
                config_snapshot[optional] = request[optional]
        config_hash = _hash(config_snapshot)
        plan_work = []
        key_by_node = {}
        for key, node in nodes.items():
            segment = node["segment"]
            feature = node["feature"]
            extension = _feature_extension(feature)
            raw_history_start = node.get("raw_history_start", node["output_start"] - extension)
            read_start = node.get("read_start", max(raw_history_start, segment["session_start"]))
            required_history = node.get("required_history", {
                "start": _time_text(raw_history_start), "end": _time_text(node["output_end"]),
                "session_id": segment["session_id"],
            })
            read_scope = node.get("read_scope", {
                "start": _time_text(read_start), "end": _time_text(node["output_end"]),
                "session_id": segment["session_id"],
            })
            raw_dependency_scope = node.get("raw_dependency_scope", read_scope)
            partition_scope = {
                "exchange": segment["exchange"],
                "instrument_id": segment["instrument_id"],
                "trade_date": segment["trade_date"],
                "session_id": segment["session_id"],
                "start": _time_text(node["output_start"]),
                "end": _time_text(node["output_end"]),
            }
            identity = {
                "feature_ref": key[0], "definition_hash": feature["definition_hash"],
                "source_ref": key[1], "partition_scope": partition_scope,
                "required_history": required_history,
            }
            work_key = "work_" + hashlib.sha256(canonical_json(identity).encode()).hexdigest()[:24]
            key_by_node[key] = work_key
            state = StageStatus.BLOCKED.value if node["blocked_reason"] else StageStatus.PENDING.value
            implementation = feature.get("implementation", {})
            plan_work.append({
                "work_key": work_key,
                "item_ids": sorted(node["item_ids"]),
                "feature_ref": key[0],
                "definition_hash": feature["definition_hash"],
                "step": feature["step"],
                "implementation_version": implementation.get("implementation_version"),
                "source_ref": key[1],
                "partition_scope": partition_scope,
                "required_history": required_history,
                "read_scope": read_scope,
                "raw_dependency_scope": raw_dependency_scope,
                "history_shortfall": node.get("history_shortfall"),
                "source_gap_reason": segment.get("source_gap_reason"),
                "role": node["role"],
                "dependency_work_keys": [],
                "cache_candidate": False,
                "cache_only": node["cache_only"],
                "input_result_refs": [],
                "blocked_reason": node["blocked_reason"],
                "state": state,
            })
        for item in plan_work:
            node = next(node for node in nodes.values()
                        if key_by_node[node["key"]] == item["work_key"])
            item["dependency_work_keys"] = sorted(key_by_node[child] for child, parent in edges
                                                  if parent == node["key"])
        plan_work.sort(key=lambda item: item["work_key"])
        dependency_edges = sorted(
            [{"from_work_key": key_by_node[child], "to_work_key": key_by_node[parent]}
             for child, parent in edges],
            key=lambda item: (item["from_work_key"], item["to_work_key"]),
        )

        unique_partitions = {
            (item["partition_scope"]["exchange"], item["partition_scope"]["instrument_id"],
             item["partition_scope"]["trade_date"])
            for item in plan_work
        }
        limit_details = {"max_work_units": len(plan_work), "max_partitions": len(unique_partitions)}
        exceeded = {key: {"actual": value, "limit": effective_budget[key]}
                    for key, value in limit_details.items()
                    if key in effective_budget and value > effective_budget[key]}
        if exceeded:
            raise BudgetExceededError(
                "Run budget does not cover the selected dependency closure",
                details={"counts": limit_details, "effective_budget": effective_budget,
                         "exceeded": exceeded},
            )
        unsupported_limits = sorted(
            set(effective_budget) & {"max_cpu_seconds", "max_memory_bytes"}
        )
        if plan_work and unsupported_limits:
            raise ResearchValidationError(
                "the current executor cannot enforce these hard resource limits",
                details={"unsupported_limits": unsupported_limits,
                         "executor": "single_owner_synchronous_v1"},
            )

        requested_count = sum(item["role"] == "requested" for item in plan_work)
        dependency_count = len(plan_work) - requested_count
        blocked_count = sum(item["state"] == StageStatus.BLOCKED.value for item in plan_work)
        ready_count = len(plan_work) - blocked_count
        if (not plan_work and (not selected_items
                               or (diagnostics and all(
                                   item.get("reason") == "NO_SCHEDULED_SESSION"
                                   for item in diagnostics)))):
            plan_status = "NO_WORK"
        elif ready_count:
            plan_status = "READY"
        elif blocked_count or diagnostics:
            plan_status = "BLOCKED"
        else:
            plan_status = "NO_WORK"
        summary = {
            "work_units": len(plan_work), "partitions": len(unique_partitions),
            "requested": requested_count, "dependency_only": dependency_count,
            "ready": ready_count, "blocked": blocked_count,
            "source_gaps": len(source_gaps), "diagnostics": len(diagnostics),
            "requested_budget": dict(request["budget"]),
            "effective_budget": effective_budget,
        }
        plan_core = {
            "schema_version": _PLAN_SCHEMA,
            "request_hash": request_hash,
            "status": plan_status,
            "study_id": request["study_id"],
            "study_revision": request["study_revision"],
            "feature_set_ref": request["feature_set_ref"],
            "feature_set_hash": feature_set["config_hash"],
            "source_refs": request["source_refs"],
            "source_hashes": source_hashes,
            "split_ref": request["split_ref"],
            "split_id": split["split_id"],
            "scope": request["scope"],
            "selected_item_ids": request["selected_item_ids"],
            "allowed_steps": effective_steps,
            "requested_allowed_steps": request["allowed_steps"],
            "missing_dependencies": request["missing_dependencies"],
            "reuse_policy": request["reuse_policy"],
            "budget": summary["effective_budget"],
            "requested_budget": summary["requested_budget"],
            "work": plan_work,
            "dependencies": dependency_edges,
            "source_gaps": source_gaps,
            "diagnostics": diagnostics,
            "summary": summary,
            "config_snapshot": config_snapshot,
            "config_hash": config_hash,
            "code_revision": code_revision,
            "environment_ref": environment_ref,
        }
        plan_hash = _hash(plan_core)
        # Cache health is a current observation, not a change to the authorized
        # graph or request identity. Dispatch must revalidate every binding.
        self._observe_cache(plan_core, request, environment)
        return {**plan_core, "plan_hash": plan_hash, "runtime_environment": environment,
                "split_id": split["split_id"]}

    def _observe_cache(self, plan: dict[str, Any], request: dict[str, Any],
                       environment: dict[str, Any]) -> None:
        from quantsage_discovery.core.artifacts import ArtifactStore

        from .execution import ResearchExecutor

        executor = ResearchExecutor(
            self._registry, ArtifactStore(self._registry.path.parent / "artifacts"),
            self._catalog,
        )
        context = {
            "run_id": "planning_preview", "request": request,
            "plan": {**plan, "runtime_environment": environment},
            "environment": environment,
        }
        work_by_key = {work["work_key"]: work for work in plan["work"]}
        for work in plan["work"]:
            reason = work["blocked_reason"]
            if (reason is not None and reason not in {
                "STEP_NOT_ALLOWED", "DEPENDENCY_NOT_SELECTED",
            } and not reason.startswith("DEPENDENCY_BLOCKED:")):
                continue
            candidate = executor.find_verified_result(context, work, record_health=False)
            if candidate is not None:
                work["cache_candidate"] = True
                work["candidate_result_ref"] = candidate["result_id"]
                work["input_result_refs"] = list(candidate["input_result_refs"])
                work["state"] = StageStatus.PENDING.value
                work["blocked_reason"] = None
        changed = True
        while changed:
            changed = False
            for work in plan["work"]:
                reason = work["blocked_reason"]
                if reason and reason.startswith("DEPENDENCY_BLOCKED:") and all(
                    work_by_key[key]["blocked_reason"] is None
                    for key in work["dependency_work_keys"]
                ):
                    work["blocked_reason"] = None
                    work["state"] = StageStatus.PENDING.value
                    changed = True
        for work in plan["work"]:
            if not work["cache_candidate"]:
                work["input_result_refs"] = [
                    work_by_key[key]["candidate_result_ref"]
                    for key in work["dependency_work_keys"]
                    if work_by_key[key].get("candidate_result_ref") is not None
                ]
        blocked = sum(work["state"] == StageStatus.BLOCKED.value for work in plan["work"])
        plan["summary"] = {**plan["summary"], "blocked": blocked,
                           "ready": len(plan["work"]) - blocked,
                           "cache_candidates": sum(work["cache_candidate"] for work in plan["work"])}
        if plan["work"]:
            plan["status"] = "READY" if blocked < len(plan["work"]) else "BLOCKED"

    @staticmethod
    def _effective_budget(request_budget: dict[str, int], study_budget: dict[str, int]) -> dict[str, int]:
        keys = set(request_budget) | set(study_budget)
        return {key: min(request_budget[key], study_budget[key])
                if key in request_budget and key in study_budget else
                request_budget.get(key, study_budget.get(key))
                for key in sorted(keys)}

    def _source(self, ref: str) -> dict[str, Any]:
        source_id, version = parse_versioned_ref(ref, "request.source_refs[]")
        source = self._repository.get_source(source_id, version)
        if source is None:
            raise UnknownReferenceError(f"unknown DataSource ref: {ref}", details={"ref": ref})
        return source
