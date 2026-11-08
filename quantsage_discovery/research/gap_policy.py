"""Shared source-gap and true-session context for Steps 04 and 05 windows."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

GAP_POLICY_VERSION = "gap_policy_v1"
KNOWN_SOURCE_GAP = "KNOWN_SOURCE_GAP"

# These reasons describe source continuity rather than a usable/effective value.
WARNING_ONLY_REASON_CODES = frozenset({
    "VOLUME_INCREMENT_UNVERIFIED",
    "CUMULATIVE_INCREMENT_INCONSISTENT",
    "TRADEVOLUME_TURNOVER_MISMATCH",
})

# Field-level evidence can affect every derived sequence even when no primitive
# directly names the field.
DOMAIN_WIDE_AFFECTED_FIELDS = frozenset({
    "source_observation",
    "continuity",
    "business_payload",
    "event_time",
    "trade_date",
    "required_identity_or_time",
})


def _utc(value: Any, *, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value)
    else:
        raise TypeError(f"{label} must be an aware timestamp or ISO timestamp")
    if parsed.tzinfo is None:
        raise ValueError(f"{label} must be timezone-aware")
    return parsed.astimezone(UTC)


def _date_text(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _fields(value: Any) -> frozenset[str]:
    if value is None:
        return frozenset()
    if isinstance(value, str):
        return frozenset({value.casefold()})
    return frozenset(str(item).casefold() for item in value)


@dataclass(frozen=True)
class WindowGapContext:
    """Frozen inputs needed to distinguish history, gaps, staleness and warnings."""

    session_start: datetime
    authorized_start: datetime
    dependency_fields: Mapping[str, frozenset[str]]
    gap_events: tuple[Mapping[str, Any], ...]
    gap_manifest_ref: Mapping[str, str] | None
    warning_flags: Mapping[str, tuple[str, ...]]

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> WindowGapContext:
        if not isinstance(document, Mapping):
            raise TypeError("window context must be an object")
        required = {
            "session_start", "authorized_start", "dependency_fields",
            "gap_events", "gap_manifest_ref", "warning_flags",
        }
        missing = required - document.keys()
        if missing:
            raise ValueError(
                "window context is missing required fields: " + ", ".join(sorted(missing))
            )
        raw_fields = document["dependency_fields"]
        if not isinstance(raw_fields, Mapping):
            raise TypeError("window context dependency_fields must be an object")
        raw_events = document["gap_events"]
        if not isinstance(raw_events, (list, tuple)):
            raise TypeError("window context gap_events must be an array")
        raw_warnings = document["warning_flags"]
        if raw_warnings is None:
            raw_warnings = {}
        if not isinstance(raw_warnings, Mapping):
            raise TypeError("window context warning_flags must be an object")
        manifest = document["gap_manifest_ref"]
        if manifest is not None and not isinstance(manifest, Mapping):
            raise ValueError("window context gap_manifest_ref must be an object or null")
        session_start = _utc(document["session_start"], label="window context session_start")
        authorized_start = _utc(
            document["authorized_start"], label="window context authorized_start",
        )
        if authorized_start < session_start:
            raise ValueError("authorized_start cannot precede the true session start")
        return cls(
            session_start=session_start,
            authorized_start=authorized_start,
            dependency_fields={
                str(role): _fields(fields) for role, fields in raw_fields.items()
            },
            gap_events=tuple(dict(event) for event in raw_events),
            gap_manifest_ref=(
                None if manifest is None else dict(manifest)
            ),
            warning_flags={
                str(role): tuple(sorted({str(flag) for flag in flags or ()}))
                for role, flags in raw_warnings.items()
            },
        )

    def history_is_insufficient(self, window_start: datetime) -> bool:
        return window_start < max(self.session_start, self.authorized_start)

    def warning_flags_for(self, roles: Iterable[str]) -> tuple[str, ...]:
        flags: set[str] = set()
        for role in roles:
            flags.update(self.warning_flags.get(role, ()))
        return tuple(sorted(flags))

    @property
    def gap_manifest_uri(self) -> str | None:
        return None if self.gap_manifest_ref is None else self.gap_manifest_ref.get("uri")

    @property
    def gap_manifest_hash(self) -> str | None:
        return (
            None if self.gap_manifest_ref is None
            else self.gap_manifest_ref.get("content_hash")
        )


def _event_fields(event: Mapping[str, Any]) -> frozenset[str]:
    return _fields(event.get("affected_fields"))


def _field_affects(event: Mapping[str, Any], dependency_fields: frozenset[str]) -> bool:
    affected = _event_fields(event)
    return bool(affected & (dependency_fields | DOMAIN_WIDE_AFFECTED_FIELDS))


def _identity_matches(
    event: Mapping[str, Any],
    *,
    exchange: str | None,
    instrument_id: str,
    trade_date: str,
    session_id: str,
) -> bool:
    event_exchange = event.get("exchange")
    if event_exchange is not None and exchange is not None and event_exchange != exchange:
        return False
    event_instrument = event.get("instrument_id")
    if event_instrument is not None and event_instrument != instrument_id:
        return False
    event_date = event.get("trade_date")
    if event_date is not None and _date_text(event_date) != trade_date:
        return False
    event_session = event.get("session_id")
    # A null session can still be located by exact instrument/date/time.
    return event_session is None or event_session == session_id


def _interval_matches(
    event: Mapping[str, Any],
    *,
    start: datetime,
    end: datetime,
    include_start: bool,
) -> bool:
    boundary = str(event.get("boundary") or "unknown")
    event_start = event.get("start_time")
    event_end = event.get("end_time")
    if event_start is None and event_end is None:
        # A whole-date/source absence is conservatively relevant to every window
        # that passed the exact instrument/date/session identity checks.
        return True
    first = _utc(event_start if event_start is not None else event_end, label="gap start")
    last = _utc(event_end if event_end is not None else event_start, label="gap end")
    if last < first:
        first, last = last, first
    if boundary == "point" or first == last:
        if include_start:
            return start <= first <= end
        return start < first <= end
    if boundary in {"whole_trade_date", "first_observed_before_unknown", "unknown"}:
        return start < last and end >= first
    # Explicit interval boundary strings are honored conservatively. An event
    # that touches only the open left endpoint is not inside a rolling window.
    if boundary in {"closed_open", "[start,end)"}:
        return first <= end and last > start
    if boundary in {"open_closed", "(start,end]"}:
        return first <= end and last > start
    if boundary in {"closed", "closed_closed", "[start,end]"}:
        return first <= end and last >= start
    if boundary in {"open", "open_open", "(start,end)"}:
        return first < end and last > start
    return first <= end and last >= start


def matching_gap_events(
    context: WindowGapContext,
    *,
    roles: Iterable[str],
    exchange: str | None,
    instrument_id: str,
    trade_date: str,
    session_id: str,
    start: datetime,
    end: datetime,
    include_start: bool = False,
) -> tuple[Mapping[str, Any], ...]:
    """Return relevant non-warning gap evidence for one window."""
    dependency_fields = frozenset().union(
        *(context.dependency_fields.get(role, frozenset()) for role in roles)
    )
    if not dependency_fields:
        return ()
    matched = []
    for event in context.gap_events:
        reasons = {str(reason) for reason in event.get("reason_codes") or ()}
        if reasons and reasons.issubset(WARNING_ONLY_REASON_CODES):
            continue
        if not _field_affects(event, dependency_fields):
            continue
        if not _identity_matches(
            event,
            exchange=exchange,
            instrument_id=instrument_id,
            trade_date=trade_date,
            session_id=session_id,
        ):
            continue
        if _interval_matches(
            event, start=start, end=end, include_start=include_start,
        ):
            matched.append(event)
    return tuple(sorted(
        matched,
        key=lambda event: (
            _utc(
                event.get("start_time") or event.get("end_time") or datetime.min.replace(tzinfo=UTC),
                label="gap event time",
            ),
            str(event.get("event_id")),
        ),
    ))


def gap_event_ids(events: Iterable[Mapping[str, Any]]) -> list[str]:
    return sorted({
        str(event["event_id"])
        for event in events
        if event.get("event_id") is not None
    })


def gap_evidence_record_ids(events: Iterable[Mapping[str, Any]]) -> list[str]:
    return sorted({
        str(event["evidence_record_id"])
        for event in events
        if event.get("evidence_record_id") is not None
    })


def empty_window_context(
    *,
    session_start: datetime,
    authorized_start: datetime,
    dependency_fields: Mapping[str, Iterable[str]],
    warning_flags: Mapping[str, Iterable[str]] | None = None,
) -> WindowGapContext:
    return WindowGapContext.from_document({
        "session_start": session_start,
        "authorized_start": authorized_start,
        "dependency_fields": dependency_fields,
        "gap_events": [],
        "gap_manifest_ref": None,
        "warning_flags": warning_flags or {},
    })


def duration_context_start(
    output_start: datetime, session_start: datetime, history: timedelta,
) -> datetime:
    """Planner-facing helper retained to make the three time concepts explicit."""
    return max(output_start - history, session_start)
