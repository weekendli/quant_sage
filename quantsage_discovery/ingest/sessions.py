"""Versioned CFFEX intraday session rules and row-time classification."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from itertools import pairwise
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class SessionClassification:
    segment_id: str
    session_id: str
    continuous_trading: bool
    rule_id: str


@dataclass(frozen=True)
class SessionRules:
    version: str
    timezone: str
    rules: tuple[dict[str, Any], ...]
    evidence: dict[str, Any]


@lru_cache(maxsize=8)
def _load_cached(path: str, mtime_ns: int, size_bytes: int) -> SessionRules:
    del mtime_ns, size_bytes
    evidence = json.loads(Path(path).read_text(encoding="utf-8"))
    session_evidence = evidence["session_rules"]
    rules = tuple(session_evidence["rules"])
    _validate_rules(rules)
    return SessionRules(
        version=str(evidence["evidence_version"]),
        timezone=str(session_evidence["timezone"]),
        rules=rules,
        evidence=evidence,
    )


def _rule_intervals(rule: dict[str, Any]) -> list[tuple[str, time, time]]:
    intervals: list[tuple[str, time, time]] = []
    auction = rule.get("opening_auction")
    if auction:
        intervals.extend(
            [
                (
                    "opening_auction_submission",
                    _parse_time(auction["start"]),
                    _parse_time(auction["order_submission_end"]),
                ),
                (
                    "opening_auction_matching",
                    _parse_time(auction["matching_start"]),
                    _parse_time(auction["end"]),
                ),
            ]
        )
    for key, segment in (
        ("morning_continuous", rule.get("morning_continuous")),
        ("afternoon_continuous_regular_day", rule.get("afternoon_continuous_regular_day")),
    ):
        if segment:
            intervals.append((key, _parse_time(segment["start"]), _parse_time(segment["end"])))
    return intervals


def _validate_rules(rules: tuple[dict[str, Any], ...]) -> None:
    by_product: dict[str, list[tuple[date, date, str]]] = {}
    for rule in rules:
        first, last = _rule_dates(rule)
        if first > last:
            raise ValueError(f"invalid effective date range in session rule {rule.get('rule_id')!r}")
        intervals = _rule_intervals(rule)
        if not intervals:
            raise ValueError(f"session rule {rule.get('rule_id')!r} has no time segments")
        ordered = sorted(intervals, key=lambda item: (item[1], item[2], item[0]))
        for segment_id, start, end in ordered:
            if start >= end:
                raise ValueError(
                    f"session segment {segment_id!r} has an empty or reversed interval"
                )
        for previous, current in pairwise(ordered):
            if current[1] < previous[2]:
                raise ValueError(
                    f"overlapping session segments in {rule.get('rule_id')!r}: "
                    f"{previous[0]!r} and {current[0]!r}"
                )
        for product in rule.get("products", []):
            by_product.setdefault(product, []).append((first, last, str(rule.get("rule_id"))))
    for product, applicable in by_product.items():
        applicable.sort(key=lambda item: item[0])
        for previous, current in pairwise(applicable):
            if current[0] <= previous[1]:
                raise ValueError(
                    f"overlapping effective session rules for {product}: "
                    f"{previous[2]!r} and {current[2]!r}"
                )


def load_session_rules(path: str | Path) -> SessionRules:
    evidence_path = Path(path).expanduser().resolve()
    stat = evidence_path.stat()
    return _load_cached(str(evidence_path), stat.st_mtime_ns, stat.st_size)


def _parse_time(value: str) -> time:
    return time.fromisoformat(value)


def _rule_dates(rule: dict[str, Any]) -> tuple[date, date]:
    start_raw = rule.get("effective_from", rule.get("effective_for_scope_start", "0001-01-01"))
    end_raw = rule.get("effective_for_scope_end", "9999-12-31")
    return date.fromisoformat(start_raw), date.fromisoformat(end_raw)


def _product_rule(product: str, trade_date: date, rules: SessionRules) -> dict[str, Any] | None:
    matches = []
    for rule in rules.rules:
        first, last = _rule_dates(rule)
        if product in rule.get("products", []) and first <= trade_date <= last:
            matches.append(rule)
    if len(matches) > 1:
        raise ValueError(f"multiple session rules apply to {product} on {trade_date}")
    return matches[0] if matches else None


def _second_friday(year: int, month: int) -> date:
    first = date(year, month, 1)
    days_to_friday = (4 - first.weekday()) % 7
    return first + timedelta(days=days_to_friday + 7)


def _contract_expiry_date(
    instrument_id: str, trade_date: date, calendar_dates: set[date]
) -> date | None:
    match = re.fullmatch(r"(?:T|TF|TS|TL)(\d{2})(\d{2})", instrument_id)
    if match is None:
        return None
    year = 2000 + int(match.group(1))
    month = int(match.group(2))
    if not 1 <= month <= 12:
        return None
    nominal = _second_friday(year, month)
    if nominal in calendar_dates:
        return nominal
    if trade_date < nominal:
        # For rows before the expiry month, it is enough to know expiry is later.
        return nominal
    return min((value for value in calendar_dates if value > nominal), default=None)


def contract_last_trading_date(instrument_id: str, calendar_dates: set[date]) -> date | None:
    """Resolve the contract's second-Friday expiry, shifted to the next trading date."""
    match = re.fullmatch(r"(?:T|TF|TS|TL)(\d{2})(\d{2})", instrument_id)
    if match is None:
        return None
    year = 2000 + int(match.group(1))
    month = int(match.group(2))
    if not 1 <= month <= 12:
        return None
    nominal = _second_friday(year, month)
    if nominal in calendar_dates:
        return nominal
    return min((value for value in calendar_dates if value > nominal), default=nominal)


def classify_event_time(
    instrument_id: str,
    trade_date: date,
    local_event_time: datetime,
    calendar_dates: list[date] | tuple[date, ...] | set[date],
    rules: str | Path | SessionRules,
    *,
    calendar_id: str = "CN_STOCK",
) -> SessionClassification | None:
    """Return a single auction/continuous segment, or None for out-of-session rows."""
    if isinstance(rules, SessionRules):
        session_rules = rules
    else:
        session_rules = load_session_rules(rules)
    calendar = set(calendar_dates)
    if trade_date not in calendar:
        return None
    if local_event_time.date() != trade_date:
        return None
    product = next((prefix for prefix in ("TF", "TS", "TL", "T") if instrument_id.startswith(prefix)), "")
    rule = _product_rule(product, trade_date, session_rules)
    if rule is None:
        return None

    local_dt = local_event_time
    if local_dt.tzinfo is not None:
        local_dt = local_dt.astimezone(ZoneInfo(session_rules.timezone)).replace(tzinfo=None)
    clock_time = local_dt.time()
    auction = rule.get("opening_auction")
    candidates: list[tuple[str, time, time, bool]] = []
    if auction:
        auction_start = _parse_time(auction["start"])
        submission_end = _parse_time(auction["order_submission_end"])
        matching_start = _parse_time(auction["matching_start"])
        auction_end = _parse_time(auction["end"])
        candidates.extend(
            [
                ("opening_auction_submission", auction_start, submission_end, False),
                ("opening_auction_matching", matching_start, auction_end, False),
            ]
        )
    morning = rule.get("morning_continuous")
    if morning:
        candidates.append(
            (
                "continuous_morning",
                _parse_time(morning["start"]),
                _parse_time(morning["end"]),
                True,
            )
        )
    afternoon = rule.get("afternoon_continuous_regular_day")
    if afternoon:
        expiry = _contract_expiry_date(instrument_id, trade_date, calendar)
        if rule.get("last_trading_day_afternoon") != "closed" or expiry != trade_date:
            candidates.append(
                (
                    "continuous_afternoon",
                    _parse_time(afternoon["start"]),
                    _parse_time(afternoon["end"]),
                    True,
                )
            )
    matches = [
        (segment_id, continuous)
        for segment_id, start, end, continuous in candidates
        if start <= clock_time < end
    ]
    if len(matches) > 1:
        raise ValueError(
            f"multiple session segments match {instrument_id} at {local_dt.isoformat()}"
        )
    if matches:
        segment_id, continuous = matches[0]
        return SessionClassification(
                segment_id=segment_id,
                session_id=f"{calendar_id}:{trade_date.isoformat()}:{segment_id}",
                continuous_trading=continuous,
                rule_id=str(rule["rule_id"]),
            )
    return None


def freeze_session_rules(path: str | Path) -> dict[str, Any]:
    """Return the evidenced, versioned session policy to persist in onboarding output."""
    evidence_path = Path(path).expanduser().resolve()
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    session_rules = evidence["session_rules"]
    source_refs = sorted(
        {source_ref for rule in session_rules["rules"] for source_ref in rule.get("source_refs", [])}
    )
    sources = [
        source for source in evidence.get("sources", []) if source.get("source_ref") in source_refs
    ]
    evidence_hash = "sha256:" + hashlib.sha256(evidence_path.read_bytes()).hexdigest()
    return {
        "schema_version": "1.0.0",
        "version": str(evidence["evidence_version"]),
        "timezone": session_rules["timezone"],
        "interval_convention": "[start, end)",
        "interval_convention_evidence": session_rules["software_interval_convention"],
        "last_trading_day_rule": session_rules["last_trading_day_rule"],
        "rules": session_rules["rules"],
        "sample_classifications": session_rules["sample_classifications"],
        "source_refs": source_refs,
        "sources": sources,
        "evidence_uri": "configs/reference/cffex_calendar_evidence.json",
        "evidence_sha256": evidence_hash,
    }
