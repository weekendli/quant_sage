"""Inspect one contract/day through an adapted onboarding DataSource.

Usage:
    python -m examples.onboarding_source --source /path/to/source.json \
        --instrument T2603 --date 2026-01-05
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Any

from quantsage_discovery.research.onboarding_adapter import OnboardingDataSourceReader


def _json_number(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def inspect_source(source_path: Path, instrument_id: str, trade_date: str) -> dict[str, Any]:
    source = json.loads(source_path.read_text(encoding="utf-8"))
    reader = OnboardingDataSourceReader(source)
    columns = [
        "exchange", "instrument_id", "trade_date", "event_time", "available_time",
        "session_id", "bid_price_1", "ask_price_1", "bid_size_1", "ask_size_1",
        "quality_flags",
    ]
    quotes = reader.read_quotes(instrument_id, trade_date, columns=columns)
    if not quotes.num_rows:
        return {
            "source_ref": f"{source['source_id']}@{source['version']}",
            "instrument_id": instrument_id,
            "trade_date": trade_date,
            "row_count": 0,
            "diagnostic": "NO_OBSERVED_QUOTES",
        }
    rows = quotes.to_pylist()
    first = rows[0]
    bid = first["bid_price_1"]
    ask = first["ask_price_1"]
    bid_size = first["bid_size_1"]
    ask_size = first["ask_size_1"]
    mid = (bid + ask) / Decimal(2)
    price_spread = ask - bid
    l1_imbalance = (
        (bid_size - ask_size) / (bid_size + ask_size)
        if bid_size + ask_size != 0 else None
    )
    tick_size = reader.tick_size(instrument_id, trade_date)
    spread_ticks = None if tick_size is None else price_spread / tick_size

    by_session: dict[str, list[Any]] = {}
    for row in rows:
        by_session.setdefault(row["session_id"], []).append(row["event_time"])
    session_ranges = {
        session_id: {
            "row_count": len(times),
            "event_time_min": min(times).isoformat(),
            "event_time_max": max(times).isoformat(),
        }
        for session_id, times in sorted(by_session.items())
    }
    gap_events = reader.gap_events(
        instrument_id=instrument_id, trade_date=trade_date,
    ).to_pylist()
    return {
        "source_ref": f"{source['source_id']}@{source['version']}",
        "instrument_id": instrument_id,
        "trade_date": trade_date,
        "row_count": quotes.num_rows,
        "session_ranges": session_ranges,
        "first_snapshot": {
            "event_time": first["event_time"].isoformat(),
            "session_id": first["session_id"],
            "mid": _json_number(mid),
            "price_spread": _json_number(price_spread),
            "l1_imbalance": _json_number(l1_imbalance),
            "tick_size": _json_number(tick_size),
            "spread_ticks": _json_number(spread_ticks),
        },
        "availability": {
            "available_time": None,
            "mode": "research_only",
            "point_in_time_verified": False,
        },
        "tick_dependent_capabilities": {
            "spread_ticks": "AVAILABLE" if tick_size is not None
            else "BLOCKED_MARKET_METADATA_MISSING",
        },
        "quality_flag_counts": dict(Counter(
            flag for row in rows for flag in (row["quality_flags"] or [])
        )),
        "gap_event_counts": {
            "by_kind": dict(Counter(row["event_kind"] for row in gap_events)),
            "by_reason": dict(Counter(
                reason for row in gap_events for reason in row["reason_codes"]
            )),
            "total": len(gap_events),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    print(json.dumps(
        inspect_source(args.source, args.instrument, args.date),
        indent=2,
        sort_keys=True,
    ))


if __name__ == "__main__":
    main()
