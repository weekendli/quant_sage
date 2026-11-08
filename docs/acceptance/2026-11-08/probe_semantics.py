"""Independent acceptance probes; run from repo root with PYTHONPATH=. python PATH.

These probes report requirement violations without changing implementation.
The real-gap probe reads existing frozen artifacts only.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from quantsage_discovery.research.business_windows import (
    business_catalog_entries,
    compute_business_window,
)
from quantsage_discovery.research.feature_schemas import normalize_duration
from quantsage_discovery.research.l2_primitives import compute_l2_primitive
from quantsage_discovery.research.onboarding_adapter import OnboardingDataSourceReader
from quantsage_discovery.research.providers import make_feature_values_table

START = datetime(2026, 1, 5, 1, 30, tzinfo=UTC)


def feature():
    return {
        "template_ref": "business_obi@1.0.0",
        "parameters": {"space": "d0"},
        "time_policy": {"lookback": normalize_duration("1m", "lookback")},
    }


def scope(start, end):
    return {"start": start.isoformat(), "end": end.isoformat()}


def context(gaps=None):
    fields = {
        "bid_price_1", "ask_price_1", "bid_size_1", "ask_size_1",
        *(f"{side}_{field}_{level}"
          for side in ("bid", "ask") for field in ("price", "size")
          for level in range(1, 6)),
    }
    return {
        "session_start": START.isoformat(),
        "authorized_start": START.isoformat(),
        "dependency_fields": {"input": fields},
        "gap_events": gaps or [],
        "gap_manifest_ref": None,
        "warning_flags": {},
    }


def history_probe():
    table = make_feature_values_table([
        {
            "event_time": START + timedelta(seconds=seconds),
            "available_time": None,
            "instrument_id": "T2603", "trade_date": "2026-01-05",
            "session_id": "same-session", "value": value,
        }
        for seconds, value in [(0, 0.8), (20, 0.0), (40, 0.4), (60, 0.6), (80, -0.2)]
    ])
    inputs = {"input": {"table": table}}
    identity = {
        "exchange": "CFFEX",
        "instrument_id": "T2603",
        "trade_date": "2026-01-05",
        "session_id": "same-session",
    }
    wide = compute_business_window(
        feature(), inputs,
        {**scope(START, START + timedelta(seconds=120)), **identity},
        context=context(),
    )
    narrow = compute_business_window(
        feature(), inputs,
        {
            **scope(START + timedelta(seconds=60), START + timedelta(seconds=120)),
            **identity,
        },
        context=context(),
    )
    expected = wide.values["value"].to_pylist()[3:]
    actual = narrow.values["value"].to_pylist()
    return {
        "passed": actual == expected,
        "requirement": "authorized same-session history remains usable when output start changes",
        "expected": expected, "actual": actual,
        "invalid_reasons": narrow.audit["invalid_reason"].to_pylist(),
    }


def real_gap_probe():
    source_path = Path(
        "artifacts/step02/adapters/step02-02a-20261107-r3/cffex_treasury_l2-v2/source.json"
    )
    source = json.loads(source_path.read_text())
    reader = OnboardingDataSourceReader(source)
    start = datetime(2026, 6, 12, 1, 30, tzinfo=UTC)
    end = start + timedelta(minutes=5)
    session = "CN_STOCK:2026-06-12:continuous_morning"
    partition = {
        **scope(start, end), "exchange": "CFFEX", "instrument_id": "TS2606",
        "trade_date": "2026-06-12", "session_id": session,
    }
    primitive = compute_l2_primitive(
        reader, source,
        {
            "feature_id": "acceptance_obi", "version": 1,
            "source_requirements": {"primitive_id": "obi_snapshot"},
            "parameters": {"space": "d0"},
        },
        partition, partition,
    )
    gaps = [
        row for row in reader.gap_events(
            instrument_id="TS2606", trade_date="2026-06-12", session_id=session,
        ).to_pylist()
        if row["event_kind"] == "QUARANTINED_RECORD"
        and start + timedelta(seconds=60) < row["start_time"] < end
        and "INVALID_L1_BOOK" in row["reason_codes"]
    ]
    real_context = context(gaps)
    real_context["gap_manifest_ref"] = source["gap_manifest_ref"]
    output = compute_business_window(
        feature(), {"input": {"table": primitive.values}}, partition,
        context=real_context,
    )
    violations = []
    for row, audit in zip(output.values.to_pylist(), output.audit.to_pylist(), strict=True):
        overlapping = [
            gap for gap in gaps
            if row["event_time"] - timedelta(seconds=60)
            < gap["start_time"] < row["event_time"]
        ]
        if overlapping and row["value"] is not None:
            violations.append({
                "event_time": row["event_time"].isoformat(), "value": row["value"],
                "invalid_reason": audit["invalid_reason"],
                "gap_event_id": overlapping[0]["event_id"],
                "gap_time": overlapping[0]["start_time"].isoformat(),
                "gap_reason": overlapping[0]["reason_codes"],
                "evidence_record_id": overlapping[0]["evidence_record_id"],
            })
    if not gaps or not output.values.num_rows:
        raise RuntimeError("real-gap fixture did not exercise the required source range")
    return {
        "passed": not violations,
        "requirement": "known source gaps affecting OBI invalidate overlapping business windows",
        "source_ref": "cffex_treasury_l2_full79@2",
        "source_content_hash": source["content_hash"],
        "gap_manifest_ref": source["gap_manifest_ref"],
        "primitive_rows": primitive.values.num_rows,
        "relevant_gap_count": len(gaps),
        "non_null_windows_crossing_known_gaps": len(violations),
        "examples": violations[:3],
    }


def catalog_probe():
    entries = business_catalog_entries()
    entry = next(item for item in entries if item["family"] == "F02")
    return {
        "passed": entry["factor_name"] == "obi__d0__h1s",
        "requirement": "business factor names preserve the original d-space notation",
        "expected": "obi__d0__h1s", "actual": entry["factor_name"],
    }


if __name__ == "__main__":
    print(json.dumps({
        "history": history_probe(), "real_gap": real_gap_probe(), "catalog": catalog_probe(),
    }, indent=2))
