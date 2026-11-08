"""Independent real-data regressions for the confirmed 2026-11-08 findings."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from quantsage_discovery.research.business_windows import (
    CATALOG_VERSION,
    compute_business_window,
)
from quantsage_discovery.research.feature_schemas import normalize_duration
from quantsage_discovery.research.l2_primitives import compute_l2_primitive
from quantsage_discovery.research.onboarding_adapter import OnboardingDataSourceReader

SOURCE_PATH = Path(
    "artifacts/step02/adapters/step02-02a-20261107-r3/"
    "cffex_treasury_l2-v2/source.json"
)
SESSION_ID = "CN_STOCK:2026-06-12:continuous_morning"
GAP_EVENT_ID = "767c1fecbf12259a834995654f3bb6753b04e8d5806f261a40824530a4c01592"
GAP_RECORD_ID = "5a17031a699f184f651d4eafcefdd65f58329739c48fd2eaa55b07164b5ad2f5"


def test_real_ts2606_gap_invalidates_obi_and_normal_control_then_recovers() -> None:
    source = json.loads(SOURCE_PATH.read_text(encoding="utf-8"))
    reader = OnboardingDataSourceReader(source)
    start = datetime(2026, 6, 12, 1, 30, tzinfo=UTC)
    end = start + timedelta(minutes=15)
    partition = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "exchange": "CFFEX",
        "instrument_id": "TS2606",
        "trade_date": "2026-06-12",
        "session_id": SESSION_ID,
    }
    primitive = compute_l2_primitive(
        reader,
        source,
        {
            "feature_id": "acceptance_obi",
            "version": 1,
            "source_requirements": {"primitive_id": "obi_snapshot"},
            "parameters": {"space": "d0"},
        },
        partition,
        partition,
    )
    gaps = reader.gap_events(
        instrument_id="TS2606",
        trade_date="2026-06-12",
        session_id=SESSION_ID,
    ).to_pylist()
    target_gap = next(event for event in gaps if event["event_id"] == GAP_EVENT_ID)
    assert target_gap["start_time"] == datetime(2026, 6, 12, 1, 34, 0, 800000, tzinfo=UTC)
    assert target_gap["evidence_record_id"] == GAP_RECORD_ID
    assert "INVALID_L1_BOOK" in target_gap["reason_codes"]

    fields = {
        "bid_price_1", "ask_price_1", "bid_size_1", "ask_size_1",
        *(f"{side}_{field}_{level}"
          for side in ("bid", "ask") for field in ("price", "size")
          for level in range(1, 6)),
    }
    output = compute_business_window(
        {
            "template_ref": f"business_obi@{CATALOG_VERSION}",
            "parameters": {"space": "d0"},
            "time_policy": {"lookback": normalize_duration("1m", "lookback")},
        },
        {"input": {"table": primitive.values}},
        partition,
        context={
            "session_start": start.isoformat(),
            "authorized_start": start.isoformat(),
            "dependency_fields": {"input": fields},
            "gap_events": gaps,
            "gap_manifest_ref": source["gap_manifest_ref"],
            "warning_flags": {},
        },
    )
    rows = output.values.to_pylist()
    audits = output.audit.to_pylist()
    by_time = {row["event_time"]: (row, audit) for row, audit in zip(rows, audits, strict=True)}
    target_time = datetime(2026, 6, 12, 1, 34, 11, 800000, tzinfo=UTC)
    target = by_time[target_time][0]
    target_audit = by_time[target_time][1]
    assert target["value"] is None
    assert target_audit["invalid_reason"] == "KNOWN_SOURCE_GAP"
    assert GAP_EVENT_ID in target_audit["source_gap_event_ids"]
    assert GAP_RECORD_ID in target_audit["source_gap_evidence_record_ids"]
    assert target_audit["gap_manifest_hash"] == source["gap_manifest_ref"]["content_hash"]

    control_time = datetime(2026, 6, 12, 1, 31, 24, 800000, tzinfo=UTC)
    assert by_time[control_time][0]["value"] == pytest.approx(0.0, abs=1e-12)
    assert by_time[control_time][1]["invalid_reason"] is None

    recovery_time = datetime(2026, 6, 12, 1, 36, 17, 300000, tzinfo=UTC)
    assert by_time[recovery_time][0]["value"] == pytest.approx(
        -0.7142857142857143, abs=1e-12,
    )
    assert by_time[recovery_time][1]["invalid_reason"] is None
