import json
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quantsage_discovery.ingest.calendar import (
    expected_cffex_dates,
    parse_calendar_json_response,
    read_frozen_calendar,
    read_local_calendar,
    sha256_file,
    validate_calendar_dates,
)
from quantsage_discovery.ingest.sessions import (
    classify_event_time,
    freeze_session_rules,
    load_session_rules,
)

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "configs/reference/cffex_calendar_evidence.json"


def test_bounded_cffex_calendar_evidence_has_exact_expected_weekdays() -> None:
    start = date(2020, 1, 2)
    end = date(2026, 7, 31)
    expected = expected_cffex_dates(EVIDENCE, start, end)

    assert len(expected) == 1594
    assert date(2020, 1, 31) not in expected
    assert date(2020, 2, 3) in expected
    assert date(2024, 2, 8) in expected
    assert date(2024, 2, 19) in expected


def test_real_local_calendar_is_read_only_pinned_and_complete_for_january() -> None:
    data_root = Path("/data/marketdata")
    if not (data_root / "catalog/duckdb/current.json").is_file():
        pytest.skip("Brighton Marketdata calendar catalog is unavailable")

    snapshot = read_local_calendar(
        data_root=data_root,
        evidence_path=EVIDENCE,
        calendar_id="CN_STOCK",
        start=date(2026, 1, 5),
        end=date(2026, 1, 7),
    )

    assert snapshot.table.num_rows == 3
    assert snapshot.provenance["db_release_id"] == "db-restructure-full-v16"
    assert snapshot.provenance["selected_file_sha256"].startswith("sha256:")


def test_calendar_rows_must_match_id_coverage_and_unique_keys() -> None:
    expected = expected_cffex_dates(EVIDENCE, date(2026, 1, 5), date(2026, 1, 7))
    assert validate_calendar_dates("CN_STOCK", expected, "CN_STOCK", expected)

    with pytest.raises(ValueError, match="unapproved calendar_id"):
        validate_calendar_dates("OTHER", expected, "CN_STOCK", expected)
    with pytest.raises(ValueError, match="duplicates"):
        validate_calendar_dates("CN_STOCK", [*expected, expected[0]], "CN_STOCK", expected)
    with pytest.raises(ValueError, match="coverage"):
        validate_calendar_dates("CN_STOCK", expected[:-1], "CN_STOCK", expected)


def test_verified_frozen_calendar_fallback_checks_hash_schema_and_coverage(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "calendar.parquet"
    pq.write_table(
        pa.table(
            {
                "calendar_id": pa.array(["CN_STOCK"] * 3, pa.string()),
                "trade_date": pa.array(
                    [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)], pa.date32()
                ),
            }
        ),
        snapshot_path,
    )
    frozen = read_frozen_calendar(
        snapshot_path=snapshot_path,
        expected_sha256=sha256_file(snapshot_path),
        evidence_path=EVIDENCE,
        calendar_id="CN_STOCK",
        start=date(2026, 1, 5),
        end=date(2026, 1, 7),
    )

    assert frozen.table.num_rows == 3
    assert frozen.provenance["provider"] == "verified_frozen_snapshot_fallback"
    with pytest.raises(ValueError, match="hash mismatch"):
        read_frozen_calendar(
            snapshot_path=snapshot_path,
            expected_sha256="0" * 64,
            evidence_path=EVIDENCE,
            calendar_id="CN_STOCK",
            start=date(2026, 1, 5),
            end=date(2026, 1, 7),
        )


def test_api_rows_are_parsed_by_column_order_and_types() -> None:
    response = {
        "columns": [
            {"name": "calendar_id", "type": "string"},
            {"name": "trade_date", "type": "date32[day]"},
        ],
        "rows": [["CN_STOCK", "2026-01-05"], ["CN_STOCK", "2026-01-06"]],
        "row_count": 2,
        "truncated": False,
        "release": "db-test-v1",
    }
    rows = parse_calendar_json_response(response, expected_release="db-test-v1")

    assert rows == [("CN_STOCK", date(2026, 1, 5)), ("CN_STOCK", date(2026, 1, 6))]


@pytest.mark.parametrize(
    ("response_update", "expected_release", "message"),
    [
        ({"truncated": True}, "db-test-v1", "truncated"),
        ({"release": "db-other-v2"}, "db-test-v1", "release"),
        ({"columns": [{"name": "trade_date", "type": "date32[day]"}]}, "db-test-v1", "columns"),
    ],
)
def test_api_response_rejects_truncation_mixed_release_and_wrong_columns(
    response_update: dict, expected_release: str, message: str
) -> None:
    response = {
        "columns": [
            {"name": "calendar_id", "type": "string"},
            {"name": "trade_date", "type": "date32[day]"},
        ],
        "rows": [["CN_STOCK", "2026-01-05"]],
        "row_count": 1,
        "truncated": False,
        "release": "db-test-v1",
    }
    response.update(response_update)

    with pytest.raises(ValueError, match=message):
        parse_calendar_json_response(response, expected_release=expected_release)


def test_session_rules_keep_auction_separate_and_quarantine_after_close() -> None:
    evidence = EVIDENCE

    auction = classify_event_time(
        "T2603", date(2026, 1, 5), datetime.fromisoformat("2026-01-05T09:29:00"), [date(2026, 1, 5)], evidence
    )
    after_close = classify_event_time(
        "T2603", date(2026, 1, 5), datetime.fromisoformat("2026-01-05T15:24:00"), [date(2026, 1, 5)], evidence
    )
    regular_afternoon = classify_event_time(
        "T2603", date(2026, 1, 5), datetime.fromisoformat("2026-01-05T13:01:00"), [date(2026, 1, 5)], evidence
    )

    assert auction is not None
    assert auction.segment_id == "opening_auction_matching"
    assert auction.continuous_trading is False
    assert auction.session_id == "CN_STOCK:2026-01-05:opening_auction_matching"
    assert after_close is None
    assert regular_afternoon is not None
    assert regular_afternoon.segment_id == "continuous_afternoon"


def test_historical_session_change_and_last_trading_day_boundary() -> None:
    evidence = EVIDENCE
    dates_2020 = [date(2020, 1, 2), date(2020, 1, 3)]

    old_matching = classify_event_time(
        "T2003", date(2020, 1, 2), datetime.fromisoformat("2020-01-02T09:14:00"), dates_2020, evidence
    )
    old_open = classify_event_time(
        "T2003", date(2020, 1, 2), datetime.fromisoformat("2020-01-02T09:15:00"), dates_2020, evidence
    )
    expiry_afternoon = classify_event_time(
        "T2603", date(2026, 3, 13), datetime.fromisoformat("2026-03-13T13:01:00"),
        [date(2026, 3, 13), date(2026, 3, 16)], evidence
    )
    shifted_expiry_afternoon = classify_event_time(
        "T2402", date(2024, 2, 19), datetime.fromisoformat("2024-02-19T13:01:00"),
        [date(2024, 2, 19), date(2024, 2, 20)], evidence
    )
    date_mismatch = classify_event_time(
        "T2603", date(2026, 1, 5), datetime.fromisoformat("2026-01-06T13:01:00"), [date(2026, 1, 5)], evidence
    )

    assert old_matching is not None and old_matching.segment_id == "opening_auction_matching"
    assert old_open is not None and old_open.segment_id == "continuous_morning"
    assert expiry_afternoon is None
    assert shifted_expiry_afternoon is None
    assert date_mismatch is None


def test_ambiguous_clock_segments_are_rejected_when_rules_load(tmp_path: Path) -> None:
    document = json.loads(EVIDENCE.read_text(encoding="utf-8"))
    document["session_rules"]["rules"][1]["morning_continuous"]["start"] = "09:29:00"
    path = tmp_path / "overlapping-rules.json"
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match="overlapping session segments"):
        load_session_rules(path)


def test_overlapping_product_rule_effective_dates_are_rejected(tmp_path: Path) -> None:
    document = json.loads(EVIDENCE.read_text(encoding="utf-8"))
    duplicate = dict(document["session_rules"]["rules"][1])
    duplicate["rule_id"] = "duplicate-post-2020"
    document["session_rules"]["rules"].append(duplicate)
    path = tmp_path / "overlapping-effective-rules.json"
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match="overlapping effective session rules"):
        load_session_rules(path)


def test_frozen_session_rules_include_source_provenance() -> None:
    frozen = freeze_session_rules(EVIDENCE)

    assert frozen["timezone"] == "Asia/Shanghai"
    assert frozen["interval_convention"].startswith("[start, end)")
    assert frozen["source_refs"]
    assert {source["source_ref"] for source in frozen["sources"]} == set(frozen["source_refs"])
    assert frozen["evidence_sha256"].startswith("sha256:")
