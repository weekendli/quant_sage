"""Step 04 business-window catalog, time-boundary, gap and formula tests."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from quantsage_discovery.research.business_windows import (
    CATALOG_VERSION,
    business_catalog_entries,
    compute_business_window,
    legacy_factor_name_aliases,
)
from quantsage_discovery.research.feature_schemas import normalize_duration
from quantsage_discovery.research.providers import make_feature_values_table

START = datetime(2026, 1, 5, 1, 30, tzinfo=UTC)
OBI_FIELDS = {
    "bid_price_1", "ask_price_1", "bid_size_1", "ask_size_1",
} | {
    f"{side}_{field}_{level}"
    for side in ("bid", "ask")
    for field in ("price", "size")
    for level in range(1, 6)
}


def _table(rows: list[tuple[float, float | None]]) -> Any:
    return make_feature_values_table([{
        "event_time": START + timedelta(seconds=seconds),
        "available_time": None,
        "instrument_id": "T2603",
        "trade_date": "2026-01-05",
        "session_id": "CN_STOCK:2026-01-05:continuous_morning",
        "value": value,
    } for seconds, value in rows])


def _feature(template: str, *, lookback: str = "30s", max_age: str | None = None,
             parameters: dict[str, Any] | None = None) -> dict[str, Any]:
    policy = {"lookback": normalize_duration(lookback, "lookback")}
    if max_age is not None:
        policy["max_endpoint_age"] = normalize_duration(max_age, "max_endpoint_age")
    return {
        "template_ref": f"business_{template}@{CATALOG_VERSION}",
        "parameters": parameters or {},
        "time_policy": policy,
    }


def _partition(start_seconds: float = -60, end_seconds: float = 120) -> dict[str, Any]:
    return {
        "start": (START + timedelta(seconds=start_seconds)).isoformat(),
        "end": (START + timedelta(seconds=end_seconds)).isoformat(),
        "exchange": "CFFEX",
        "instrument_id": "T2603",
        "trade_date": START.date().isoformat(),
        "session_id": "CN_STOCK:2026-01-05:continuous_morning",
    }


def _context(
    *,
    session_start: float = -60,
    authorized_start: float = -60,
    dependency_fields: dict[str, set[str]] | None = None,
    gap_events: list[dict[str, Any]] | None = None,
    warning_flags: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    return {
        "session_start": (START + timedelta(seconds=session_start)).isoformat(),
        "authorized_start": (START + timedelta(seconds=authorized_start)).isoformat(),
        "dependency_fields": dependency_fields or {"input": OBI_FIELDS},
        "gap_events": gap_events or [],
        "gap_manifest_ref": {
            "uri": "file:///frozen/gap_manifest.json",
            "content_hash": "sha256:" + "a" * 64,
        },
        "warning_flags": warning_flags or {},
    }


def _gap(
    second: float,
    *,
    fields: list[str] | None = None,
    event_id: str = "gap-event-1",
    boundary: str = "point",
) -> dict[str, Any]:
    at = START + timedelta(seconds=second)
    return {
        "event_id": event_id,
        "event_kind": "QUARANTINED_RECORD",
        "certainty": "OBSERVED",
        "exchange": "CFFEX",
        "instrument_id": "T2603",
        "trade_date": START.date(),
        "session_id": "CN_STOCK:2026-01-05:continuous_morning",
        "start_time": at,
        "end_time": at,
        "boundary": boundary,
        "reason_codes": ["INVALID_L1_BOOK"],
        "affected_fields": fields or ["bid_price_1", "ask_price_1"],
        "evidence_record_id": "record-1",
    }


def _canonical_catalog_names() -> set[str]:
    entries = set()
    spaces = ("d0", "d1", "d2", "d3", "d5")
    sizes = ("q25", "q50", "q100")
    lookbacks = ("1s", "3s", "1m", "5m", "10m", "15m", "30m", "60m")
    for lookback in lookbacks:
        entries.add(f"spread_ticks__h{lookback}")
        entries.update(f"obi__{space}__h{lookback}" for space in spaces)
        entries.update(f"wobi__{space}__h{lookback}" for space in spaces)
        entries.update(
            f"min_coverage__{space}__{size}__h{lookback}"
            for space in spaces for size in sizes
        )
        entries.update(
            f"capacity_imb__{space}__{size}__h{lookback}"
            for space in spaces for size in sizes
        )
        entries.update(
            f"{family}__{size}__h{lookback}"
            for family in ("size_spread", "fill_dist_asym", "walk_cost_asym")
            for size in sizes
        )
        entries.update(
            f"{family}__h{lookback}"
            for family in (
                "active_flow_imb", "volume_to_depth", "mid_move_ticks", "flow_impact_eff",
            )
        )
    return entries


def test_catalog_names_counts_and_pending_semantics_are_exact() -> None:
    entries = business_catalog_entries()
    assert len(entries) == 432
    assert Counter(entry["family"] for entry in entries) == {
        "F01": 8, "F02": 40, "F03": 40, "F04": 120, "F05": 120,
        "F06": 24, "F07": 24, "F08": 24, "F09": 8, "F10": 8,
        "F11": 8, "F12": 8,
    }
    assert sum(entry["status"] == "pending_semantics" for entry in entries) == 48
    assert len({entry["factor_name"] for entry in entries}) == 432
    expected = _canonical_catalog_names()
    assert len(expected) == 432
    assert {entry["factor_name"] for entry in entries} == expected
    assert not any("__s" in entry["factor_name"] for entry in entries)
    first_f02 = next(entry for entry in entries if entry["family"] == "F02")
    assert first_f02["factor_name"] == "obi__d0__h1s"


def test_legacy_s_space_names_map_one_to_one_to_canonical_names() -> None:
    canonical = {entry["factor_name"] for entry in business_catalog_entries()}
    aliases = legacy_factor_name_aliases()
    assert len(aliases) == 320
    assert len(set(aliases.values())) == 320
    assert set(aliases.values()).issubset(canonical)
    assert aliases["obi__s0__h1s"] == "obi__d0__h1s"
    assert aliases["capacity_imb__s5__q100__h60m"] == \
        "capacity_imb__d5__q100__h60m"


def test_snapshot_mean_is_mean_of_ratios_not_aggregate_ratio() -> None:
    values = _table([(0, 0.8), (10, 0.0), (31, 0.6)])
    output = compute_business_window(
        _feature("obi", parameters={"space": "d0"}),
        {"input": {"table": values}},
        _partition(),
        context=_context(),
    )
    assert output.values["value"].to_pylist()[1] == pytest.approx(0.4)
    assert output.values["value"].to_pylist()[2] == pytest.approx(0.3)
    assert output.audit["n_total"].to_pylist()[1] == 2


def test_future_append_does_not_change_existing_prefix() -> None:
    prefix = _table([(0, 0.8), (10, 0.0), (20, 0.4)])
    full = _table([(0, 0.8), (10, 0.0), (20, 0.4), (40, -1.0)])
    feature = _feature("obi", lookback="15s", parameters={"space": "d0"})
    left = compute_business_window(
        feature, {"input": {"table": prefix}}, _partition(), context=_context(),
    )
    right = compute_business_window(
        feature, {"input": {"table": full}}, _partition(), context=_context(),
    )
    assert left.values["value"].to_pylist() == right.values["value"].to_pylist()[:3]


def test_endpoint_equal_time_age_and_mid_window_reset() -> None:
    buy = _table([(0, 100), (10, 110), (20, 120)])
    sell = _table([(0, 200), (10, 200), (20, 205)])
    no_reset = _table([(0, 0), (10, 0), (20, 0)])
    inputs = {
        "buy": {"table": buy}, "sell": {"table": sell},
        "buy_reset": {"table": no_reset}, "sell_reset": {"table": no_reset},
    }
    output = compute_business_window(
        _feature("active_flow_imb", lookback="10s", max_age="1s"),
        inputs, _partition(),
        context=_context(
            session_start=-10,
            authorized_start=-10,
            dependency_fields={
                "buy": {"BUYVOL"},
                "sell": {"SELLVOL"},
                "buy_reset": {"BUYVOL"},
                "sell_reset": {"SELLVOL"},
            },
        ),
    )
    assert output.values["value"].to_pylist()[2] == pytest.approx(1 / 3)
    assert output.audit["matched_buy_time"].to_pylist()[2].isoformat() == \
        (START + timedelta(seconds=10)).isoformat()

    reset_at_middle = _table([(0, 0), (15, 1), (20, 0)])
    inputs["buy_reset"] = {"table": reset_at_middle}
    invalid = compute_business_window(
        _feature("active_flow_imb", lookback="10s", max_age="1s"),
        inputs, _partition(),
        context=_context(
            session_start=-10,
            authorized_start=-10,
            dependency_fields={
                "buy": {"BUYVOL"},
                "sell": {"SELLVOL"},
                "buy_reset": {"BUYVOL"},
                "sell_reset": {"SELLVOL"},
            },
        ),
    )
    assert invalid.values["value"].to_pylist()[2] is None
    assert invalid.audit["invalid_reason"].to_pylist()[2] == "WINDOW_RESET"

    late_buy = _table([(0, 100), (9, 105), (20, 120)])
    late_sell = _table([(0, 200), (9, 210), (20, 205)])
    strict = compute_business_window(
        _feature("active_flow_imb", lookback="10s", max_age="1ns"),
        {**inputs, "buy": {"table": late_buy}, "sell": {"table": late_sell},
         "buy_reset": {"table": no_reset}},
        {**_partition(), "start": (START + timedelta(seconds=1)).isoformat()},
        context=_context(
            session_start=-10,
            authorized_start=-10,
            dependency_fields={
                "buy": {"BUYVOL"},
                "sell": {"SELLVOL"},
                "buy_reset": {"BUYVOL"},
                "sell_reset": {"SELLVOL"},
            },
        ),
    )
    assert strict.audit["invalid_reason"].to_pylist()[-1] == "ENDPOINT_AGE_EXCEEDED"


def test_volume_sum_divided_by_mean_depth_and_zero_net_flow() -> None:
    volume = _table([(0, 2), (10, 3), (20, 5)])
    depth = _table([(0, 10), (10, 20), (20, 30)])
    output = compute_business_window(
        _feature("volume_to_depth", lookback="10s"),
        {"volume": {"table": volume}, "depth": {"table": depth}},
        _partition(),
        context=_context(
            session_start=-10,
            authorized_start=-10,
            dependency_fields={"volume": {"TRADEVOLUME"}, "depth": OBI_FIELDS},
        ),
    )
    assert output.values["value"].to_pylist()[2] == pytest.approx(1 / 6)

    buy = _table([(0, 100), (10, 110), (20, 120)])
    sell = _table([(0, 200), (10, 200), (20, 210)])
    zero = _table([(0, 0), (10, 0), (20, 0)])
    mid = _table([(0, 100), (10, 101), (20, 102)])
    tick = _table([(0, 0.25), (10, 0.25), (20, 0.25)])
    result = compute_business_window(
        _feature("flow_impact_eff", lookback="10s", max_age="1s"),
        {
            "buy": {"table": buy}, "sell": {"table": sell},
            "buy_reset": {"table": zero}, "sell_reset": {"table": zero},
            "volume": {"table": volume}, "depth": {"table": depth},
            "mid": {"table": mid}, "tick": {"table": tick},
        },
        _partition(),
        context=_context(
            session_start=-10,
            authorized_start=-10,
            dependency_fields={
                "buy": {"BUYVOL"},
                "sell": {"SELLVOL"},
                "buy_reset": {"BUYVOL"},
                "sell_reset": {"SELLVOL"},
                "volume": {"TRADEVOLUME"},
                "depth": OBI_FIELDS,
                "mid": {"bid_price_1", "ask_price_1"},
                "tick": {"tick_size"},
            },
        ),
    )
    assert result.values["value"].to_pylist()[2] is None
    assert result.audit["invalid_reason"].to_pylist()[2] == "ZERO_DENOMINATOR"


def test_known_source_gap_uses_evidence_and_recovers_after_complete_lookback() -> None:
    values = _table([(0, 0.8), (10, 0.0), (15, 0.4), (20, 0.6)])
    feature = _feature("obi", lookback="10s", parameters={"space": "d0"})
    gap_event = _gap(
        10,
        fields=["bid_price_1", "ask_price_1"],
        event_id="767c1fec-real-shape",
    )
    output = compute_business_window(
        feature,
        {"input": {"table": values}},
        _partition(end_seconds=21),
        context=_context(
            session_start=0,
            authorized_start=0,
            gap_events=[gap_event],
        ),
    )
    rows = output.values.to_pylist()
    audits = output.audit.to_pylist()
    by_time = {row["event_time"]: (row, audit) for row, audit in zip(rows, audits, strict=True)}
    at_gap = by_time[START + timedelta(seconds=10)]
    assert at_gap[0]["value"] is None
    assert at_gap[1]["invalid_reason"] == "KNOWN_SOURCE_GAP"
    assert at_gap[1]["source_gap_event_ids"] == ["767c1fec-real-shape"]
    assert at_gap[1]["source_gap_evidence_record_ids"] == ["record-1"]
    assert at_gap[1]["gap_manifest_hash"] == "sha256:" + "a" * 64

    at_left_boundary = by_time[START + timedelta(seconds=20)]
    assert at_left_boundary[0]["value"] == pytest.approx(0.5)
    assert at_left_boundary[1]["invalid_reason"] is None
    assert at_left_boundary[1]["source_gap_event_ids"] == []

    recovered = compute_business_window(
        feature,
        {"input": {"table": values}},
        _partition(start_seconds=15, end_seconds=21),
        context=_context(
            session_start=0,
            authorized_start=0,
            gap_events=[gap_event],
        ),
    )
    assert recovered.values["value"].to_pylist() == [None, pytest.approx(0.5)]


def test_gap_field_scope_warning_and_metric_unavailability_are_distinct() -> None:
    values = _table([(0, 0.8), (5, None), (10, 0.4)])
    feature = _feature("obi", lookback="10s", parameters={"space": "d0"})
    unrelated = _gap(5, fields=["TRADEVOLUME"], event_id="volume-only")
    warning_only = compute_business_window(
        feature,
        {"input": {"table": values}},
        _partition(end_seconds=11),
        context=_context(
            session_start=0,
            authorized_start=0,
            gap_events=[unrelated],
            warning_flags={"input": ["VOLUME_INCREMENT_UNVERIFIED"]},
        ),
    )
    assert warning_only.values["value"].to_pylist()[2] == pytest.approx(0.4)
    assert "VOLUME_INCREMENT_UNVERIFIED" in warning_only.audit["warning_flags"].to_pylist()[2]
    assert warning_only.audit["invalid_reason"].to_pylist()[2] is None

    unavailable = compute_business_window(
        feature,
        {"input": {"table": _table([(0, None), (5, None), (10, None)])}},
        _partition(end_seconds=11),
        context=_context(session_start=0, authorized_start=0),
    )
    assert unavailable.values["value"].to_pylist()[2] is None
    assert unavailable.audit["invalid_reason"].to_pylist()[2] == "NO_VALID_OBSERVATIONS"
    assert "KNOWN_SOURCE_GAP" not in unavailable.invalid_reason_counts


def test_interval_and_null_session_gap_scope_is_exact_by_identity() -> None:
    values = _table([(0, 0.8), (5, 0.1), (10, 0.4), (15, 0.5), (20, 0.6)])
    interval = _gap(5, fields=["bid_price_1"], event_id="interval-gap")
    interval["boundary"] = "closed_open"
    interval["end_time"] = START + timedelta(seconds=8)
    interval["session_id"] = None
    output = compute_business_window(
        _feature("obi", lookback="10s", parameters={"space": "d0"}),
        {"input": {"table": values}},
        _partition(0, 21),
        context=_context(
            session_start=-10,
            authorized_start=-10,
            gap_events=[interval],
        ),
    )
    assert output.values["value"].to_pylist() == [
        pytest.approx(0.8),
        None,
        None,
        None,
        pytest.approx(0.55),
    ]
    assert output.audit["invalid_reason"].to_pylist() == [
        None, "KNOWN_SOURCE_GAP", "KNOWN_SOURCE_GAP", "KNOWN_SOURCE_GAP", None,
    ]

    other_instrument = _gap(5, fields=["bid_price_1"], event_id="other-instrument")
    other_instrument["instrument_id"] = "T2606"
    other_day = _gap(5, fields=["bid_price_1"], event_id="other-date")
    other_day["trade_date"] = START.date().replace(day=6)
    unaffected = compute_business_window(
        _feature("obi", lookback="10s", parameters={"space": "d0"}),
        {"input": {"table": values}},
        _partition(0, 21),
        context=_context(
            session_start=-10,
            authorized_start=-10,
            gap_events=[other_instrument, other_day],
        ),
    )
    assert all(
        reason is None
        for reason in unaffected.audit["invalid_reason"].to_pylist()
    )


def test_output_start_does_not_replace_authorized_session_history() -> None:
    values = _table([(0, 0.8), (20, 0.0), (40, 0.4), (60, 0.6), (80, -0.2)])
    feature = _feature("obi", lookback="1m", parameters={"space": "d0"})
    inputs = {"input": {"table": values}}
    wide = compute_business_window(
        feature, inputs, _partition(0, 121),
        context=_context(session_start=0, authorized_start=0),
    )
    narrow = compute_business_window(
        feature, inputs, _partition(60, 121),
        context=_context(session_start=0, authorized_start=0),
    )
    assert narrow.values["event_time"].to_pylist() == [
        START + timedelta(seconds=60), START + timedelta(seconds=80),
    ]
    assert narrow.values["value"].to_pylist() == [
        pytest.approx(1 / 3, abs=1e-12, rel=1e-12),
        pytest.approx(4 / 15, abs=1e-12, rel=1e-12),
    ]
    assert narrow.values["value"].to_pylist() == wide.values["value"].to_pylist()[3:]
    assert narrow.audit["invalid_reason"].to_pylist() == [None, None]

    unauthorized = compute_business_window(
        feature, inputs, _partition(60, 121),
        context=_context(session_start=0, authorized_start=60),
    )
    assert unauthorized.values["value"].to_pylist() == [None, None]
    assert unauthorized.audit["invalid_reason"].to_pylist() == [
        "HISTORY_INSUFFICIENT", "HISTORY_INSUFFICIENT",
    ]


def test_flow_volume_depth_and_mid_windows_are_invariant_to_output_start() -> None:
    buy = _table([(0, 100), (20, 100), (40, 110), (60, 120), (80, 130)])
    sell = _table([(0, 50), (20, 50), (40, 50), (60, 60), (80, 70)])
    reset = _table([(0, 0), (20, 0), (40, 0), (60, 0), (80, 0)])
    volume = _table([(0, 1), (20, 2), (40, 3), (60, 4), (80, 5)])
    depth = _table([(0, 10), (20, 20), (40, 30), (60, 40), (80, 50)])
    mid = _table([(0, 100), (20, 101), (40, 102), (60, 103), (80, 104)])
    tick = _table([(0, 0.25), (20, 0.25), (40, 0.25), (60, 0.25), (80, 0.25)])
    flow_inputs = {
        "buy": {"table": buy}, "sell": {"table": sell},
        "buy_reset": {"table": reset}, "sell_reset": {"table": reset},
    }
    flow_context = _context(
        session_start=0,
        authorized_start=0,
        dependency_fields={
            "buy": {"BUYVOL"}, "sell": {"SELLVOL"},
            "buy_reset": {"BUYVOL"}, "sell_reset": {"SELLVOL"},
        },
    )
    volume_inputs = {
        "volume": {"table": volume}, "depth": {"table": depth},
    }
    volume_context = _context(
        session_start=0,
        authorized_start=0,
        dependency_fields={"volume": {"TRADEVOLUME"}, "depth": OBI_FIELDS},
    )
    mid_inputs = {"mid": {"table": mid}, "tick": {"table": tick}}
    mid_context = _context(
        session_start=0,
        authorized_start=0,
        dependency_fields={
            "mid": {"bid_price_1", "ask_price_1"},
            "tick": {"tick_size"},
        },
    )
    cases = [
        (-60, 121),
        (60, 121),
    ]

    def common(output, start):
        return [
            (row["event_time"], row["value"])
            for row in output.values.to_pylist()
            if row["event_time"] >= START + timedelta(seconds=start)
        ]

    flow_wide = compute_business_window(
        _feature("active_flow_imb", lookback="1m", max_age="1m"),
        flow_inputs, _partition(*cases[0]), context=flow_context,
    )
    flow_narrow = compute_business_window(
        _feature("active_flow_imb", lookback="1m", max_age="1m"),
        flow_inputs, _partition(*cases[1]), context=flow_context,
    )
    assert common(flow_narrow, 60) == common(flow_wide, 60)
    assert [value for _, value in common(flow_narrow, 60)] == [
        pytest.approx(1 / 3), pytest.approx(1 / 5),
    ]

    volume_wide = compute_business_window(
        _feature("volume_to_depth", lookback="1m"),
        volume_inputs, _partition(*cases[0]), context=volume_context,
    )
    volume_narrow = compute_business_window(
        _feature("volume_to_depth", lookback="1m"),
        volume_inputs, _partition(*cases[1]), context=volume_context,
    )
    assert common(volume_narrow, 60) == common(volume_wide, 60)
    assert all(value is not None for _, value in common(volume_narrow, 60))

    mid_wide = compute_business_window(
        _feature("mid_move_ticks", lookback="1m", max_age="1m"),
        mid_inputs, _partition(*cases[0]), context=mid_context,
    )
    mid_narrow = compute_business_window(
        _feature("mid_move_ticks", lookback="1m", max_age="1m"),
        mid_inputs, _partition(*cases[1]), context=mid_context,
    )
    assert common(mid_narrow, 60) == common(mid_wide, 60)
    assert [value for _, value in common(mid_narrow, 60)] == [
        pytest.approx(12.0), pytest.approx(12.0),
    ]


def test_f11_and_f12_nonzero_values_follow_independent_hand_calculation() -> None:
    mid = _table([(0, 99.5), (10, 100), (20, 102)])
    tick = _table([(0, 0.25), (10, 0.25), (20, 0.25)])
    mid_output = compute_business_window(
        _feature("mid_move_ticks", lookback="10s", max_age="1s"),
        {"mid": {"table": mid}, "tick": {"table": tick}},
        _partition(-10, 21),
        context=_context(
            session_start=-10,
            authorized_start=-10,
            dependency_fields={
                "mid": {"bid_price_1", "ask_price_1"},
                "tick": {"tick_size"},
            },
        ),
    )
    assert mid_output.values["value"].to_pylist() == [
        None, pytest.approx(2.0), pytest.approx(8.0),
    ]
    assert mid_output.audit["matched_mid_time"].to_pylist()[1].isoformat() == \
        START.isoformat()

    volume = _table([(0, 1), (10, 1), (20, 1)])
    depth = _table([(0, 10), (10, 20), (15, 20), (20, 30)])
    resets = _table([(0, 0), (10, 0), (20, 0)])
    common_inputs = {
        "buy_reset": {"table": resets}, "sell_reset": {"table": resets},
        "volume": {"table": volume}, "depth": {"table": depth},
        "mid": {"table": mid}, "tick": {"table": tick},
    }
    context = _context(
        session_start=-10,
        authorized_start=-10,
        dependency_fields={
            "buy": {"BUYVOL"}, "sell": {"SELLVOL"},
            "buy_reset": {"BUYVOL"}, "sell_reset": {"SELLVOL"},
            "volume": {"TRADEVOLUME"}, "depth": OBI_FIELDS,
            "mid": {"bid_price_1", "ask_price_1"}, "tick": {"tick_size"},
        },
    )
    positive = compute_business_window(
        _feature("flow_impact_eff", lookback="10s", max_age="1s"),
        {
            **common_inputs,
            "buy": {"table": _table([(0, 0), (10, 100), (20, 120)])},
            "sell": {"table": _table([(0, 0), (10, 100), (20, 110)])},
        },
        _partition(-10, 21), context=context,
    )
    assert positive.values["value"].to_pylist()[2] == pytest.approx(20.0)

    negative = compute_business_window(
        _feature("flow_impact_eff", lookback="10s", max_age="1s"),
        {
            **common_inputs,
            "buy": {"table": _table([(0, 0), (10, 100), (20, 110)])},
            "sell": {"table": _table([(0, 0), (10, 100), (20, 120)])},
            "mid": {"table": _table([(0, 99.5), (10, 100), (20, 101)])},
        },
        _partition(-10, 21), context=context,
    )
    assert negative.values["value"].to_pylist()[2] == pytest.approx(-10.0)

    nonzero_epsilon = compute_business_window(
        _feature(
            "flow_impact_eff", lookback="10s", max_age="1s",
            parameters={"epsilon": "1"},
        ),
        {
            **common_inputs,
            "buy": {"table": _table([(0, 0), (10, 100), (20, 120)])},
            "sell": {"table": _table([(0, 0), (10, 100), (20, 110)])},
        },
        _partition(-10, 21), context=context,
    )
    assert nonzero_epsilon.values["value"].to_pylist()[2] == pytest.approx(8 / 1.4)
