"""Step 04-A catalog, window ordering, endpoint and reset tests."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from quantsage_discovery.research.business_windows import (
    CATALOG_VERSION,
    business_catalog_entries,
    compute_business_window,
)
from quantsage_discovery.research.feature_schemas import normalize_duration
from quantsage_discovery.research.providers import make_feature_values_table

START = datetime(2026, 1, 5, 1, 30, tzinfo=UTC)


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
    }


def test_catalog_counts_and_pending_semantics_are_exact() -> None:
    entries = business_catalog_entries()
    assert len(entries) == 432
    assert Counter(entry["family"] for entry in entries) == {
        "F01": 8, "F02": 40, "F03": 40, "F04": 120, "F05": 120,
        "F06": 24, "F07": 24, "F08": 24, "F09": 8, "F10": 8,
        "F11": 8, "F12": 8,
    }
    assert sum(entry["status"] == "pending_semantics" for entry in entries) == 48
    assert len({entry["factor_name"] for entry in entries}) == 432


def test_snapshot_mean_is_mean_of_ratios_not_aggregate_ratio() -> None:
    values = _table([(0, 0.8), (10, 0.0), (31, 0.6)])
    output = compute_business_window(
        _feature("obi", parameters={"space": "d0"}),
        {"input": {"table": values}},
        _partition(),
    )
    assert output.values["value"].to_pylist()[1] == pytest.approx(0.4)
    assert output.values["value"].to_pylist()[2] == pytest.approx(0.3)
    assert output.audit["n_total"].to_pylist()[1] == 2


def test_future_append_does_not_change_existing_prefix() -> None:
    prefix = _table([(0, 0.8), (10, 0.0), (20, 0.4)])
    full = _table([(0, 0.8), (10, 0.0), (20, 0.4), (40, -1.0)])
    feature = _feature("obi", lookback="15s", parameters={"space": "d0"})
    left = compute_business_window(feature, {"input": {"table": prefix}}, _partition())
    right = compute_business_window(feature, {"input": {"table": full}}, _partition())
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
    )
    assert output.values["value"].to_pylist()[2] == pytest.approx(1 / 3)
    assert output.audit["matched_buy_time"].to_pylist()[2].isoformat() == \
        (START + timedelta(seconds=10)).isoformat()

    reset_at_middle = _table([(0, 0), (15, 1), (20, 0)])
    inputs["buy_reset"] = {"table": reset_at_middle}
    invalid = compute_business_window(
        _feature("active_flow_imb", lookback="10s", max_age="1s"),
        inputs, _partition(),
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
    )
    assert strict.audit["invalid_reason"].to_pylist()[-1] == "ENDPOINT_AGE_EXCEEDED"


def test_volume_sum_divided_by_mean_depth_and_zero_net_flow() -> None:
    volume = _table([(0, 2), (10, 3), (20, 5)])
    depth = _table([(0, 10), (10, 20), (20, 30)])
    output = compute_business_window(
        _feature("volume_to_depth", lookback="10s"),
        {"volume": {"table": volume}, "depth": {"table": depth}},
        _partition(),
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
    )
    assert result.values["value"].to_pylist()[2] is None
    assert result.audit["invalid_reason"].to_pylist()[2] == "ZERO_DENOMINATOR"
