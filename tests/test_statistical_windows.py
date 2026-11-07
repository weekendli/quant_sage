"""Step 05-A fixed-grid trailing-window and basic-description tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from quantsage_discovery.core import Registry
from quantsage_discovery.research import l2_primitives
from quantsage_discovery.research.errors import ResearchValidationError
from quantsage_discovery.research.feature_schemas import normalize_duration
from quantsage_discovery.research.l2_primitives import register_l2_primitive_catalog
from quantsage_discovery.research.providers import make_feature_values_table
from quantsage_discovery.research.service import ResearchService
from quantsage_discovery.research.statistical_windows import (
    CATALOG_VERSION,
    METRICS,
    compute_statistical_window,
    register_statistical_catalog,
    validate_statistical_input,
)

START = datetime(2026, 1, 5, 1, 30, tzinfo=UTC)


def _table(values: list[float | None], *, skip_second: int | None = None):
    rows = []
    for second, value in enumerate(values):
        if second == skip_second:
            continue
        rows.append({
            "event_time": START + timedelta(seconds=second),
            "available_time": None,
            "instrument_id": "T2603",
            "trade_date": "2026-01-05",
            "session_id": "s",
            "value": value,
        })
    return make_feature_values_table(rows)


def _feature(metric: str, *, window: str = "10s", max_age: str = "2s"):
    return {
        "metric_ref": f"stat.{metric}@{CATALOG_VERSION}",
        "time_policy": {"profile_window": normalize_duration(window, "W")},
        "sampling": {
            "interval": normalize_duration("1s", "interval"),
            "evaluation_stride": normalize_duration("5s", "stride"),
            "max_feature_age": normalize_duration(max_age, "age"),
        },
    }


def _partition(end_second: int = 21):
    return {
        "instrument_id": "T2603", "trade_date": "2026-01-05", "session_id": "s",
        "start": START.isoformat(), "end": (START + timedelta(seconds=end_second)).isoformat(),
    }


def _read_scope():
    return {"session_start": START.isoformat()}


def test_trailing_mean_uses_complete_one_second_grid():
    output = compute_statistical_window(
        _feature("mean"), {"table": _table(list(range(21)))}, _partition(), _read_scope(),
    )
    assert output.values["event_time"].to_pylist() == [
        START + timedelta(seconds=10), START + timedelta(seconds=15),
        START + timedelta(seconds=20),
    ]
    assert output.values["value"].to_pylist() == [
        pytest.approx(5.5), pytest.approx(10.5), pytest.approx(15.5),
    ]
    assert output.audit["n_total"].to_pylist() == [10, 10, 10]


def test_gap_in_window_invalidates_parent_and_future_append_does_not_rewrite_prefix():
    gap = compute_statistical_window(
        _feature("mean", max_age="0.500s"),
        {"table": _table(list(range(21)), skip_second=5)},
        _partition(),
        _read_scope(),
    )
    assert gap.values["value"].to_pylist()[0] is None
    assert "STALE_DATA" in gap.invalid_reason_counts
    full = compute_statistical_window(
        _feature("mean"), {"table": _table(list(range(31)))}, _partition(31),
        _read_scope(),
    )
    prefix = compute_statistical_window(
        _feature("mean"), {"table": _table(list(range(21)))}, _partition(21),
        _read_scope(),
    )
    assert prefix.values["value"].to_pylist() == full.values["value"].to_pylist()[:3]


def test_constant_and_short_inputs_have_explicit_outcomes():
    constant = _table([5.0] * 21)
    mean = compute_statistical_window(
        _feature("mean"), {"table": constant}, _partition(),
        _read_scope(),
    )
    std = compute_statistical_window(
        _feature("std"), {"table": constant}, _partition(),
        _read_scope(),
    )
    assert mean.values["value"].to_pylist() == [5.0, 5.0, 5.0]
    assert std.values["value"].to_pylist() == [0.0, 0.0, 0.0]
    short = compute_statistical_window(
        _feature("mean"), {"table": _table(list(range(5)))}, _partition(),
        _read_scope(),
    )
    assert short.values.num_rows == 3
    assert short.values["value"].to_pylist() == [None, None, None]
    assert "STALE_DATA" in short.invalid_reason_counts


def test_metric_catalog_contains_selected_basic_descriptions_only() -> None:
    assert set(METRICS) == {
        "mean", "std", "median", "mad", "iqr", "q01", "q05", "q25", "q75",
        "q95", "q99", "min", "max", "zero_ratio", "positive_ratio",
        "negative_ratio", "unchanged_ratio",
    }


def test_changed_output_start_keeps_true_session_grid_and_common_values():
    table = {"table": _table(list(range(32)))}
    early = compute_statistical_window(
        _feature("mean"), table, _partition(31), _read_scope(),
    )
    later_partition = {
        **_partition(31),
        "start": (START + timedelta(seconds=12)).isoformat(),
    }
    later = compute_statistical_window(
        _feature("mean"), table, later_partition, _read_scope(),
    )

    expected_grid = [START + timedelta(seconds=second) for second in (10, 15, 20, 25, 30)]
    assert early.values["event_time"].to_pylist() == expected_grid
    assert later.values["event_time"].to_pylist() == expected_grid[1:]
    assert later.values["value"].to_pylist() == early.values["value"].to_pylist()[1:]
    assert later.audit["window_start"].to_pylist() == [
        START + timedelta(seconds=second) for second in (5, 10, 15, 20)
    ]


def test_whitelist_rejects_unsupported_view_unit_and_metric(tmp_path):
    registry = Registry(tmp_path / "research.duckdb")
    service = ResearchService(registry)
    try:
        register_l2_primitive_catalog(service)
        register_statistical_catalog(service)

        rejected = _freeze_feature_set(
            service,
            "unsupported_view",
            primitive_id="buy_cumulative_reset",
            metric="mean",
        )
        with pytest.raises(ResearchValidationError, match="explicit Step 05 whitelist"):
            rejected()

        non_directional = _freeze_feature_set(
            service,
            "non_directional_metric",
            primitive_id="spread_ticks",
            metric="positive_ratio",
        )
        with pytest.raises(ResearchValidationError, match="not admissible"):
            non_directional()

        with pytest.raises(ResearchValidationError, match="family or unit"):
            validate_statistical_input(
                {
                    "feature_id": "feature_mismatched",
                    "version": 1,
                    "kind": "primitive",
                    "family": "price_dynamics",
                    "unit": "ticks",
                    "source_requirements": {"primitive_id": "mid"},
                },
                [f"stat.mean@{CATALOG_VERSION}"],
            )
    finally:
        registry.close()


def _freeze_feature_set(
    service: ResearchService,
    feature_set_id: str,
    *,
    primitive_id: str,
    metric: str,
):
    parameters = {}
    if primitive_id in {"obi_snapshot", "wobi_snapshot"}:
        parameters["space"] = "d0"
    primitive_recipe = {
        "template_ref": f"l2_{primitive_id}@{l2_primitives.CATALOG_VERSION}",
        "step": 3,
        "parameters": parameters,
    }
    document = {
        "schema_version": "feature_set_v1",
        "feature_set_id": feature_set_id,
        "version": 1,
        "name": feature_set_id,
        "description": "Whitelist regression fixture.",
        "items": [
            {"item_id": "input", "alias": "input", "recipe": primitive_recipe},
            {
                "item_id": "profile",
                "alias": "profile",
                "recipe": {
                    "template_ref": f"statistical_trailing@{CATALOG_VERSION}",
                    "step": 5,
                    "parameters": {},
                    "input": {"item_id": "input"},
                    "profile_window": "10s",
                    "sampling": {
                        "axis": "clock_time",
                        "interval": "1s",
                        "evaluation_stride": "5s",
                        "max_feature_age": "2s",
                    },
                    "validity": {"min_samples": 1},
                    "metrics": [f"stat.{metric}@{CATALOG_VERSION}"],
                },
            },
        ],
        "created_by": {"kind": "agent", "id": "step05a-test"},
        "change_reason": "Exercise the Step 05-A whitelist.",
    }
    draft = service.save_feature_set(document)
    return lambda: service.freeze_feature_set(
        f"{feature_set_id}@1", expected_revision=draft["draft_revision"],
    )
