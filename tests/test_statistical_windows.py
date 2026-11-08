"""Step 05-A fixed-grid trailing-window and basic-description tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pyarrow as pa
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
    _metric_value,
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


def _gap_context(
    *,
    start_second: int,
    end_second: int | None = None,
    affected_fields: list[str] | None = None,
    event_id: str = "stat-gap-1",
) -> dict:
    at = START + timedelta(seconds=start_second)
    finish = at if end_second is None else START + timedelta(seconds=end_second)
    return {
        "session_start": START.isoformat(),
        "authorized_start": START.isoformat(),
        "dependency_fields": {
            "input": set(affected_fields or ["bid_price_1", "ask_price_1"]),
        },
        "gap_events": [{
            "event_id": event_id,
            "event_kind": "QUARANTINED_RECORD",
            "certainty": "OBSERVED",
            "exchange": "CFFEX",
            "instrument_id": "T2603",
            "trade_date": START.date(),
            "session_id": "s",
            "start_time": at,
            "end_time": finish,
            "boundary": "point" if end_second is None else "closed_open",
            "reason_codes": ["INVALID_L1_BOOK"],
            "affected_fields": affected_fields or ["bid_price_1", "ask_price_1"],
            "evidence_record_id": "quarantined-record-1",
        }],
        "gap_manifest_ref": {
            "uri": "file:///frozen/gap_manifest.json",
            "content_hash": "sha256:" + "b" * 64,
        },
        "warning_flags": {},
    }


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


def test_known_source_gap_is_distinct_from_stale_and_recovers_after_complete_window():
    output = compute_statistical_window(
        _feature("mean"),
        {"table": _table(list(range(21)))},
        _partition(),
        _read_scope(),
        gap_context=_gap_context(start_second=5),
    )
    values = output.values["value"].to_pylist()
    reasons = output.audit["invalid_reason"].to_pylist()
    assert values == [None, 10.5, 15.5]
    assert reasons == ["KNOWN_SOURCE_GAP", None, None]
    assert output.audit["source_gap_event_ids"].to_pylist()[0] == ["stat-gap-1"]
    assert output.audit["source_gap_evidence_record_ids"].to_pylist()[0] == [
        "quarantined-record-1",
    ]
    assert output.audit["gap_manifest_hash"].to_pylist()[0] == "sha256:" + "b" * 64

    stale = compute_statistical_window(
        _feature("mean", max_age="0.500s"),
        {"table": _table(list(range(21)), skip_second=5)},
        _partition(),
        _read_scope(),
    )
    assert "STALE_DATA" in stale.invalid_reason_counts
    assert "KNOWN_SOURCE_GAP" not in stale.invalid_reason_counts

    warning_context = _gap_context(start_second=5)
    warning_context["gap_events"][0]["affected_fields"] = ["TRADEVOLUME"]
    warning_context["warning_flags"] = {
        "input": ["VOLUME_INCREMENT_UNVERIFIED"],
    }
    warning_only = compute_statistical_window(
        _feature("mean"),
        {"table": _table(list(range(21)))},
        _partition(),
        _read_scope(),
        gap_context=warning_context,
    )
    assert warning_only.values["value"].to_pylist() == [
        pytest.approx(5.5), pytest.approx(10.5), pytest.approx(15.5),
    ]
    assert "VOLUME_INCREMENT_UNVERIFIED" in \
        warning_only.audit["warning_flags"].to_pylist()[0]


def test_statistical_window_inherits_serialized_parent_gap_lineage():
    values = [float(index) for index in range(21)]
    values[5] = None
    table = _table(values)
    event_times = table["event_time"].to_pylist()
    input_audit = pa.table({
        "event_time": pa.array(event_times, pa.timestamp("us", tz="UTC")),
        "invalid_reason": pa.array(
            [
                "KNOWN_SOURCE_GAP" if index == 5 else None
                for index in range(len(event_times))
            ],
            pa.string(),
        ),
        "source_gap_event_ids": pa.array(
            [[f"parent-gap-{index}"] if index == 5 else [] for index in range(len(event_times))],
            pa.list_(pa.string()),
        ),
        "source_gap_evidence_record_ids": pa.array(
            [["parent-record"] if index == 5 else [] for index in range(len(event_times))],
            pa.list_(pa.string()),
        ),
    })
    output = compute_statistical_window(
        _feature("mean"),
        {"table": table},
        _partition(),
        _read_scope(),
        input_audit=input_audit,
    )
    assert output.values["value"].to_pylist()[0] is None
    assert output.audit["invalid_reason"].to_pylist()[0] == "KNOWN_SOURCE_GAP"
    assert output.audit["source_gap_event_ids"].to_pylist()[0] == ["parent-gap-5"]
    assert output.audit["source_gap_evidence_record_ids"].to_pylist()[0] == [
        "parent-record",
    ]


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


@pytest.mark.parametrize(
    ("metric", "values", "expected"),
    [
        ("std", [1.0, 2.0, 3.0], 1.0),
        ("mean", [0.0, 1.0, 2.0, 3.0, 4.0], 2.0),
        ("median", [0.0, 1.0, 2.0, 3.0, 4.0], 2.0),
        ("mad", [0.0, 1.0, 2.0, 3.0, 4.0], 1.0),
        ("iqr", [0.0, 1.0, 2.0, 3.0, 4.0], 2.0),
        ("q01", [0.0, 1.0, 2.0, 3.0, 4.0], 0.04),
        ("q25", [0.0, 1.0, 2.0, 3.0, 4.0], 1.0),
        ("q75", [0.0, 1.0, 2.0, 3.0, 4.0], 3.0),
        ("q95", [0.0, 1.0, 2.0, 3.0, 4.0], 3.8),
        ("zero_ratio", [-1.0, 0.0, 0.0, 2.0], 0.5),
        ("positive_ratio", [-1.0, 0.0, 2.0, 3.0], 0.5),
        ("negative_ratio", [-1.0, 0.0, 2.0, 3.0], 0.25),
        ("unchanged_ratio", [1.0, 1.0, 2.0, 2.0, 2.0], 0.75),
    ],
)
def test_basic_descriptions_match_independent_hand_values(
    metric: str, values: list[float], expected: float,
) -> None:
    assert _metric_value(metric, values) == pytest.approx(expected, abs=1e-12, rel=1e-12)


def test_std_uses_sample_denominator_and_rejects_single_observation() -> None:
    assert _metric_value("std", [1.0, 2.0, 3.0]) == pytest.approx(1.0)
    assert _metric_value("std", [7.0]) is None


def _shape_feature(metric_ref: str, *, window: str = "300s"):
    return {
        "metric_ref": metric_ref,
        "time_policy": {"profile_window": normalize_duration(window, "W")},
        "sampling": {
            "interval": normalize_duration("1s", "interval"),
            "evaluation_stride": normalize_duration("60s", "stride"),
            "max_feature_age": normalize_duration("2s", "age"),
        },
        "validity": {"min_samples": 256},
    }


def test_catch22_window_preserves_complete_grid_and_future_prefix():
    metric_ref = f"c22.CO_f1ecac@{CATALOG_VERSION}"
    prefix = compute_statistical_window(
        _shape_feature(metric_ref),
        {"table": _table([float(index) for index in range(321)])},
        _partition(321),
        _read_scope(),
    )
    extended = compute_statistical_window(
        _shape_feature(metric_ref),
        {"table": _table([float(index) for index in range(421)])},
        _partition(421),
        _read_scope(),
    )
    assert prefix.values["event_time"].to_pylist() == [START + timedelta(seconds=300)]
    assert prefix.values["value"].to_pylist() == extended.values["value"].to_pylist()[:1]
    assert prefix.audit["n_total"].to_pylist() == [300]
    assert prefix.audit["n_valid"].to_pylist() == [300]
    assert prefix.audit["library_name"].to_pylist() == ["pycatch22"]
    assert prefix.audit["library_version"].to_pylist() == ["0.4.5"]
    assert prefix.audit["n_unique"].to_pylist() == [300]
    assert prefix.audit["backend_elapsed_ms"].to_pylist()[0] >= 0


def test_tsfeatures_window_uses_the_selected_fixed_r_metric():
    metric_ref = f"tsf.crossing_points@{CATALOG_VERSION}"
    values = [float(index % 2) for index in range(421)]
    prefix = compute_statistical_window(
        _shape_feature(metric_ref),
        {"table": _table(values[:361])},
        _partition(361),
        _read_scope(),
    )
    output = compute_statistical_window(
        _shape_feature(metric_ref),
        {"table": _table(values)},
        _partition(421),
        _read_scope(),
    )
    assert output.values["event_time"].to_pylist() == [
        START + timedelta(seconds=300), START + timedelta(seconds=360),
        START + timedelta(seconds=420),
    ]
    assert output.values["value"].to_pylist() == [299.0, 299.0, 299.0]
    assert prefix.values["event_time"].to_pylist() == output.values["event_time"].to_pylist()[:2]
    assert prefix.values["value"].to_pylist() == output.values["value"].to_pylist()[:2]
    assert output.audit["backend_kind"].to_pylist() == ["r_subprocess"] * 3
    assert output.audit["library_name"].to_pylist() == ["tsfeatures"] * 3
    assert output.audit["library_version"].to_pylist() == ["1.1.1"] * 3
    assert all(
        value is not None and value >= 0
        for value in output.audit["backend_elapsed_ms"].to_pylist()
    )


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


def test_raw_mid_allows_original_unit_descriptions_but_not_shape_extensions(tmp_path):
    registry = Registry(tmp_path / "research.duckdb")
    service = ResearchService(registry)
    try:
        register_l2_primitive_catalog(service)
        register_statistical_catalog(service)

        legal = _freeze_feature_set(
            service, "mid_original_unit", primitive_id="mid", metric="mean",
        )()
        assert legal["status"] == "frozen"

        forbidden = _freeze_feature_set(
            service, "mid_shape_forbidden", primitive_id="mid",
            metric="c22.CO_f1ecac",
        )
        with pytest.raises(ResearchValidationError, match="not admissible") as error:
            forbidden()
        assert error.value.details["input_view"] == "mid"
        assert "c22.CO_f1ecac" in error.value.details["unsupported_metrics"]
    finally:
        registry.close()


def test_compute_revalidates_mid_shape_policy_at_execution_boundary():
    feature = _shape_feature(f"c22.CO_f1ecac@{CATALOG_VERSION}")
    mid_input = {
        "feature_id": "feature_mid",
        "version": 1,
        "kind": "primitive",
        "family": "price_dynamics",
        "unit": "quoted_price_unit",
        "source_requirements": {"primitive_id": "mid"},
    }
    with pytest.raises(ResearchValidationError, match="not admissible"):
        compute_statistical_window(
            feature,
            {"table": _table([float(index) for index in range(321)])},
            _partition(321),
            _read_scope(),
            input_feature=mid_input,
        )


def test_raw_mid_original_unit_description_still_executes():
    feature = _feature("mean")
    mid_input = {
        "feature_id": "feature_mid",
        "version": 1,
        "kind": "primitive",
        "family": "price_dynamics",
        "unit": "quoted_price_unit",
        "source_requirements": {"primitive_id": "mid"},
    }
    output = compute_statistical_window(
        feature,
        {"table": _table([100.0 + index for index in range(21)])},
        _partition(),
        _read_scope(),
        input_feature=mid_input,
    )
    assert output.values["value"].to_pylist() == [
        pytest.approx(105.5), pytest.approx(110.5), pytest.approx(115.5),
    ]


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
                    "validity": {
                        "min_samples": (
                            256 if metric.startswith(("c22.", "tsf.")) else 1
                        ),
                    },
                    "metrics": [(
                        f"{metric}@{CATALOG_VERSION}"
                        if metric.startswith(("c22.", "tsf."))
                        else f"stat.{metric}@{CATALOG_VERSION}"
                    )],
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
