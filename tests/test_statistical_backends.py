"""Pinned Step 05-B backend and golden-reference tests."""

from __future__ import annotations

import hashlib
import json
import subprocess
from itertools import pairwise
from pathlib import Path

import numpy as np
import pytest

from quantsage_discovery.core import Registry
from quantsage_discovery.research.errors import ResearchValidationError
from quantsage_discovery.research.l2_primitives import register_l2_primitive_catalog
from quantsage_discovery.research.service import ResearchService
from quantsage_discovery.research.statistical_backends import (
    CATCH22_NAMES,
    CATCH22_VERSION,
    R_VERSION,
    TSFEATURES_METRICS,
    TSFEATURES_VERSION,
    BackendUnavailableError,
    rscript_path,
    run_catch22_metric,
    run_tsfeature_metrics,
)
from quantsage_discovery.research.statistical_windows import (
    CATALOG_VERSION as STATISTICAL_VERSION,
)
from quantsage_discovery.research.statistical_windows import (
    register_statistical_catalog,
)

FIXTURES = Path(__file__).parent / "fixtures" / "statistical_backends_v1"
CATCH22_EXPECTED = (
    "DN_HistogramMode_5",
    "DN_HistogramMode_10",
    "CO_f1ecac",
    "CO_FirstMin_ac",
    "CO_HistogramAMI_even_2_5",
    "CO_trev_1_num",
    "MD_hrv_classic_pnn40",
    "SB_BinaryStats_mean_longstretch1",
    "SB_TransitionMatrix_3ac_sumdiagcov",
    "PD_PeriodicityWang_th0_01",
    "CO_Embed2_Dist_tau_d_expfit_meandiff",
    "IN_AutoMutualInfoStats_40_gaussian_fmmi",
    "FC_LocalSimple_mean1_tauresrat",
    "DN_OutlierInclude_p_001_mdrmd",
    "DN_OutlierInclude_n_001_mdrmd",
    "SP_Summaries_welch_rect_area_5_1",
    "SB_BinaryStats_diff_longstretch0",
    "SB_MotifThree_quantile_hh",
    "SC_FluctAnal_2_rsrangefit_50_1_logi_prop_r1",
    "SC_FluctAnal_2_dfa_50_1_2_logi_prop_r1",
    "SP_Summaries_welch_rect_centroid",
    "FC_LocalSimple_mean3_stderr",
)
TSFEATURES_EXPECTED = (
    "tsf.x_acf1",
    "tsf.x_acf10",
    "tsf.x_pacf5",
    "tsf.arch_r2_l12",
    "tsf.spectral_entropy",
    "tsf.stability_w60",
    "tsf.lumpiness_w60",
    "tsf.crossing_points",
)


def _goldens() -> tuple[dict, dict]:
    return (
        json.loads((FIXTURES / "inputs.json").read_text(encoding="utf-8")),
        json.loads((FIXTURES / "golden.json").read_text(encoding="utf-8")),
    )


def _requires_r() -> None:
    try:
        rscript_path()
    except BackendUnavailableError as exc:
        pytest.skip(str(exc))


def test_catch22_catalog_is_the_exact_locked_22_long_names():
    import pycatch22

    assert CATCH22_NAMES == CATCH22_EXPECTED
    assert len(set(CATCH22_NAMES)) == 22
    assert CATCH22_VERSION == "0.4.5"
    assert all(callable(getattr(pycatch22, name, None)) for name in CATCH22_NAMES)
    assert TSFEATURES_VERSION == "1.1.1"
    assert tuple(TSFEATURES_METRICS) == TSFEATURES_EXPECTED


def test_catch22_selection_invokes_only_the_requested_function(monkeypatch):
    import pycatch22

    calls = []
    selected = "CO_f1ecac"

    def fake(values):
        calls.append((selected, len(values)))
        return 0.5

    monkeypatch.setattr(pycatch22, selected, fake)
    value, reason = run_catch22_metric(
        f"c22.{selected}", [float(index) for index in range(300)],
    )
    assert value == 0.5
    assert reason is None
    assert calls == [(selected, 300)]


def test_catch22_goldens_match_locked_backend():
    inputs, golden = _goldens()
    fixture_ids = inputs["fixture_order"]
    for metric_id in (f"c22.{name}" for name in CATCH22_NAMES):
        expected = golden["catch22"][metric_id]
        for fixture_id, expected_result in zip(fixture_ids, expected, strict=True):
            import pycatch22

            value, reason = run_catch22_metric(metric_id, inputs["fixtures"][fixture_id])
            if expected_result["value"] is None:
                assert value is None
                assert reason == expected_result["reason"]
            else:
                assert reason is None
                if metric_id in golden["tolerances"]["exact_integer_metrics"]:
                    assert value == expected_result["value"]
                else:
                    assert value == pytest.approx(
                        expected_result["value"], abs=1e-12, rel=1e-12,
                    )
                official = float(
                    getattr(pycatch22, metric_id.split(".", 1)[1])(
                        inputs["fixtures"][fixture_id],
                    )
                )
                assert value == pytest.approx(official, abs=1e-12, rel=1e-12)


def test_golden_fixture_hashes_order_and_backend_lock_are_self_consistent():
    inputs, golden = _goldens()
    canonical = json.dumps(
        inputs, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ) + "\n"
    assert "sha256:" + hashlib.sha256(canonical.encode()).hexdigest() == \
        golden["fixture_inputs_sha256"]
    assert golden["fixture_inputs_order"] == inputs["fixture_order"]
    assert set(inputs["fixture_order"]) == set(inputs["fixtures"])
    assert golden["backend_lock"]["catch22"]["version"] == CATCH22_VERSION
    assert golden["backend_lock"]["catch22"]["source_revision"] == "v0.4.5"
    assert golden["backend_lock"]["tsfeatures"]["version"] == TSFEATURES_VERSION
    assert (FIXTURES / "python_backend.lock").read_text(encoding="utf-8").splitlines() == [
        "numpy==2.4.6",
        "pycatch22==0.4.5",
    ]


def test_fixed_r_tsfeatures_goldens_match_locked_backend():
    _requires_r()
    inputs, golden = _goldens()
    fixture_ids = inputs["fixture_order"]
    for metric_id in TSFEATURES_METRICS:
        windows = [inputs["fixtures"][fixture_id] for fixture_id in fixture_ids]
        actual = run_tsfeature_metrics(metric_id, windows)
        for fixture_id, result, expected in zip(
            fixture_ids, actual, golden["tsfeatures"][metric_id], strict=True,
        ):
            value, reason = result
            if expected["value"] is None:
                assert value is None, fixture_id
                assert reason == expected["reason"], (metric_id, fixture_id)
            else:
                assert reason is None, (metric_id, fixture_id)
                if metric_id in golden["tolerances"]["exact_integer_metrics"]:
                    assert value == expected["value"], (metric_id, fixture_id)
                else:
                    assert value == pytest.approx(
                        expected["value"], abs=1e-12, rel=1e-12,
                    ), (metric_id, fixture_id)


def test_arch_r2_is_the_regression_r_squared_not_nr_squared():
    _requires_r()
    inputs, _ = _goldens()
    values = np.asarray(inputs["fixtures"]["heteroskedastic"], dtype=float)
    centred = values - values.mean()
    squared = centred ** 2
    rows = np.asarray([squared[index - 12:index + 1][::-1] for index in range(12, len(squared))])
    y = rows[:, 0]
    x = np.column_stack([np.ones(len(rows)), rows[:, 1:]])
    fitted = x @ np.linalg.lstsq(x, y, rcond=None)[0]
    expected_r2 = 1.0 - np.sum((y - fitted) ** 2) / np.sum((y - y.mean()) ** 2)

    value, reason = run_tsfeature_metrics("tsf.arch_r2_l12", [values.tolist()])[0]
    assert reason is None
    assert value == pytest.approx(expected_r2, abs=1e-10, rel=1e-10)
    assert value <= 1.0


def test_crossing_ties_are_counted_on_the_low_side():
    _requires_r()
    values = [0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 2.0, 2.0, 2.0]
    raw_value = _raw_tsfeature_reference("tsf.crossing_points", values)
    assert raw_value["status"] == "OK"
    assert raw_value["value"] == 1.0
    assert sum(value == sorted(values)[len(values) // 2] for value in values) == 3

    alternating = [float(index % 2) for index in range(300)]
    value, reason = run_tsfeature_metrics("tsf.crossing_points", [alternating])[0]
    assert reason is None
    assert value == 299


def test_crossing_ties_production_window_has_real_median_ties():
    _requires_r()
    group = [0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 2.0, 2.0, 2.0]
    values = (group * 29)[:256]
    median = sorted(values)[len(values) // 2]
    assert values.count(median) > 0
    expected_flags = [value <= median for value in values]
    expected = sum(left != right for left, right in pairwise(expected_flags))
    value, reason = run_tsfeature_metrics("tsf.crossing_points", [values])[0]
    assert reason is None
    assert value == expected


def test_stability_and_lumpiness_are_standardised_block_statistics():
    _requires_r()
    _inputs, golden = _goldens()
    base = _golden_value(golden, "tsf.stability_w60", "white_noise_seed_20261107")
    scaled = run_tsfeature_metrics("tsf.stability_w60", [[100.0 + 7.0 * value for value in
        json.loads((FIXTURES / "inputs.json").read_text())["fixtures"]["white_noise_seed_20261107"]]])
    assert scaled[0][1] is None
    assert scaled[0][0] == pytest.approx(base, abs=1e-12, rel=1e-12)

    lump_base = _golden_value(golden, "tsf.lumpiness_w60", "white_noise_seed_20261107")
    lump_scaled = run_tsfeature_metrics("tsf.lumpiness_w60", [[100.0 + 7.0 * value for value in
        json.loads((FIXTURES / "inputs.json").read_text())["fixtures"]["white_noise_seed_20261107"]]])
    assert lump_scaled[0][1] is None
    assert lump_scaled[0][0] == pytest.approx(lump_base, abs=1e-12, rel=1e-12)


def test_shape_failure_statuses_are_explicit():
    _requires_r()
    inputs, _goldens_data = _goldens()
    short = run_tsfeature_metrics("tsf.x_acf1", [inputs["fixtures"]["short"]])[0]
    constant = run_tsfeature_metrics("tsf.x_acf1", [inputs["fixtures"]["constant"]])[0]
    assert short == (None, "INSUFFICIENT_HISTORY")
    assert constant == (None, "CONSTANT_INPUT")


def test_shape_metric_sets_require_256_samples_and_label_baselines(tmp_path):
    registry = Registry(tmp_path / "research.duckdb")
    service = ResearchService(registry)
    try:
        register_l2_primitive_catalog(service)
        register_statistical_catalog(service)

        with pytest.raises(ResearchValidationError, match="min_samples >= 256"):
            _freeze_profile(
                service, "too_short", metric_set_ref="catch22_classic_22@1", min_samples=1,
            )

        frozen = _freeze_profile(
            service, "catch22_baseline", metric_set_ref="catch22_classic_22@1",
        )
        assert len(_profile_outputs(frozen)) == 22
        baseline_output = _profile_outputs(frozen)[0]
        baseline_feature = service.get_feature(baseline_output["feature_ref"])
        assert (
            baseline_output["metric_selection"]["label"]
            == "complete_catch22_baseline"
        )
        assert baseline_feature["backend"]["library_name"] == "pycatch22"

        custom = _freeze_profile(
            service, "custom_shape", metrics=[f"c22.CO_f1ecac@{STATISTICAL_VERSION}"],
        )
        custom_output = _profile_outputs(custom)[0]
        feature = service.get_feature(custom_output["feature_ref"])
        assert custom_output["metric_selection"]["label"] == "custom_subset"
        assert feature["backend"]["library_version"] == "0.4.5"
    finally:
        registry.close()


def test_arch_shape_is_rejected_for_non_directional_input(tmp_path):
    registry = Registry(tmp_path / "research.duckdb")
    service = ResearchService(registry)
    try:
        register_l2_primitive_catalog(service)
        register_statistical_catalog(service)
        with pytest.raises(ResearchValidationError, match="not admissible"):
            _freeze_profile(
                service,
                "arch_on_spread",
                primitive="spread_ticks",
                metrics=[f"tsf.arch_r2_l12@{STATISTICAL_VERSION}"],
            )
    finally:
        registry.close()


def _golden_value(golden: dict, metric_id: str, fixture_id: str) -> float:
    fixture_ids = golden["fixture_inputs_order"]
    index = fixture_ids.index(fixture_id)
    return golden["tsfeatures"][metric_id][index]["value"]


def _raw_tsfeature_reference(metric_id: str, values: list[float]) -> dict:
    request = json.dumps(
        {"metric_id": metric_id, "windows": [values]},
        separators=(",", ":"),
    )
    backend = Path(__file__).parents[1] / "scripts" / "tsfeatures_backend.R"
    completed = subprocess.run(
        [str(rscript_path()), "--vanilla", str(backend)],
        input=request,
        text=True,
        capture_output=True,
        check=False,
        timeout=180,
    )
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert R_VERSION in payload["r_version"]
    assert payload["tsfeatures_version"] == "1.1.1"
    return payload["results"][0]


def _freeze_profile(
    service: ResearchService,
    feature_set_id: str,
    *,
    primitive: str = "obi_snapshot",
    metrics: list[str] | None = None,
    metric_set_ref: str | None = None,
    min_samples: int = 256,
) -> dict:
    parameters = {"space": "d0"} if primitive == "obi_snapshot" else {}
    recipe = {
        "template_ref": f"statistical_trailing@{STATISTICAL_VERSION}",
        "step": 5,
        "parameters": {},
        "input": {"item_id": "input"},
        "profile_window": "300s",
        "sampling": {
            "axis": "clock_time",
            "interval": "1s",
            "evaluation_stride": "60s",
            "max_feature_age": "2s",
        },
        "validity": {"min_samples": min_samples},
    }
    if metric_set_ref is not None:
        recipe["metric_set_ref"] = metric_set_ref
    else:
        recipe["metrics"] = metrics
    document = {
        "schema_version": "feature_set_v1",
        "feature_set_id": feature_set_id,
        "version": 1,
        "name": feature_set_id,
        "description": "Step 05-B catalog fixture.",
        "items": [
            {
                "item_id": "input",
                "alias": "input",
                "recipe": {
                    "template_ref": f"l2_{primitive}@2.0.0",
                    "step": 3,
                    "parameters": parameters,
                },
            },
            {"item_id": "profile", "alias": "profile", "recipe": recipe},
        ],
        "created_by": {"kind": "agent", "id": "step05b-test"},
        "change_reason": "Exercise Step 05-B selection contracts.",
    }
    draft = service.save_feature_set(document)
    return service.freeze_feature_set(
        f"{feature_set_id}@1", expected_revision=draft["draft_revision"],
    )


def _profile_outputs(frozen: dict) -> list[dict]:
    return next(
        item["outputs"] for item in frozen["resolved_items"] if item["item_id"] == "profile"
    )
