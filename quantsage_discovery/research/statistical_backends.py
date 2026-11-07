"""Pinned catch22 and R tsfeatures execution backends for Step 05-B."""

from __future__ import annotations

import json
import math
import os
import subprocess
import time
import warnings as warnings_module
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CATCH22_VERSION = "0.4.5"
CATCH22_SOURCE_REVISION = "v0.4.5"
TSFEATURES_VERSION = "1.1.1"
R_VERSION = "4.4.3"
TSFEATURES_SOURCE_SHA256 = (
    "5eeb239f51552a42900c68895712c1bbcc6a5bb82b5ae33936876b28d80d0cfc"
)
TSFEATURES_CONDA_BUILD = "r44hc72bb7e_2"
TSFEATURES_CONDA_SHA256 = (
    "b6b3dcc35059b5b3d2649cefacf308bfdf4fc5909a7ac9b9165e393a8fffc48d"
)
TSFEATURES_POLICY_VERSION = "tsfeatures_selected_8@1"
SHAPE_MIN_SAMPLES = 256

CATCH22_NAMES = (
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

_CATCH22_UNITS = {
    "DN_HistogramMode_5": "source_unit",
    "DN_HistogramMode_10": "source_unit",
    "CO_f1ecac": "sample_lag",
    "CO_FirstMin_ac": "sample_lag",
    "CO_HistogramAMI_even_2_5": "dimensionless",
    "CO_trev_1_num": "dimensionless",
    "MD_hrv_classic_pnn40": "dimensionless",
    "SB_BinaryStats_mean_longstretch1": "sample_count",
    "SB_TransitionMatrix_3ac_sumdiagcov": "dimensionless",
    "PD_PeriodicityWang_th0_01": "dimensionless",
    "CO_Embed2_Dist_tau_d_expfit_meandiff": "dimensionless",
    "IN_AutoMutualInfoStats_40_gaussian_fmmi": "sample_lag",
    "FC_LocalSimple_mean1_tauresrat": "dimensionless",
    "DN_OutlierInclude_p_001_mdrmd": "sample_position",
    "DN_OutlierInclude_n_001_mdrmd": "sample_position",
    "SP_Summaries_welch_rect_area_5_1": "dimensionless",
    "SB_BinaryStats_diff_longstretch0": "sample_count",
    "SB_MotifThree_quantile_hh": "dimensionless",
    "SC_FluctAnal_2_rsrangefit_50_1_logi_prop_r1": "dimensionless",
    "SC_FluctAnal_2_dfa_50_1_2_logi_prop_r1": "dimensionless",
    "SP_Summaries_welch_rect_centroid": "cycles_per_sample",
    "FC_LocalSimple_mean3_stderr": "source_unit",
}

TSFEATURES_METRICS = {
    "tsf.x_acf1": {
        "name": "First autocorrelation",
        "library_metric_name": "acf_features$x_acf1",
        "unit": "dimensionless",
        "parameters": {"lag": 1},
    },
    "tsf.x_acf10": {
        "name": "Squared autocorrelation sum through lag 10",
        "library_metric_name": "acf_features$x_acf10",
        "unit": "dimensionless",
        "parameters": {"lags": [1, 10]},
    },
    "tsf.x_pacf5": {
        "name": "Squared partial autocorrelation sum through lag 5",
        "library_metric_name": "pacf_features$x_pacf5",
        "unit": "dimensionless",
        "parameters": {"lags": [1, 5]},
    },
    "tsf.arch_r2_l12": {
        "name": "ARCH LM regression R squared",
        "library_metric_name": "arch_stat$ARCH.LM",
        "unit": "dimensionless",
        "parameters": {"lags": 12, "demean": True},
    },
    "tsf.spectral_entropy": {
        "name": "AR Burg spectral entropy",
        "library_metric_name": "entropy$entropy",
        "unit": "dimensionless",
        "parameters": {
            "method": "burg",
            "prior_weight": 0.001,
            "frequency_count": "ceiling(n/2+1)",
        },
    },
    "tsf.stability_w60": {
        "name": "Variance of standardised 60-point block means",
        "library_metric_name": "stability$stability",
        "unit": "standardised_variance",
        "parameters": {"width": 60, "minimum_full_blocks": 4},
    },
    "tsf.lumpiness_w60": {
        "name": "Variance of standardised 60-point block variances",
        "library_metric_name": "lumpiness$lumpiness",
        "unit": "standardised_variance",
        "parameters": {"width": 60, "minimum_full_blocks": 4},
    },
    "tsf.crossing_points": {
        "name": "Median crossing count",
        "library_metric_name": "crossing_points$crossing_points",
        "unit": "sample_count",
        "parameters": {"ties_side": "low", "comparison": "x<=median"},
    },
}


class BackendUnavailableError(RuntimeError):
    """The pinned reference backend is not installed or cannot start."""


@dataclass(frozen=True)
class BackendMetricResult:
    value: float | None
    reason: str | None
    warnings: tuple[str, ...] = ()
    elapsed_ms: float = 0.0


def catch22_metric_document(metric_id: str) -> dict[str, Any]:
    name = metric_id.removeprefix("c22.")
    if name not in CATCH22_NAMES:
        raise ValueError(f"unknown catch22 metric: {metric_id}")
    return {
        "metric_id": metric_id,
        "name": name,
        "library_name": "pycatch22",
        "library_metric_name": name,
        "library_version": CATCH22_VERSION,
        "source_revision": CATCH22_SOURCE_REVISION,
        "unit": _CATCH22_UNITS[name],
        "parameters": {},
        "output_unit": _CATCH22_UNITS[name],
    }


def tsfeature_metric_document(metric_id: str) -> dict[str, Any]:
    if metric_id not in TSFEATURES_METRICS:
        raise ValueError(f"unknown tsfeatures metric: {metric_id}")
    spec = TSFEATURES_METRICS[metric_id]
    return {
        "metric_id": metric_id,
        "name": spec["name"],
        "library_name": "tsfeatures",
        "library_metric_name": spec["library_metric_name"],
        "library_version": TSFEATURES_VERSION,
        "source_revision": (
            f"CRAN tsfeatures_{TSFEATURES_VERSION}.tar.gz sha256:{TSFEATURES_SOURCE_SHA256}"
        ),
        "unit": spec["unit"],
        "parameters": dict(spec["parameters"]),
        "output_unit": spec["unit"],
    }


def metric_backend_document(metric_id: str) -> dict[str, Any]:
    metric_id = metric_id.split("@", 1)[0]
    if metric_id.startswith("c22."):
        return catch22_metric_document(metric_id)
    if metric_id.startswith("tsf."):
        document = tsfeature_metric_document(metric_id)
        document["backend"] = {
            "kind": "r_subprocess",
            "r_version": R_VERSION,
            "package_build": TSFEATURES_CONDA_BUILD,
            "package_binary_sha256": TSFEATURES_CONDA_SHA256,
            "policy_version": TSFEATURES_POLICY_VERSION,
        }
        return document
    raise ValueError(f"metric has no shape backend: {metric_id}")


def run_catch22_metric(metric_id: str, values: list[float]) -> tuple[float | None, str | None]:
    """Invoke exactly one canonical pycatch22 long-name function."""
    result = run_catch22_metric_detailed(metric_id, values)
    return result.value, result.reason


def run_catch22_metric_detailed(metric_id: str, values: list[float]) -> BackendMetricResult:
    """Invoke one canonical pycatch22 function and retain backend evidence."""
    name = metric_id.removeprefix("c22.")
    if name not in CATCH22_NAMES:
        raise ValueError(f"unknown catch22 metric: {metric_id}")
    if len(values) < SHAPE_MIN_SAMPLES:
        return BackendMetricResult(None, "INSUFFICIENT_HISTORY")
    try:
        import pycatch22
    except ImportError as exc:
        raise BackendUnavailableError("pycatch22==0.4.5 is not installed") from exc
    function = getattr(pycatch22, name, None)
    if not callable(function):
        raise BackendUnavailableError(
            f"pycatch22==0.4.5 does not expose canonical metric {name}"
        )
    started = time.perf_counter()
    try:
        with warnings_module.catch_warnings(record=True) as captured:
            warnings_module.simplefilter("always")
            value = float(function(list(values)))
    except Exception as exc:  # noqa: BLE001 - backend failures become explicit metric NA
        return BackendMetricResult(
            None,
            _failure_reason(values),
            (f"{type(exc).__name__}: {exc}",),
            (time.perf_counter() - started) * 1_000,
        )
    elapsed_ms = (time.perf_counter() - started) * 1_000
    backend_warnings = tuple(str(item.message) for item in captured)
    if math.isfinite(value):
        return BackendMetricResult(value, None, backend_warnings, elapsed_ms)
    return BackendMetricResult(
        None, _failure_reason(values), backend_warnings, elapsed_ms,
    )


def run_tsfeature_metrics(
    metric_id: str,
    windows: list[list[float]],
) -> list[tuple[float | None, str | None]]:
    """Run one selected tsfeatures metric for a batch of completed windows."""
    results = run_tsfeature_metrics_detailed(metric_id, windows)
    return [(result.value, result.reason) for result in results]


def run_tsfeature_metrics_detailed(
    metric_id: str,
    windows: list[list[float]],
) -> list[BackendMetricResult]:
    """Run one selected R metric and retain warnings and per-window timing."""
    if metric_id not in TSFEATURES_METRICS:
        raise ValueError(f"unknown tsfeatures metric: {metric_id}")
    if not windows:
        return []
    output: list[BackendMetricResult] = [
        BackendMetricResult(None, "INSUFFICIENT_HISTORY")
        if len(values) < SHAPE_MIN_SAMPLES else BackendMetricResult(None, None)
        for values in windows
    ]
    runnable = [index for index, values in enumerate(windows)
                if len(values) >= SHAPE_MIN_SAMPLES]
    if not runnable:
        return output
    rscript = rscript_path()
    request = json.dumps(
        {"metric_id": metric_id, "windows": [windows[index] for index in runnable]},
        separators=(",", ":"),
    )
    script = Path(__file__).resolve().parents[2] / "scripts" / "tsfeatures_backend.R"
    try:
        completed = subprocess.run(
            [str(rscript), "--vanilla", str(script)],
            input=request,
            text=True,
            capture_output=True,
            check=False,
            timeout=180,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise BackendUnavailableError(f"cannot execute pinned R backend: {exc}") from exc
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "unknown R failure"
        raise BackendUnavailableError(f"pinned R backend failed: {detail}")
    try:
        payload = json.loads(completed.stdout)
        if R_VERSION not in str(payload.get("r_version", "")):
            raise BackendUnavailableError(
                f"R backend version mismatch: expected {R_VERSION}, "
                f"found {payload.get('r_version')}"
            )
        if str(payload.get("tsfeatures_version")) != TSFEATURES_VERSION:
            raise BackendUnavailableError(
                "tsfeatures backend version mismatch: expected "
                f"{TSFEATURES_VERSION}, found {payload.get('tsfeatures_version')}"
            )
        results = payload["results"]
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise BackendUnavailableError("pinned R backend returned malformed JSON") from exc
    if not isinstance(results, list) or len(results) != len(runnable):
        raise BackendUnavailableError("pinned R backend returned the wrong result count")
    for index, result in zip(runnable, results, strict=True):
        values = windows[index]
        status = result.get("status") if isinstance(result, dict) else None
        raw_value = result.get("value") if isinstance(result, dict) else None
        raw_warnings = result.get("warnings") if isinstance(result, dict) else None
        backend_warnings = tuple(str(item) for item in raw_warnings or [])
        raw_elapsed = result.get("elapsed_ms") if isinstance(result, dict) else None
        elapsed_ms = float(raw_elapsed) if isinstance(raw_elapsed, (int, float)) else 0.0
        if status == "OK" and isinstance(raw_value, (int, float)):
            value = float(raw_value)
            if math.isfinite(value):
                output[index] = BackendMetricResult(
                    value, None, backend_warnings, elapsed_ms,
                )
                continue
        error = result.get("error") if isinstance(result, dict) else None
        if error:
            backend_warnings += (str(error),)
        output[index] = BackendMetricResult(
            None, _failure_reason(values), backend_warnings, elapsed_ms,
        )
    return output


def rscript_path() -> Path:
    configured = os.environ.get("QUANTSAGE_RSCRIPT")
    path = Path(configured) if configured else (
        Path(__file__).resolve().parents[2] / ".r-backend" / "bin" / "Rscript"
    )
    if not path.is_file():
        raise BackendUnavailableError(
            "tsfeatures requires the pinned project-local R backend; set QUANTSAGE_RSCRIPT"
        )
    return path


def _failure_reason(values: list[float]) -> str:
    if len(values) < SHAPE_MIN_SAMPLES:
        return "INSUFFICIENT_HISTORY"
    if values and min(values) == max(values):
        return "CONSTANT_INPUT"
    return "NUMERICAL_FAILURE"
