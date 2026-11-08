"""Generate deterministic catch22 and fixed-R tsfeatures golden fixtures."""

from __future__ import annotations

import hashlib
import json
import math
import platform
import random
import sys
from pathlib import Path

from quantsage_discovery.research.statistical_backends import (
    CATCH22_NAMES,
    CATCH22_VERSION,
    R_VERSION,
    TSFEATURES_CONDA_BUILD,
    TSFEATURES_CONDA_SHA256,
    TSFEATURES_METRICS,
    TSFEATURES_SOURCE_SHA256,
    TSFEATURES_VERSION,
    run_catch22_metric,
    run_tsfeature_metrics,
)

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "tests" / "fixtures" / "statistical_backends_v1"


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _fixtures() -> dict[str, list[float]]:
    rng = random.Random(20261107)
    white = [rng.gauss(0.0, 1.0) for _ in range(300)]

    ar = []
    previous = 0.0
    for _ in range(300):
        previous = 0.82 * previous + rng.gauss(0.0, 1.0)
        ar.append(previous)

    heteroskedastic = [
        rng.gauss(0.0, 0.20 + 1.40 * (math.sin(index / 13.0) ** 2))
        for index in range(300)
    ]
    ties = [float(rng.choice((-1, 0, 1))) for _ in range(300)]
    return {
        "white_noise_seed_20261107": [round(value, 12) for value in white],
        "periodic_sine": [
            round(math.sin(index * 2.0 * math.pi / 24.0), 12) for index in range(300)
        ],
        "ar1_phi_082": [round(value, 12) for value in ar],
        "heteroskedastic": [round(value, 12) for value in heteroskedastic],
        "ties": ties,
        "short": [0.0, 1.0, 2.0, 3.0, 4.0],
        "constant": [2.5] * 300,
        "near_constant": [
            round(2.5 + 1e-12 * math.sin(index), 15) for index in range(300)
        ],
    }


def main() -> None:
    fixtures = _fixtures()
    windows = list(fixtures.values())
    fixture_ids = list(fixtures)

    catch22: dict[str, list[dict[str, object]]] = {}
    for name in CATCH22_NAMES:
        metric_id = f"c22.{name}"
        results = []
        for values in windows:
            value, reason = run_catch22_metric(metric_id, values)
            results.append({"value": value, "reason": reason})
        catch22[metric_id] = results

    tsfeatures: dict[str, list[dict[str, object]]] = {}
    for metric_id in TSFEATURES_METRICS:
        results = []
        for value, reason in run_tsfeature_metrics(metric_id, windows):
            results.append({"value": value, "reason": reason})
        tsfeatures[metric_id] = results

    inputs_document = {
        "schema_version": "statistical_backend_inputs_v1",
        "fixture_order": fixture_ids,
        "fixtures": fixtures,
    }
    input_bytes = (_canonical(inputs_document) + "\n").encode()
    golden_document = {
        "schema_version": "statistical_backend_golden_v1",
        "fixture_inputs_sha256": "sha256:" + hashlib.sha256(input_bytes).hexdigest(),
        "fixture_inputs_order": fixture_ids,
        "generation_script": "scripts/generate_statistical_goldens.py",
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "backend_lock": {
            "catch22": {
                "library": "pycatch22",
                "version": CATCH22_VERSION,
                "source_revision": "v0.4.5",
            },
            "tsfeatures": {
                "library": "tsfeatures",
                "version": TSFEATURES_VERSION,
                "source_sha256": TSFEATURES_SOURCE_SHA256,
                "r_version": R_VERSION,
                "conda_build": TSFEATURES_CONDA_BUILD,
                "conda_binary_sha256": TSFEATURES_CONDA_SHA256,
            },
        },
        "tolerances": {
            "default": {"atol": 1e-12, "rtol": 1e-12},
            "exact_integer_metrics": [
                "c22.CO_FirstMin_ac",
                "c22.PD_PeriodicityWang_th0_01",
                "tsf.crossing_points",
            ],
        },
        "catch22": catch22,
        "tsfeatures": tsfeatures,
    }

    OUTPUT.mkdir(parents=True, exist_ok=True)
    (OUTPUT / "inputs.json").write_text(_canonical(inputs_document) + "\n", encoding="utf-8")
    (OUTPUT / "golden.json").write_text(
        json.dumps(golden_document, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "output": str(OUTPUT),
        "catch22_metrics": len(catch22),
        "tsfeatures_metrics": len(tsfeatures),
        "fixtures": len(fixtures),
    }, sort_keys=True))


if __name__ == "__main__":
    main()
