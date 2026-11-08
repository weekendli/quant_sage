"""Run existing tests against process-local deliberate defects; never edits source.

Usage from repository root:
    PYTHONPATH=. python docs/acceptance/2026-11-08/audit_test_strength.py --case std_ddof

An exit code of zero for a mutation means the selected tests did NOT detect it.
Results describe selected tests only, not a whole-suite mutation score.
"""

from __future__ import annotations

import argparse
import json
import statistics
from dataclasses import replace
from itertools import pairwise

import pytest

from quantsage_discovery.research import (
    business_windows,
    catalog,
    planner,
    statistical_backends,
    statistical_windows,
)

CASES = {
    "baseline_business": ["tests/test_business_windows.py"],
    "baseline_statistics": ["tests/test_statistical_windows.py"],
    "missing_f11": [
        "tests/test_business_windows.py::test_f11_and_f12_nonzero_values_follow_independent_hand_calculation",
    ],
    "wrong_f12_finite": [
        "tests/test_business_windows.py::test_f11_and_f12_nonzero_values_follow_independent_hand_calculation",
    ],
    "std_ddof": ["tests/test_statistical_windows.py"],
    "metadata_ignored": [
        "tests/test_l2_primitives.py::test_source_identity_changes_for_each_relevant_single_factor",
    ],
    "duplicate_catch22_names": [
        "tests/test_statistical_backends.py::test_catch22_catalog_is_the_exact_locked_22_long_names",
    ],
    "wrong_crossing_ties": [
        "tests/test_statistical_backends.py::test_crossing_ties_production_window_has_real_median_ties",
    ],
    "numerical_suite": ["tests"],
    "mid_shape_freeze": [
        "tests/test_statistical_windows.py::test_raw_mid_allows_original_unit_descriptions_but_not_shape_extensions",
    ],
    "gap_propagation_removed": [
        "tests/test_business_windows.py::test_known_source_gap_uses_evidence_and_recovers_after_complete_lookback",
        "tests/test_statistical_windows.py::test_known_source_gap_is_distinct_from_stale_and_recovers_after_complete_window",
        "tests/test_acceptance_remediation.py",
    ],
    "output_start_as_history": [
        "tests/test_business_windows.py::test_output_start_does_not_replace_authorized_session_history",
        "tests/test_business_windows.py::test_flow_volume_depth_and_mid_windows_are_invariant_to_output_start",
    ],
    "wrong_space_prefix": [
        "tests/test_business_windows.py::test_catalog_names_counts_and_pending_semantics_are_exact",
    ],
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=CASES, required=True)
    args = parser.parse_args()
    observations = {"target_calls": 0}

    if args.case in {"missing_f11", "numerical_suite"}:
        def missing(*_args, **_kwargs):
            observations["target_calls"] += 1
            observations["f11_calls"] = observations.get("f11_calls", 0) + 1
            raise RuntimeError("intentional audit defect: F11 removed")
        business_windows._mid_move_window = missing

    if args.case in {"wrong_f12_finite", "numerical_suite"}:
        original = business_windows._flow_windows

        def wrong_flow(family, *other_args, **kwargs):
            output = original(family, *other_args, **kwargs)
            if family == "flow_impact_eff":
                import pyarrow as pa

                observations["target_calls"] += 1
                values = output.values["value"].to_pylist()
                observations["finite_values_exercised"] = sum(v is not None for v in values)
                table = output.values.set_column(
                    output.values.schema.get_field_index("value"),
                    output.values.schema.field("value"),
                    pa.array([None if v is None else 0.0 for v in values], pa.float64()),
                )
                return replace(output, values=table)
            return output
        business_windows._flow_windows = wrong_flow

    if args.case in {"gap_propagation_removed", "numerical_suite"}:
        def missing_business_gaps(*_args, **_kwargs):
            observations["target_calls"] += 1
            return ()

        def missing_sample_gaps(*_args, **_kwargs):
            observations["target_calls"] += 1
            return ()

        business_windows._window_gaps = missing_business_gaps
        statistical_windows._sample_gaps = missing_sample_gaps

    if args.case in {"output_start_as_history", "numerical_suite"}:
        original_rolling = business_windows._rolling_mean

        def wrong_history(feature, data, partition, lookback, context, roles):
            observations["target_calls"] += 1
            output_start = business_windows._as_utc(partition["start"])
            wrong_context = replace(
                context,
                authorized_start=max(context.authorized_start, output_start),
            )
            return original_rolling(
                feature, data, partition, lookback, wrong_context, roles,
            )

        business_windows._rolling_mean = wrong_history

    if args.case in {"std_ddof", "numerical_suite"}:
        original_metric = statistical_windows._metric_value

        def wrong_std(metric, values):
            if metric == "std":
                observations["target_calls"] += 1
                observations["std_calls"] = observations.get("std_calls", 0) + 1
                return statistics.pstdev(values)
            return original_metric(metric, values)
        statistical_windows._metric_value = wrong_std

    elif args.case == "metadata_ignored":
        original_identity = planner._source_identity_hash

        def missing_metadata(source):
            observations["target_calls"] += 1
            return original_identity({k: v for k, v in source.items() if k != "market_metadata_ref"})
        planner._source_identity_hash = missing_metadata

    elif args.case == "duplicate_catch22_names":
        statistical_backends.CATCH22_NAMES = (statistical_backends.CATCH22_NAMES[0],) * 22
        observations["unique_names"] = len(set(statistical_backends.CATCH22_NAMES))

    elif args.case == "wrong_crossing_ties":
        def wrong_crossing(metric, windows):
            assert metric == "tsf.crossing_points"
            observations["target_calls"] += 1
            observations["median_ties"] = sum(
                value == statistics.median(window) for window in windows for value in window
            )
            return [
                (float(sum(a != b for a, b in pairwise(flags))), None)
                for flags in [
                    [value >= statistics.median(window) for value in window]
                    for window in windows
                ]
            ]
        statistical_backends.run_tsfeature_metrics = wrong_crossing
        from tests import test_statistical_backends

        test_statistical_backends.run_tsfeature_metrics = wrong_crossing

    elif args.case == "mid_shape_freeze":
        observations["target_calls"] += 1
        catalog.validate_statistical_input = lambda *_args, **_kwargs: None

    elif args.case == "wrong_space_prefix":
        original_entry = business_windows._entry

        def wrong_entry(family_id, family, parameters, lookback, pending):
            observations["target_calls"] += 1
            item = original_entry(family_id, family, parameters, lookback, pending)
            item["factor_name"] = item["factor_name"].replace("__d", "__s", 1)
            return item

        business_windows._entry = wrong_entry

    result = pytest.main(["-q", "-p", "no:cacheprovider", *CASES[args.case]])
    print("AUDIT_RESULT=" + json.dumps({
        "case": args.case, "selected_tests": CASES[args.case],
        "pytest_exit_code": int(result), "observations": observations,
    }, sort_keys=True))
    return int(result)


if __name__ == "__main__":
    raise SystemExit(main())
