# Step 05 Results — Statistical Features & Quality

## 05-A primitive-only sampling and descriptions — 2026-11-07

**Status: COMPLETE for 05-A. Step 05 remains PARTIAL pending 05-B catch22 and fixed tsfeatures.**

### Delivered behavior

- Added trusted provider `quantsage_statistical_windows@1.0.1` (`statistical_windows_v2`) and `statistical_trailing@1.0.1`.
- Added the explicit `basic_description@2` metric set: mean, sample std (`ddof=1`), median, MAD, IQR, q01/q05/q25/q75/q95/q99, min/max, zero/positive/negative/unchanged ratios.
- The frozen work scope now carries the planner's true `session_start`. The clock grid and evaluation stride are anchored to that origin, so changing a Run's output start changes only output coverage, not common evaluation times or values.
- Added a closed input-view whitelist for exact primitive IDs and business families. Freeze validation rejects unsupported inputs, mismatched family/unit metadata, and inadmissible metrics such as directional sign ratios on a non-negative spread.
- Sampling uses backward as-of input with explicit profile W, evaluation stride and `max_feature_age`. Missing/stale samples invalidate the entire parent trailing window; no drop-and-stitch or subsegment fallback is performed.
- Deterministic quantiles use linear interpolation at `(n-1)p`. Short/constant/NA cases have explicit outcomes.
- Primitive-only input is supported directly; Step 04 is not a prerequisite.

### Real Run

| Object | Value |
|---|---|
| Source | `cffex_treasury_l2_full79@2` |
| Input | primitive-only `obi_snapshot(d0)` |
| FeatureSet / Study | `real_statistical_windows@2` / `real_statistical_window_study_v2@1` |
| Run | `run_99f1309184674edb81ea13d96411b97c` |
| Window / grid / stride / max age | 10s / 1s / 5s / 2s |
| Work | 1 primitive dependency + 2 requested statistical metrics |
| Mean feature / result | `feature_476a100f397674dc60b5e522@1` / `result_c30e9c430fcc329dbf079779944f3aef` |
| Std feature / result | `feature_ff5f375e6ffd9e068e3c2383@1` / `result_69cd5b5be3d02d61f52b7a6131a1470a` |
| Mean/std result | 1,438 evaluation points each, 1,232 valid, 206 `STALE_DATA` |

Final verified refs from the corrected Run:

| Feature | Result | Value artifact hash | Audit rows / hash | Quality |
|---|---|---|---|---|
| `feature_22f963ab142071c7cae3f99f@1` (`obi_snapshot d0`) | `result_5e78d1de4629df7b7464883aca209cbf` | `sha256:a993f36d4c7057d2ec0e9283c715b333ceb5c42c12054fdabca97ac80583d841` | 12,255 / `sha256:5d26277ee608a1ffca12dec2fe5141db5a6aeb24c83f38ef4a733f9aa6714a2b` | 12,255/12,255 valid |
| `feature_476a100f397674dc60b5e522@1` (mean) | `result_c30e9c430fcc329dbf079779944f3aef` | `sha256:7b6248431f58973190e356f6304ecbf9de8a0ceba2288d2a1cdfdde27b08139b` | 1,438 / `sha256:6409c379eb7f9fac9a6fa9a362980327ace894e8372b70cc0ff7dd1f0b53f799` | 1,232/1,438 valid |
| `feature_ff5f375e6ffd9e068e3c2383@1` (std) | `result_69cd5b5be3d02d61f52b7a6131a1470a` | `sha256:4a679b0f066dde1be72d9a6c678d930b1a51cb356472371cbfd406671e3ddfb1` | 1,438 / `sha256:8f1497dc468cbf90fa5763efbe1820edfd88a04f2f1bb98981e4d4d154b89eba` | 1,232/1,438 valid |

Mean/std artifacts begin at the first complete-window evaluation point, `2026-01-05T01:30:10+00:00`, and end at `03:29:55+00:00`. Their 206 invalid rows are `STALE_DATA`; they are retained rather than dropped or stitched. The dependency primitive result has 12,255/12,255 valid rows and retains a source-warning flag.

### Verification

Commands actually run:

```bash
/data/src/research/.venv/bin/python -m pytest -q tests/test_statistical_windows.py tests/test_run_planner.py tests/test_feature_sets.py
/data/src/research/.venv/bin/python -m pytest -q
/data/src/research/.venv/bin/python -m ruff check quantsage_discovery scripts tests examples
/data/src/research/.venv/bin/python -m examples.statistical_windows_workflow \
  --database artifacts/research/registry.duckdb \
  --socket /tmp/quantsage-1000/registry-08ab336dc96daf95.sock
```

Focused regression: 45 passed. Dedicated 05-A suite: 6 passed. Complete suite: 302 passed. Full-project Ruff: passed. The dedicated tests cover true-session alignment, changed output start with identical common values, complete-window mean, stale-gap invalidation, future-prefix invariance, empty/short/constant handling, exact selected metric catalog, and unsupported input/unit/metric rejection.

Limits: 05-B catch22 0.4.5 and the fixed R tsfeatures/golden work are not implemented. This is not a complete statistical baseline. No Atlas, conditional baseline, anomaly or null model was fabricated.

Next increment: **05-B**.
