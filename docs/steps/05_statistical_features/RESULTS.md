# Step 05 Results — Statistical Features & Quality

## 05-A primitive-only sampling and descriptions — 2026-11-07

**Status: COMPLETE for 05-A. The 05-B backend increment is recorded below; overall Step 05 remains PARTIAL only at the retrospective segment-summary boundary.**

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

At the 05-A boundary, catch22 and fixed-R tsfeatures were not yet implemented. The 05-B section below records that follow-on delivery. No Atlas, conditional baseline, anomaly or null model was fabricated.

Next increment after 05-A was: **05-B**.

## 05-B catch22 and fixed tsfeatures backends — 2026-11-07

**Status: COMPLETE for 05-B. Overall Step 05 remains PARTIAL pending retrospective segment-summary delivery.**

### Backend lock

- catch22: `pycatch22==0.4.5`, source tag `v0.4.5`, canonical 22 long names only.
- tsfeatures: R package `1.1.1`, CRAN source `tsfeatures_1.1.1.tar.gz` with SHA-256 `5eeb239f51552a42900c68895712c1bbcc6a5bb82b5ae33936876b28d80d0cfc`.
- R: `4.4.3`; conda-forge binary `r-tsfeatures-1.1.1-r44hc72bb7e_2.conda` with SHA-256 `b6b3dcc35059b5b3d2649cefacf308bfdf4fc5909a7ac9b9165e393a8fffc48d`.
- Runtime backend path is project-local `.r-backend/bin/Rscript`; `QUANTSAGE_RSCRIPT` may point to an equivalent locked installation. No system R is required or modified.
- Dependency locks: `tests/fixtures/statistical_backends_v1/r_environment.lock` and `python_backend.lock`.
- Recreate and verify the isolated environments with [BACKENDS.md](BACKENDS.md).

### Delivered behavior

- Registered provider `quantsage_statistical_windows@1.1.0` / implementation `statistical_windows_v3`, template `statistical_trailing@1.1.0`, and 47 metrics: 17 base descriptions, 22 catch22 metrics, and 8 fixed-R tsfeatures metrics.
- Added metric sets `basic_description@3`, `catch22_classic_22@1`, `tsfeatures_selected_8@1`, and `catch22_plus_selected_tsfeatures_30@1`.
- A selected catch22 item invokes exactly its canonical long-name function. A selected tsfeatures item invokes exactly its mapped R function; unselected backend families are not called first and filtered afterward.
- `catch22_classic_22` and the complete 22+8 set are labeled complete baselines. Explicit metric lists and other named subsets are labeled `custom_subset` in the frozen `resolved_items.outputs` mapping without changing unchanged Feature definition identities.
- Shape metrics require explicit `validity.min_samples >= 256`. Short inputs return `INSUFFICIENT_HISTORY`; constant/non-finite backend results retain an explicit reason and are never replaced with zero. ARCH uses the package's regression `R²`, crossing ties use `x<=median`, and stability/lumpiness use the locked 60-point standardised block implementation.
- The input whitelist admits ARCH only for price-change/net-flow views (`mid_move_ticks`, `active_flow_imb`, `flow_impact_eff`) and rejects unsupported primitive/business combinations at freeze time.
- Statistical outputs publish `statistical_window_audit_v1`, including metric/library/version/source, parameters, input/output units, backend warnings, uniqueness and elapsed time.

### Golden references

- Inputs: `tests/fixtures/statistical_backends_v1/inputs.json`, SHA-256 `6f7347e29ab0e04812fa6bf4662b7bb0b19fb4f73787cae8fb9930b62e1f479c`.
- Outputs: `tests/fixtures/statistical_backends_v1/golden.json`, SHA-256 `5c0548d9babbae4d1e1c4b11ece607037ac28ff55ec4c962517f0d2e480d6945`.
- Generator: `scripts/generate_statistical_goldens.py`; R adapter: `scripts/tsfeatures_backend.R`.
- Fixtures cover fixed-seed white noise, periodic data, AR(1), heteroskedastic data, ties, short, constant, and near-constant series. Numeric comparisons use `atol=1e-12`, `rtol=1e-12`; crossing and discrete lag/count metrics are exact.

### Real Run

| Object | Value |
|---|---|
| Source / split | `cffex_treasury_l2_full79@2` / frozen `research_only` split |
| Input | primitive `obi_snapshot(d0)` |
| FeatureSet / Study | `real_statistical_shape_windows@3` / `real_statistical_shape_study_v3@1` |
| Run | `run_93e384e3d2fe49cabaff1a5711cd842a` |
| Window / grid / stride / max age / minimum | 300s / 1s / 60s / 2s / 256 |
| Selected metrics | `c22.CO_f1ecac`, `tsf.spectral_entropy`, `tsf.crossing_points` |
| Work / status | 1 primitive dependency + 3 requested metrics; `COMPLETE` |

| Metric | Feature / result | Value / audit hashes | Quality |
|---|---|---|---|
| `c22.CO_f1ecac` | `feature_dc10057e02388903602411a9@1` / `result_9ff9b2196493cc071bed2beba5415b61` | value `sha256:e1bf5d952337a9febb07af853d6199b7ea5fe0924b6114dd0aa226e6c49cbff2`; audit `sha256:8e833b8235e8af27ccd1b6d461c6c31ae4b3de49a2438fbed9aa775e1840c8fd` | 26/115 valid; 89 `STALE_DATA` |
| `tsf.spectral_entropy` | `feature_df668ce874e8faebae8c094c@1` / `result_af1195cf1ef3dc934e728930996d4d4e` | value `sha256:95b6b07ba53aca15af8787759473f3a52dd30cd85ba4b7b4db85da983355c876`; audit `sha256:83ca50f4b18dd4d972f0d6044a42ea431cfb07d23c6b1331dfd8d513d805f5ca` | 26/115 valid; 89 `STALE_DATA` |
| `tsf.crossing_points` | `feature_82044aeebf3a515fe7d6125c@1` / `result_5acb044a9021a53de9de6ba3827f4331` | value `sha256:a40349f4ff68cd3378c518236b0fe532f2bceca0c6dadc4152f181cd8b6bc527`; audit `sha256:d5e8ff2edcf3a08e49eab4e9aba5985f67870f882b37c38dbfbea63243c26c7b` | 26/115 valid; 89 `STALE_DATA` |

Each statistical result has a 115-row `statistical_window_audit_v1` artifact. The primitive dependency has 12,255/12,255 valid values and remains linked to its source-warning audit. Stale parent windows are retained as NA rather than stitched from valid subsegments. A prior Run `run_f477685b127a47f7b3ea5818515a4563` failed before publication because its new audit schema lacked a validator; that failed Run remains immutable history. The successful Run above was submitted under a new request key after the validator was registered and tested.

### Verification

Commands actually run:

```bash
/data/src/research/.venv/bin/python -m scripts.generate_statistical_goldens
/data/src/research/.venv/bin/python -m pytest -q
/data/src/research/.venv/bin/python -m ruff check quantsage_discovery scripts tests examples
/data/src/research/.venv/bin/python -m examples.statistical_backends_workflow \
  --database artifacts/research/registry.duckdb \
  --socket /tmp/quantsage-1000/registry-08ab336dc96daf95.sock
```

Complete suite: 314 passed. Full-project Ruff: passed. Tests cover the exact 22-name catalog, selected-only catch22 invocation, real locked-R golden equivalence, ARCH regression `R²`, spectral entropy, crossing ties, block standardisation, explicit short/constant reasons, full-baseline versus custom-subset labels, 256-sample gating, statistical audit metadata, and future-prefix invariance through both backends.

Limits: this Run uses primitive input and selected metrics. The separate ready Step 02–05 business-ref A/B/C closure is recorded in [Step 13 RESULTS](../13_end_to_end_integration/RESULTS.md). Retrospective segment-summary mode remains pending. No Atlas, conditional baseline, anomaly or null model was fabricated.

Next increment: **E2E A/B/C**.
