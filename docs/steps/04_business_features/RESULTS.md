# Step 04 Results — Business Features & Time Windows

## 04-A ready business-window subset — 2026-11-07

**Status: COMPLETE for the ready subset. F07/F08 aggregation remains pending, so complete 432-item numerical delivery is not claimed.**

### Delivered behavior

- Added trusted provider `quantsage_business_windows@1.0.0` / `business_windows_v1`.
- Registered all 432 legal names with family counts `8,40,40,120,120,24,24,24,8,8,8,8`; F07/F08 contribute exactly 48 `pending_semantics` definitions.
- Added role-aware multi-input FeatureSet recipes, unit checking and explicit dependency bindings.
- Implemented valid snapshot means, `(t-L,t]` interval sums, backward as-of endpoints at `<= t-L`, endpoint-age limits, reset propagation, safe ratios and `business_window_audit_v1`.
- F02/F03/F05 compute mean-of-ratios; F10 computes summed native interval volume divided by mean depth; F09/F12 compute endpoint deltas before ratios. No horizon is expanded into a primitive, no future label is used.

### Actual Run

| Object | Value |
|---|---|
| Source | `cffex_treasury_l2_full79@2` |
| Study / FeatureSet | `real_business_window_study@1` / `real_business_windows@1` |
| Run | `run_0c071b03b41e4ebf8694168ec8d1ce0a` |
| Selected ready business items | 6 |
| Work | 18 requested + 30 dependency-only = 48; all succeeded |
| Results | 18 business values + 18 window audits; dependency results also published |
| Rows / valid | 388,384 result rows across dependencies; 384,752 valid values |
| NA reasons | `HISTORY_INSUFFICIENT=18`, `ENDPOINT_AGE_EXCEEDED=2` |

Selected Feature refs:

| Item | Feature |
|---|---|
| `obi_30s` | `feature_04b4251e890a47f6baaecc4f@1` |
| `wobi_1m` | `feature_290c200df2c3a99486330eb5@1` |
| `coverage_5m` | `feature_54ad5e9dd6d5238f3a2f3d7b@1` |
| `capacity_10m` | `feature_c21f1dc5821b21c8b0638570@1` |
| `active_flow_30s` | `feature_d764367864ccf281d6b0b3a8@1` |
| `volume_depth_30s` | `feature_7b6e4634fa333c312a52951a@1` |

The first rows are NA until the full same-session window exists; later rows recover without borrowing the prior session. `active_flow_30s` uses a frozen 5-second maximum endpoint age. F07/F08 were not selected and no aggregation was invented for them.

### Verification

The dedicated 04-A suite verifies the exact 432 catalog and 48 pending count, mean-of-ratios, future-prefix invariance, equal-time `<=t-L` endpoint selection, endpoint-age rejection, mid-window reset invalidation, session-local windows and zero net-flow NA. Focused regression passed 72 tests; the complete suite passed 296 tests; full-project Ruff passed.

Commands:

```bash
/data/src/research/.venv/bin/python -m pytest -q \
  tests/test_business_windows.py tests/test_feature_sets.py tests/test_run_planner.py \
  tests/test_research_execution.py tests/test_research_queries.py
/data/src/research/.venv/bin/python -m pytest -q
/data/src/research/.venv/bin/ruff check quantsage_discovery scripts tests examples
```

Limits: tick-dependent F01/F06/F11/F12 and all d1+ variants remain unavailable without user metadata; F07/F08 horizon aggregation stays pending; only the selected ready subset has real numerical results.

Next increment: **05-A sampling / complete statistical windows / base descriptions**.

## 04-A remediation: gap propagation, history bounds and canonical names — 2026-11-08

**Status: COMPLETE for the remediated ready subset. F07/F08 aggregation remains pending.**

- Published `quantsage_business_windows@2.0.0` / `business_windows_v2` with `business_window_audit_v2`.
- Known source gaps are matched by affected field, exact contract/trade date/session and frozen boundary semantics. Affected business windows publish NA with `KNOWN_SOURCE_GAP`, event/evidence lineage and gap manifest hash; normal/no-update, stale, formula-unavailable and warning-only states stay distinct.
- Output scope now only filters publication. True session start, authorized history and required L/W are explicit; F02/F09/F10/F11 common-time results no longer depend on output start. Rolling sums use window-local summation to avoid cross-range floating cancellation.
- The 432-item catalog uses canonical `d0/d1/d2/d3/d5` names; v1 `s-space` names have a separate one-to-one historical alias map and are not emitted by the new catalog.
- Real F1/F2 evidence: wide Run `run_635ace3d11df4da0aad6e276e8dccb01`, narrow `run_60eb1d5c9d2a46248cec398514485bfa`, and statistical downstream `run_d3fd495cf0ba42f7931e9e771621499d`. Full suite: 344 passed; targeted: 144 passed; Ruff passed.

See [remediation verification](../../acceptance/2026-11-08/REMEDIATION_VERIFICATION.md).
