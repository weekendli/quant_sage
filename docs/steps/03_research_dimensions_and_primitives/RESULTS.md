# Step 03 Results — Research Dimensions & Primitives

## 03-A basic primitives over the real frozen source — 2026-11-07

**Status: COMPLETE for 03-A. The complete Step 03 remains PARTIAL; 03-B owns the remaining 50-name F01–F08 snapshot catalog and terminal dependency set.**

### Delivered implementation

- Registered trusted provider `quantsage_l2_snapshot@1.0.2`, implementation `l2_snapshot_primitives_v3`.
- Added seven selectable Step 03-A templates: `mid`, `spread_ticks`, `depth_bid_1`, `depth_ask_1`, `depth_imbalance_1`, `microprice_proxy` and `quote_interarrival`. The real Run selects the six that do not require tick metadata.
- Extended planning to bind source metadata/policy refs and declared source fields into the frozen source identity. Missing required source fields now produce `SOURCE_FIELD_MISSING`; a non-L2 source produces `SOURCE_FORMAT_UNSUPPORTED`.
- Extended execution so a trusted domain provider can publish the exact `feature_values_v1` primary table plus a row-level `l2_primitive_audit_v1` artifact in one durable receipt/transaction. Cached health and query APIs verify both artifacts while result reads expose only the primary value columns.
- Kept each primitive's validity mask independent. L1-dependent primitives use only their declared L1 inputs; `quote_interarrival` uses session-local source order; zero denominators, unknown tick, invalid L1, and first-observation interarrival remain explicit NA reasons.

### Actual catalog and execution

| Object | Value |
|---|---|
| Source | `cffex_treasury_l2_full79@2` |
| Source content hash | `sha256:73acc246fc198e1a32f3184a445e075af347954cba5f98769e8322e51299a949` |
| Study | `real_l2_primitive_study_v3@1` |
| FeatureSet | `real_l2_primitives@3` |
| Run | `run_187b8caa8d934c8591b729a1ca03f3a3` |
| Scope | CFFEX `T2603`, trade date `2026-01-05`, opening auction + morning + afternoon |
| Selected work/result count | 18 / 18, all requested, no dependency-only work |
| Artifact roles | 18 `feature_values_v1` + 18 `l2_primitive_audit_v1` |
| Result rows / valid values | 145,644 result rows (24,274 per selected primitive); 145,641 valid values |

Feature mapping:

| Item | Feature ref |
|---|---|
| `mid` | `feature_de2b6d9318c64e171db579ac@1` |
| `depth_bid_1` | `feature_81da92d8fae61c915c3c31a4@1` |
| `depth_ask_1` | `feature_094e3d900f172b0da0e4cea0@1` |
| `depth_imbalance_1` | `feature_c5dcb5129da1bb9c8cfa2e57@1` |
| `microprice_proxy` | `feature_d117982f970a20922391b9a2@1` |
| `quote_interarrival` | `feature_887432f4e0344d10149bd7d5@1` |

Each of the six features has one result per session. Five cross-sectional primitives have 24,274/24,274 valid rows. `quote_interarrival` has 24,271/24,274 valid rows: one first-row NA in each continuous session and an all-NA opening-auction partition containing only one quote. The session-specific first-row behavior is intentional; no prior-session interval was borrowed.

The real `mid` result for the morning session is `result_cdb703a1f5201531d9dae27c49ac75a2`, definition hash `sha256:de2b6d9318c64e171db579ac246f4d3bc86ac34dced062b2824c5aeaf333bbda`. Its first values are `107.915`, `107.9125`, `107.9075`. Value artifact hash is `sha256:f565dda602440d5b03a026445c6cc1d391102e217527df46dfa2aa9620a0f6f7`; audit artifact hash is `sha256:0481f584ecffba3e4c30b0e5e6dddf89dabbcf2275fb4649342ba5cfef728850`.

The source has no user-supplied tick metadata. Therefore `spread_ticks` was intentionally not selected in the real Run and remains `MARKET_METADATA_MISSING` for execution. No historical tick value from the reference formula was substituted for user data.

### Correctness and failure evidence

- An eight-row irregular quote fixture hand-checks mid, spread, L1 bid/ask depth, L1 imbalance, microprice proxy, and session-local interarrival exactly.
- Zero imbalance denominator and unknown tick tests produce null values with exact reason codes.
- Appending future rows leaves all previously computed prefix values unchanged.
- A trade-only source is blocked with `SOURCE_FORMAT_UNSUPPORTED`/`SOURCE_FIELD_MISSING` before LOB computation.
- Adding a metadata ref changes the frozen source identity and therefore the cache/computation identity.
- Three preliminary real Runs exposed implementation/owner-version issues. Their results were retained for audit and marked `invalid` with explicit health reasons. Final Run `run_187b8…` had `cache_candidates=0` and used catalog/implementation v3.

Verification commands:

```bash
/data/src/research/.venv/bin/python -m pytest -q tests/test_l2_primitives.py \
  tests/test_run_planner.py tests/test_research_execution.py tests/test_research_queries.py
/data/src/research/.venv/bin/python -m pytest -q
/data/src/research/.venv/bin/ruff check quantsage_discovery scripts tests examples
```

The focused affected set passed 62 tests. The complete suite passed 262 tests, and full-project Ruff passed.

### Limits and handoff

- 03-A is not the complete Step 03 catalog. 03-B must add tick-distance space depth, WOBI/coverage/capacity, five-level VWAP/fill/walk, cumulative flow/reset evidence, and the full F01–F08 name counts.
- Real `spread_ticks` and all tick-dependent 03-B formulas remain blocked until the user supplies verified historical metadata and a new DataSource/catalog-bound Run is created.
- `available_time` remains unknown and all outputs are research-only.
- `quote_interarrival` is observation-time adjacency inside the frozen session, not a verified exchange update clock or quote-age claim.

Next increment: **03-B**.
