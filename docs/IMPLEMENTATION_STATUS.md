# Implementation Status

> Steps 01 and 02 are implemented and verified. Step 03 and later stages remain not started; this repository's completed workflow stops at data onboarding.

| Step | Status | Changes / evidence | Tests | Known limitations |
|---|---|---|---|---|
| M0 Project scaffold | COMPLETE | Python package, pyproject, YAML config validation, argparse CLI and pytest; root README | Included in Step 01 tests and CLI smoke | Installation into a dedicated environment remains a user deployment choice; verification used the existing Python environment |
| 01 Workflow Engine | COMPLETE | Generic DAG, typed artifacts, mandatory validators, scoped caches, atomic publication, SQLite ledger, partition resume and structured logs; [results](steps/01_workflow_engine/RESULTS.md) | 2026-11-02: 14 tests passed; Ruff passed; seven CLI commands exercised | Local thread executor and file artifacts; stage plugins must enforce their domain resource budgets |
| 01 extension: explicit research batches | PARTIAL | Catalog-backed item plans, data scopes/targets, partition cache/resume, state history, parent deltas and CLI item status; [increment results](steps/01_workflow_engine/RESEARCH_BATCH_RESULTS.md) | 2026-11-06: full suite 123 passed (21 batch tests), Ruff passed, synthetic A/B/C demonstration completed | No real Step 03–05 algorithms; full Study/search/coverage model and shared-table bindings remain pending |
| 01-A: DuckDB Registry and embedded owner | COMPLETE | DuckDB ledger, version/checksum history, transactional upgrade, owner request queue, workspace lock, shared same-process handles and explicit close; [results](steps/01_workflow_engine/RESULTS.md#01-a-duckdb-registry-与嵌入-owner2026-11-06) | 2026-11-06: 13 Registry tests and full suite 136 passed; Ruff passed | New DuckDB workspaces only; no historical database import; local Linux file lock; Study/FeatureSet and later increments remain pending |
| 01-B: Study / DataSource management | COMPLETE | Immutable sources, serialized reference/hash validation, user metadata rules, revisioned Study/history and active-state guard; [results](steps/01_workflow_engine/RESULTS.md#01-b-study--datasource-管理2026-11-06) | 30 new management tests; full suite 166 passed; Ruff and Python module smoke passed | Fixture-backed generic source contract; real onboarding mapping belongs to 02-A; real metadata path pending |
| 01-C: Feature / FeatureSet freezing | COMPLETE | Versioned trusted catalog, canonical definitions, dependency resolver, atomic freeze and draft revisions; [results](steps/01_workflow_engine/RESULTS.md#01-c-feature--featureset-冻结2026-11-06) | 20 new freeze tests; full suite 186 passed; Ruff passed | Synthetic registered providers only; real algorithms and Run planner pending |
| 01-D: Run planning and idempotent submission | COMPLETE | Scope-aware frozen DAG, recursive history, trade-date/session partitioning, permissions, whole-plan budget gate and atomic Run/task registration; [results](steps/01_workflow_engine/RESULTS.md#01-d-run-规划与幂等登记2026-11-07) | 17 planner tests; full suite 203 passed; Ruff passed | Synthetic sources; execution/cache publication follows in 01-E |
| 01-E: Execution, publication and recovery | COMPLETE | Frozen DAG, synthetic Parquet providers, verified cache bindings, durable receipts, atomic result/link/state commits, health and attempt history; [results](steps/01_workflow_engine/RESULTS.md#01-e-执行发布与恢复2026-11-07) | 18 execution tests; full suite 221 passed; Ruff and independent-process execute/resume example passed | Synchronous synthetic executor; CPU/memory hard caps explicitly rejected before submission; real L2 algorithms pending |
| 01-F: Results / Coverage / Review | IN_PROGRESS | Query, coverage, review and comparison APIs under implementation | Pending acceptance | No completion claim |
| 02 Data Onboarding | COMPLETE | Bootstrap source freeze, CFFEX L2 normalization, quality reporting, typed quotes/quarantine, calendar/session snapshots, and atomic `onboarding.json`; [results](steps/02_data_onboarding/RESULTS.md), [frozen revision scope](steps/02_data_onboarding/CODE_REVISION_SCOPE.md) | 102 Step02/workflow tests and Ruff passed before the frozen run; post-run product-payload regression: 7 tests + Ruff; full 79-month producer run succeeded; independent consumer PASS and calendar audit PASS | Research-only; `available_time` absent and point-in-time causality unverified; `spread_ticks` and volume bars disabled; 223,963 accepted rows carry depth-invalid flags; 47,536 cumulative/increment reconciliation warnings are producer-reported; seven calendar trading dates have no source rows |
| 03 Primitives | NOT_STARTED | — | — | — |
| 04 Business Features & Time Windows | NOT_STARTED | — | — | — |
| 05 Statistical Features & Quality | NOT_STARTED | — | — | — |
| 06 Event Detector | NOT_STARTED | — | — | — |
| 07 Regime Detector | NOT_STARTED | — | — | — |
| 08 Phenomenon Discovery | NOT_STARTED | — | — | — |
| 09 Hypothesis Generators | NOT_STARTED | — | — | — |
| 10 Discovery Cube | NOT_STARTED | — | — | — |
| 11 Falsification | NOT_STARTED | — | — | — |
| 12 Research Planner | NOT_STARTED | — | — | — |
| 13 Integration | NOT_STARTED | — | — | — |
