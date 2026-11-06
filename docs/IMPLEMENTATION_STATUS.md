# Implementation Status

> Steps 01 and 02 are implemented and verified. Step 03 and later stages remain not started; this repository's completed workflow stops at data onboarding.

| Step | Status | Changes / evidence | Tests | Known limitations |
|---|---|---|---|---|
| M0 Project scaffold | COMPLETE | Python package, pyproject, YAML config validation, argparse CLI and pytest; root README | Included in Step 01 tests and CLI smoke | Installation into a dedicated environment remains a user deployment choice; verification used the existing Python environment |
| 01 Workflow Engine | COMPLETE | Generic DAG, typed artifacts, mandatory validators, scoped caches, atomic publication, SQLite ledger, partition resume and structured logs; [results](steps/01_workflow_engine/RESULTS.md) | 2026-11-02: 14 tests passed; Ruff passed; seven CLI commands exercised | Local thread executor and file artifacts; stage plugins must enforce their domain resource budgets |
| 01 extension: explicit research batches | PARTIAL | Catalog-backed item plans, data scopes/targets, partition cache/resume, state history, parent deltas and CLI item status; [increment results](steps/01_workflow_engine/RESEARCH_BATCH_RESULTS.md) | 2026-11-06: full suite 123 passed (21 batch tests), Ruff passed, synthetic A/B/C demonstration completed | No real Step 03–05 algorithms; full Study/search/coverage model and shared-table bindings remain pending |
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
