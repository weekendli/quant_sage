# Step-by-step Implementation Tracker

States are updated from actual implementation and verification evidence; later stages remain `NOT_STARTED`.

| Step | Status | Tests run | Manifest/example evidence | Limitations |
|---|---|---|---|---|
| 01: Workflow Engine | COMPLETE | 14 pytest tests passed; Ruff passed, 2026-11-02 | [Results](steps/01_workflow_engine/RESULTS.md); artifacts/verification/step01-20261102/ | Local thread executor and file artifacts |
| 01 extension: research batches | PARTIAL | 123 full-suite tests passed, including 21 batch tests; Ruff passed, 2026-11-06 | [Increment results](steps/01_workflow_engine/RESEARCH_BATCH_RESULTS.md); synthetic A/B/C reports | Generic execution only; real features and full Study/search inventory remain pending |
| 01-A: DuckDB Registry / embedded owner | COMPLETE | 13 Registry tests; full suite 136 passed; Ruff passed, 2026-11-06 | [Increment results](steps/01_workflow_engine/RESULTS.md#01-a-duckdb-registry-与嵌入-owner2026-11-06); tests/test_registry_duckdb.py; tests/test_workflow_engine.py | No database import or new domain algorithms; next increment 01-B |
| 01-B: Study / Source management | COMPLETE | 30 management tests; full suite 166 passed; Ruff and module entry smoke passed | [Results](steps/01_workflow_engine/RESULTS.md#01-b-study--datasource-管理2026-11-06); tests/research_fixtures.py; examples/research_management.py | Generic source fixtures; real adapter and later management increments pending |
| 01-C: FeatureSet freeze | COMPLETE | 20 freeze tests; full suite 186 passed; Ruff passed | [Results](steps/01_workflow_engine/RESULTS.md#01-c-feature--featureset-冻结2026-11-06); tests/test_feature_sets.py | Numerical providers and planner pending |
| 01-D: Run planning / submission | COMPLETE | 17 planner tests; full suite 203 passed; Ruff passed, 2026-11-07 | [Results](steps/01_workflow_engine/RESULTS.md#01-d-run-规划与幂等登记2026-11-07); tests/test_run_planner.py | Synthetic planning; execution and verified reuse follow in 01-E |
| 01-E: Execution / publication / recovery | COMPLETE | 18 execution tests; full suite 221 passed; Ruff and subprocess example passed, 2026-11-07 | [Results](steps/01_workflow_engine/RESULTS.md#01-e-执行发布与恢复2026-11-07); tests/test_research_execution.py; examples/research_execution.py | Synthetic providers; unsupported hard CPU/memory limits rejected in preflight |
| 01-F: Results / Coverage / Review | IN_PROGRESS | Pending acceptance | Implementation underway | No real Step 03–05 computations yet |
| 02: Data Onboarding & Quality | COMPLETE | 102 Step02/workflow tests + Ruff passed before production; post-run payload identity regression 7 tests + Ruff; 79-month run and independent consumer verification PASS; final calendar audit PASS | [Results and published entry/report/config/archive links](steps/02_data_onboarding/RESULTS.md); [calendar evidence](references/cffex_calendar_evidence.md); [code revision scope](steps/02_data_onboarding/CODE_REVISION_SCOPE.md) | Research-only and point-in-time causality unverified; no available_time; spread_ticks and volume bars disabled; 223,963 accepted depth-invalid rows flagged; 47,536 increment reconciliation warnings reported by producer; seven trading dates have no source rows |
| 03: Research Dimensions & Primitives | NOT_STARTED | — | — | — |
| 04: Business Features & Time Windows | NOT_STARTED | — | — | — |
| 05: Statistical Features & Quality | NOT_STARTED | — | — | — |
| 06: Event Detector | NOT_STARTED | — | — | — |
| 07: Regime Detector | NOT_STARTED | — | — | — |
| 08: Phenomenon Discovery | NOT_STARTED | — | — | — |
| 09: Hypothesis Generation | NOT_STARTED | — | — | — |
| 10: Discovery Cube | NOT_STARTED | — | — | — |
| 11: Falsification & Validation | NOT_STARTED | — | — | — |
| 12: Research Planner & Knowledge | NOT_STARTED | — | — | — |
| 13: End-to-End Integration | NOT_STARTED | — | — | — |
