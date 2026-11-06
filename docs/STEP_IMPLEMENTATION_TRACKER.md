# Step-by-step Implementation Tracker

States are updated from actual implementation and verification evidence; later stages remain `NOT_STARTED`.

| Step | Status | Tests run | Manifest/example evidence | Limitations |
|---|---|---|---|---|
| 01: Workflow Engine | COMPLETE | 14 pytest tests passed; Ruff passed, 2026-11-02 | [Results](steps/01_workflow_engine/RESULTS.md); artifacts/verification/step01-20261102/ | Local thread executor and file artifacts |
| 01 extension: research batches | PARTIAL | 123 full-suite tests passed, including 21 batch tests; Ruff passed, 2026-11-06 | [Increment results](steps/01_workflow_engine/RESEARCH_BATCH_RESULTS.md); synthetic A/B/C reports | Generic execution only; real features and full Study/search inventory remain pending |
| 01-A: DuckDB Registry / embedded owner | COMPLETE | 13 Registry tests; full suite 136 passed; Ruff passed, 2026-11-06 | [Increment results](steps/01_workflow_engine/RESULTS.md#01-a-duckdb-registry-与嵌入-owner2026-11-06); tests/test_registry_duckdb.py; tests/test_workflow_engine.py | No database import or new domain algorithms; next increment 01-B |
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
