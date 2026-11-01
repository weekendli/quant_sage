# Codex Repository Instructions — Stepwise Development

The `STEP_INDEX.md` points to 13 self-contained stage directories. For a selected stage, read its `SPEC.md`, `IMPLEMENT.md` and `VERIFY.md`, plus `COMMON_CONTRACTS.md` and `ARCHITECTURE.md`.

1. Work on **exactly one selected stage** at a time. Do not pre-build other stages as empty stubs.
2. Keep stage public inputs/outputs schema-validated, serialized and reproducible; pass large data by artifact ref.
3. Each stage must be independently verifiable with synthetic artifact fixtures even if upstream code is not yet complete; when upstream is implemented, also run integration with real upstream outputs.
4. Never assume `available_time == event_time` without explicit evidence, or look into holdout/future for fit/thresholds/online detection.
5. Respect irregular time updates, exchange sessions, stale quotes, contract identities and train/development/holdout separation.
6. Every implemented stage must include executable tests for success, failure, no-signal/empty input, lineage, and time-awareness where relevant.
7. Reports must state what actually ran, measured results, test commands and skipped cases; do not fabricate scientific outcomes.
8. Update `IMPLEMENTATION_STATUS.md` at the end of each stage. Only mark complete after `VERIFY.md` requirements pass.
9. If a component has too broad a scope, break its *implementation* into commits while preserving its output contract, without drifting to the next stage.
