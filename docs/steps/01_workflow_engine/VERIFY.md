# Acceptance / Verification — Step 01: Workflow Engine

**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## Minimal test fixture

A synthetic no-op DAG producing small test artifacts; no market data needed.

## Required acceptance checks

- [ ] Cycle / missing dependency triggers an explicit validation error.
- [ ] Same snapshot, config, code and inputs produce a cache hit with no duplicate outputs.
- [ ] Changed stage config invalidates the affected stage and descendants only.
- [ ] Killed partition followed by resume creates one committed artifact per partition.
- [ ] Failed artifact validation never publishes a SUCCEEDED manifest.

## Required evidence in Codex completion report

- [ ] Exact command(s) actually run (including `pytest` selectors and CLI smoke test where applicable).
- [ ] Tests have passing assertions on actual serialized artifacts; no no-op implementations.
- [ ] Schema and provenance IDs verified, with sample artifact paths or manifest examples.
- [ ] Deterministic behavior under a pinned seed/config and recorded upstream snapshot.
- [ ] Error/empty-input paths explicitly tested and reported.
- [ ] Performance/resource characteristics or limits documented (streaming for data-heavy stages).
- [ ] `IMPLEMENTATION_STATUS.md` updated only after confirmation.

## Handoff / done definition

A downstream step may depend on this module **only after** its public artifact contract is stable and the above tests pass. Missing prerequisites may be represented by validated fixture artifacts while developing this module, but final integration must use real upstream outputs.
