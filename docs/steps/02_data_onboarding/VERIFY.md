# Acceptance / Verification — Step 02: Data Onboarding & Quality

**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## Minimal test fixture

S0 + S4 + S5: 2 instruments, irregular updates, one session boundary, one invalid quote.

## Required acceptance checks

- [ ] Duplicate timestamp with distinct sequence is distinguished from exact duplicate.
- [ ] Crossed/negative quotes are quarantined rather than silently used.
- [ ] Cumulative volume reset at session boundary does not yield negative trade volume.
- [ ] Irregular time gaps and quote age remain explicit; no implicit forward fill over session.
- [ ] Data without available_time is marked unknown and not treated as verified point-in-time.

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
