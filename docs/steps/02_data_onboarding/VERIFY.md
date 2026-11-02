# Acceptance / Verification — Step 02: Data Onboarding & Quality

**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## POC delivery contract

Use [DELIVERY.md](DELIVERY.md) as the handoff contract. All checks below are requirements, not completed evidence.

- [ ] One published onboarding.json resolves quotes, quarantine, calendar.parquet and session_rules.json; embeds Passport/QualityReport; all data artifacts have valid common ArtifactManifests. Source schema is referenced by hash and mapping, not redefined manually.
- [ ] All retained and quarantined rows preserve physical source identity; POC does not merge duplicates, and input_rows = accepted_rows + quarantined_rows. Zero-row quarantine retains its schema.
- [ ] Discover and verify the actual futures calendar_id through Marketdata API; parse positional JSON rows by columns and types; reject truncated or mixed-release responses. Freeze query parameters, range, release, response metadata and output hash.
- [ ] A truncated result, unknown calendar_id, empty/inadequate calendar coverage, missing session rules, API error and verified offline snapshot fallback are exercised with serialized API fixtures; run real calendar integration once CA and network access are configured, report any skipped integration honestly.
- [ ] A trading-day list alone cannot assign intraday sessions. Frozen session rules classify boundary times explicitly; ambiguous/out-of-session rows are quarantined, absent rules block successful publication.
- [ ] Consumer fixture reads only the published entry and referenced artifacts; verifies hand-calculated mid, price-unit spread, L1 OBI and session-local intervals. Unknown tick size disables spread_ticks; unknown availability remains research_only. No Step 03 factory is required for this handoff test.
- [ ] Failure before final publication leaves no readable READY entry. Empty input or zero accepted records produces an explicit diagnostic and cannot trigger downstream processing.

## Minimal test fixture

S0 + S4 + S5: 2 instruments, irregular updates, one session boundary, one invalid quote.

## Required acceptance checks

- [ ] Supplied `docs/references/part-0.parquet` matches the recorded SHA-256, 3,178,989 rows, 26 row groups and ordered 66-column physical schema (62 uppercase business fields plus four ingestion metadata fields); nullable declarations are distinguished from observed null counts.
- [ ] Explicit mapping of `TRADINGDATE` date32, timezone-naive `TRADINGTIME` timestamp[us], int64 `UNIX`, string `SECURITYID`, all five book levels and decimal precision is verified on serialized outputs. Missing time/book fixture columns produce `MISSING_FIELD`.
- [ ] `TRADEVOLUME` / `TRADEAMOUNT` remain interval increments; `TOTALVOLUME` / `TOTALAMOUNT` remain cumulative. Validate consistency within confirmed source/day boundaries without double differencing or silently forcing discrepancies to zero.
- [ ] `UNIX` milliseconds and local `TRADINGTIME` are checked under explicit timezone configuration; a wrong timezone/unit fixture fails, and the full sample reproduces the recorded offset result.
- [ ] All four upstream metadata fields survive normalization/quarantine. Source filename disagreement never overwrites actual contract identity; `source_row` and `ingest_run_id` do not become exchange sequence or per-record availability.
- [ ] Reproduce sample event-key duplicate counts from the profile. Distinguish identical business payloads, conflicting values at one event key and repeated provenance keys; the POC quarantines the entire repeated event-key group with reason codes and preserves all source rows; no automatic merge/removal is performed.
- [ ] Zero settlement/close values and reserved fields follow explicit field-specific policies; a valid book is not rejected solely because a daily field is zero. Missing/zero/negative book fixtures cover invalid quote handling.
- [ ] Do not invent a sequence for the supplied source. If another adapter supplies a verified exchange sequence, its same-timestamp records follow its declared key contract rather than this POC's no-sequence rule.
- [ ] Crossed/negative quotes are quarantined rather than silently used.
- [ ] Cumulative volume reset at a confirmed source boundary does not yield negative trade volume; a lunch break alone does not force a daily cumulative reset.
- [ ] Physical input ordering is audited before sorting; interleaved contracts and cross-file duplicates are not counted as exchange ordering errors. Gap/quote-age calculations respect source, actual contract and session; no implicit forward fill over session.
- [ ] Research-only book primitives are permitted after quality checks; unknown availability blocks claims of verified point-in-time cross-source causality. Snapshot-derived features never claim true order/cancel reconstruction.
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
