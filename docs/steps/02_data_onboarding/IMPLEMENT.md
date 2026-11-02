# Codex Implementation Task — Step 02: Data Onboarding & Quality

**Milestone:** M1  
**Upstream dependencies:** Step 01  
**Scope:** implement only this stage, not downstream algorithms.

## Required reading

1. [`../../AGENTS.md`](../../AGENTS.md) — repository-wide rules.
2. [`../../COMMON_CONTRACTS.md`](../../COMMON_CONTRACTS.md) — strict data/time/provenance contract.
3. [`SPEC.md`](SPEC.md) — full stage design.
4. [`VERIFY.md`](VERIFY.md) — this stage's verifiable exit gate.
5. [`DELIVERY.md`](DELIVERY.md) — POC consumer scenarios, delivery entry point, calendar API and release rules.

## What to build

1. Implement the `l2_snapshots` Parquet/CSV adapter using the actual uppercase physical columns in [`L2 Schema`](../../references/gov_bond_l2.schema.md) and its JSON. The supplied `part-0.parquet` contains 3,178,989 rows and 66 columns (62 L2 fields plus four ingestion metadata fields), not a summary table. Validate date/timestamp/decimal/string types without replacing them with Word declarations.
2. Deliver the files defined in DELIVERY.md. Embed the minimal DatasetPassport and QualityReport in onboarding.json; expose normalized data and frozen dependencies through ArtifactManifest references. Reuse the existing source schema JSON plus the explicit mapping/derived-column rules.
3. Validate timestamp semantics, sessions, source sequences and quote data.
4. Quarantine bad rows with raw-record provenance and reason codes.
5. Supply independently sorted contract/session streams and the alignment constraints needed by downstream as-of joins. POC handoff tests exercise staleness and availability flags without materializing an aligned cross-contract table or implementing Step 03.
6. Preserve `product`, `source_file`, `source_row`, `ingest_run_id` and physical Parquet row provenance in accepted and quarantined outputs. Distinguish event-key duplicates, identical business payloads, conflicting payloads and source-record duplicates; never treat `source_row` as an exchange sequence. For this POC, quarantine all rows of a duplicate event-key group without a reliable exchange sequence; do not merge sources or silently select one record.
7. Validate `TRADINGTIME` against `UNIX` in milliseconds under an explicit source timezone. Retain unknown availability; neither UNIX nor the ingestion batch ID supplies per-record receipt time.
8. Stream row groups/batches for the sample and full dataset. Audit physical ordering before sorting; scope cumulative differences, deduplication and time gaps by source, actual contract, date and confirmed session semantics.
9. Fetch the selected government-bond-futures trading calendar through the supplied Marketdata API; validate positional rows/types, truncated=false, a consistent release, calendar ID and query coverage. Freeze calendar.parquet and the request provenance. Obtain intraday session definitions separately from a verified API table or versioned explicit rules; do not infer them from trading dates.
10. Publish onboarding.json last after artifact validation and row reconciliation. The downstream consumer fixture must use only this entry point and its artifact refs. See DELIVERY.md for required files, fields, unknown-value handling and empty/error paths.

## Supplied sample and evidence

Read [`part-0.profile.json`](../../references/part-0.profile.json) for observed results, tool versions and source hash. These are descriptive file checks, not passing adapter tests. Reproduce with the read-only documentation utility from the repository root:

```bash
python3 docs/references/inspect_l2_sample.py
```

Use the recorded `pyarrow` and `pandas` versions. The utility scans all fields in batches for null/L1 checks, and loads only nine identity/time/provenance columns for grouping. It is a single-sample audit, not the 30GB production streaming adapter. Its physical-order gap quantiles include intraday breaks and must not be reused as a session-filtered staleness distribution.

Use small serialized fixtures for failure branches, then integrate with the supplied real sample. Missing `TRADINGTIME` or required book columns must still produce `MISSING_FIELD`. The calendar provider is the supplied Marketdata LAN API; the calendar ID, session rules, sentinel policy, tick size and multipliers require explicit provenance/configuration. No CA certificate or API client is currently in the repository, so the documentation update does not claim a live API check.

## Input contract

Use the precise inputs declared in `SPEC.md`. Load upstream artifacts via manifest references. When a prerequisite stage is not yet implemented, **test with serialized fixture artifacts that obey the contract**, never with fake production success or untracked in-memory mocks.

## Required outputs

Implement the stage's declared persistable output artifacts with stable IDs, `schema_version`, input hashes, `available_time` semantics where applicable, status and quality flags. Integrate with the common stage interface; artifact file formats can follow `SPEC.md` and shared contracts.

## Codex prompt (paste into Codex)

> Read AGENTS.md, COMMON_CONTRACTS.md, steps/02_data_onboarding/SPEC.md, steps/02_data_onboarding/IMPLEMENT.md, steps/02_data_onboarding/DELIVERY.md and steps/02_data_onboarding/VERIFY.md. Implement **only Step 02: Data Onboarding & Quality** and dependencies strictly required to make its documented contract runnable. Create actual code and synthetic fixtures; add unit tests, an integration test and time-leakage or quality tests as applicable. Do not leave algorithmic stubs. Run tests, report exact command and results, update IMPLEMENTATION_STATUS.md with the true status, and stop before implementing the next stage.

## Exit criterion

Stage implementation is not complete until every item in [`VERIFY.md`](VERIFY.md) is checked by real automated tests or a documented reproducible CLI run; no manufactured test claims.
