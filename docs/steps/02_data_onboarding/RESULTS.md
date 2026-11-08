# Step 02 Results — Data Onboarding

**Acceptance status:** COMPLETE. The full producer run succeeded, the independent consumer verifier returned `PASS`, and the final independent calendar audit passed on 2026-11-04. This closes Step 02 only; Step 03 was not run.

## Delivered run

The onboarding workflow froze and processed all 79 available monthly Parquet files for CFFEX government bond futures L2 snapshots. The source has 66 physical fields, 103 contracts, 1,587 observed trading dates, and spans 2020-01-02 through 2026-07-31. No later workflow stage was run.

| Measure | Verified result |
|---|---:|
| Input rows | 257,407,377 |
| Accepted quote rows | 257,069,701 |
| Quarantined rows | 337,676 |
| Reconciliation | 257,407,377 = 257,069,701 + 337,676 |
| Quote partitions | 16,636 |
| Quarantine artifacts | 79 |
| Frozen calendar dates | 1,594 |
| Source files / contracts | 79 / 103 |
| Published artifacts verified by consumer | 16,717, plus the verified outer entry manifest |

The output entry is `READY_WITH_WARNINGS`. It and its sidecar are [onboarding.json](../../../artifacts/step02/runs/step02-full79-20261103-r1/stages/stage_run_421b1736b6ef423499fab02b35375b16/artifacts/onboarding/onboard/snapshot_196d8ba3203cebdc50f94420a373638c4b0db6f98f4d7b2fe87b3a0220465fc2/1ea99d6d79592680e791512303d280e66ba1a9d83f00592ea79b218fa8610ea8/onboarding.json) and [the outer manifest](../../../artifacts/step02/runs/step02-full79-20261103-r1/stages/stage_run_421b1736b6ef423499fab02b35375b16/artifacts/onboarding/manifest.json). Entry SHA-256 is `sha256:3c801280b563ac650df23d2345cbc1a811ea60708bec474a2a3ba8d317c6bb3f`; outer manifest SHA-256 is `sha256:aacdcc4caffd69acd39ef7641368d3c0bceea0fd77c647f647a7f5ce211fef51`.

| Run identity | Value |
|---|---|
| Run | `step02-full79-20261103-r1` |
| Data snapshot | `snapshot_196d8ba3203cebdc50f94420a373638c4b0db6f98f4d7b2fe87b3a0220465fc2` |
| Split | `split_dac2ce9652419c9ca8b4c8955d89b718f8b53274be543b7a7d3a630036b9588f` (`unassigned_research_only`) |
| Config hash | `sha256:1ea99d6d79592680e791512303d280e66ba1a9d83f00592ea79b218fa8610ea8` |
| Resolved YAML file SHA-256 | `sha256:f623e1600ed64bcf054a110e3035604b3c22f6e9bc4ce3ecdbe6689d8c16d756` |
| Frozen run-level code revision | `sha256:134977720b12e3fabf7c6ca3e95b0e8a73cdf62461f6b195f47c7ff2621e2666` |
| Frozen onboard-stage revision | `sha256:ddb9154418f5e225e772add97f5315d803311d5855508d724f3f8eb2a6896da8` |

The [79-month config](../../../configs/full_79months.yaml), [resolved config](../../../artifacts/step02/preflight/snapshot_196d8ba3203cebdc50f94420a373638c4b0db6f98f4d7b2fe87b3a0220465fc2/ad191616f156406e926bbed53d846049b92265f8c6f7d4d3e27534b57a3407d0/resolved.yaml), [frozen source inventory](../../../artifacts/step02/preflight/snapshot_196d8ba3203cebdc50f94420a373638c4b0db6f98f4d7b2fe87b3a0220465fc2/ad191616f156406e926bbed53d846049b92265f8c6f7d4d3e27534b57a3407d0/source_inventory.json), and [frozen split manifest](../../../artifacts/step02/preflight/snapshot_196d8ba3203cebdc50f94420a373638c4b0db6f98f4d7b2fe87b3a0220465fc2/ad191616f156406e926bbed53d846049b92265f8c6f7d4d3e27534b57a3407d0/split_manifest.json) preserve the exact inputs and research-only split. The frozen inventory hash is `sha256:28e3923642f78679294ced6e5c0d8db2603b6f1542a73b146651f1e7dba9152c`, and split-manifest hash is `sha256:2e08b131d1b8d25b92fd7b90c94cd3cb9499b4944761198cb3f487f006907040`. The frozen package source and its revision scope are documented in [CODE_REVISION_SCOPE.md](CODE_REVISION_SCOPE.md) and the [snapshot manifest](../../../artifacts/step02/code_snapshots/snapshot_196d8ba3203cebdc50f94420a373638c4b0db6f98f4d7b2fe87b3a0220465fc2/ddb9154418f5e225e772add97f5315d803311d5855508d724f3f8eb2a6896da8/snapshot_manifest.json), whose archive-content hash is `ab67c533dd853df42ac17efe5b24bbf156107c21adbb7b42c01004d10abd3e4f`.

The frozen inventory, producer, independent consumer, and independent source baseline reconcile all 79 input files by content hash, byte count, row count, and per-file trade-date sets/ranges with zero mismatches. The files total 5,903,156,058 bytes and 257,407,377 rows.

## Quality and coverage

- The consumer independently reconciled all input rows to the published quote and quarantine files. It found 1,915 invalid L1 rows, 45,430 rows outside evidenced sessions, and 145,284 repeated event-key groups (290,568 group-member rows, including 145,284 duplicate extras). Every duplicate group was payload-identical under the consumer's comparison of product plus 62 L2 business fields; conflicting groups and repeated source-provenance keys were both zero. The union of these three quarantine categories was 337,676 rows, with 237 overlapping row hits across those categories. Rule-hit counts overlap, so they must not be summed as unique rows.
- The duplicate extra-row rate is 145,284 / 257,407,377 = 0.0005644127 (0.05644%). Entire repeated event-key groups are isolated because the source has no verified exchange sequence; no record is silently selected or merged.
- Another 223,963 accepted rows carry `DEPTH_BOOK_INVALID`; the snapshots remain available with that limitation flagged. The source-order audit found 0 reorder rows. The producer QualityReport flagged 47,536 cumulative/increment reconciliation mismatches; source values were not corrected and no cumulative reset rows were inferred. The independent consumer verifies the serialized report and output reconciliation, but does not independently recompute this cumulative/increment rule. Volume bars are disabled.
- Calendar evidence covers 1,594 trading dates. The seven dates with no source rows are `2021-01-08`, `2021-01-11`, `2021-04-07`, `2021-05-06`, `2021-05-07`, `2021-06-03`, and `2021-06-17`. These are source gaps, not calendar closures. Source data exists for 1,587 dates; accepted quotes span 1,586 dates because all 195,840 rows for `2021-05-31` were quarantined.
- The report includes 16,665 source/contract/date scopes, 66,529 evidenced-session scopes, and 49,871 within-session event-time-gap summaries covering 257,166,813 positive intervals. These are observation-time interval proxies. True quote age/staleness and point-in-time availability remain unverified because `available_time` is null for all rows; `research_only` remains true and cross-source point-in-time causality is disabled. Tick-size metadata is unknown for all 103 contracts, so `spread_ticks` is disabled; price-unit spread remains available.
- `END_OF_DAY_FIELDS_UNVERIFIED` and `VOLUME_INCREMENT_UNVERIFIED` are flagged on all 257,069,701 accepted rows. The dataset does not invent zero rows for dates without input.

The consumer matched the three-rule quarantine union against the independent full-source baseline in [independent_session_baseline.json](../../../artifacts/verification/independent_session_baseline.json). Calendar source applicability and the seven missing source dates were also independently audited against the published [CFFEX calendar evidence](../../references/cffex_calendar_evidence.md); final audit passed. The complete independent report is [consumer_full79_r1_batched_r3_8gb_2026-11-04.json](../../../artifacts/verification/consumer_full79_r1_batched_r3_8gb_2026-11-04.json) (SHA-256 `783bd14d490525268d2724fcd7c7027386f462489f797b26b007df98c809bff6`). It reports `PASS`, verifies the outer manifest, and scanned 16,716 Parquet files. A compatible checkpoint reused 16,715 artifact scan results after entry, reference, sidecar, scanner, and runtime binding checks. All artifact payloads (15,480,321,723 bytes total) were rehashed, along with sidecars and the outer entry manifest; date aggregations and global identity checks were rerun across the full input.

The producer run took 2:28:43 wall time, used a maximum resident set of 4,142,668 KB, and exited 0. The [workflow JSONL log](../../../artifacts/step02/logs/step02-full79-20261103-r1.jsonl), [stdout](../../../artifacts/step02/run_logs/full79_r1.stdout.log), and [`/usr/bin/time -v` output](../../../artifacts/step02/run_logs/full79_r1.time.log) preserve its run record.

## Verification commands

Run from the repository root. The production command uses the frozen resolved config above; it documents the completed run and is not an instruction to repeat it under the same run ID.

```bash
/usr/bin/time -v python -u -m quantsage_discovery.cli run \
  --config /data/src/quant_sage/artifacts/step02/preflight/snapshot_196d8ba3203cebdc50f94420a373638c4b0db6f98f4d7b2fe87b3a0220465fc2/ad191616f156406e926bbed53d846049b92265f8c6f7d4d3e27534b57a3407d0/resolved.yaml \
  --run-id step02-full79-20261103-r1 --until onboard \
  > artifacts/step02/run_logs/full79_r1.stdout.log \
  2> artifacts/step02/run_logs/full79_r1.time.log
```

The full consumer verifier used DuckDB with an 8 GB memory limit and 2 threads. Its compatible checkpoint was `artifacts/verification/consumer_full79_r1_batched_r2_checkpoint_2026-11-04.jsonl`.

```bash
/data/src/research/.venv/bin/python -u scripts/verify_onboarding.py \
  --entry artifacts/step02/runs/step02-full79-20261103-r1/stages/stage_run_421b1736b6ef423499fab02b35375b16/artifacts/onboarding/onboard/snapshot_196d8ba3203cebdc50f94420a373638c4b0db6f98f4d7b2fe87b3a0220465fc2/1ea99d6d79592680e791512303d280e66ba1a9d83f00592ea79b218fa8610ea8/onboarding.json \
  --entry-manifest artifacts/step02/runs/step02-full79-20261103-r1/stages/stage_run_421b1736b6ef423499fab02b35375b16/artifacts/onboarding/manifest.json \
  --duckdb-memory-limit 8GB --duckdb-threads 2 \
  --checkpoint artifacts/verification/consumer_full79_r1_batched_r2_checkpoint_2026-11-04.jsonl \
  --report artifacts/verification/consumer_full79_r1_batched_r3_8gb_2026-11-04.json
```

The final pre-run Step02/workflow suite passed 102 tests and full-scope Ruff. The post-run payload identity correction separately passed the 7-test onboarding-stage suite and Ruff for the changed producer/test files. January 2026 POC consumer acceptance is recorded in [consumer_jan2026_poc_r5_2026-11-03.json](../../../artifacts/verification/consumer_jan2026_poc_r5_2026-11-03.json).

```bash
/data/src/research/.venv/bin/python -m pytest -q tests/test_onboarding*.py tests/test_workflow_engine.py \
  --basetemp /tmp/qs-source-date-fingerprint-suite-20261104
/data/src/research/.venv/bin/ruff check quantsage_discovery scripts tests
python -m pytest -q tests/test_onboarding_stage.py \
  --basetemp artifacts/verification/product-payload-stage-20261104
python -m ruff check quantsage_discovery/stages/onboard.py tests/test_onboarding_stage.py
```

## Frozen run and later producer correction

The full run and its archived source use run revision `134977…` / onboard-stage revision `ddb915…`. After publication, payload fingerprinting was corrected to include `product`; the current working tree is run revision `8730b89606a6f07b4845c05ede1bcc3eb345780c01fa1ca649d4ee5100bc71f1` / onboard-stage revision `807f3d1d19ed9bac55278f8f87982bff2931918fa908988629e89e4d162d8dcf`. The change passed a serialized regression that changes only product within a duplicate event group. It was not used to rerun production and does not replace or modify the frozen archive. The independent full-source baseline also compared product plus all 62 business fields and found no conflicting payload groups in this dataset. See [the revision scope note](CODE_REVISION_SCOPE.md).

Step 03 and all later stages remain unimplemented and were not run. The source availability and product metadata limits above remain part of this dataset's research contract.

## 02-A frozen source, metadata and gap adaptation — 2026-11-07

**Status: COMPLETE for the available inputs.** The frozen 79-month onboarding entry is now registered as a versioned research `DataSource`. The user's real market metadata has not been provided, so no production tick/unit values were invented; the source remains usable for tick-independent research while tick-dependent work is explicitly blocked.

### Implemented behavior

- `quantsage adapt-onboarding-source` validates the published entry/outer manifest chain, all quote/quarantine content hashes, calendar uniqueness/order, session evidence, row reconciliation, and source schema before creating a new immutable source version.
- `quantsage_discovery/research/onboarding_adapter.py` creates a bounded source index, 49,800 evidenced `[start,end)` session intervals, a descriptive research-only access manifest derived from the original unassigned split, a versioned `gap_policy_v1` manifest, and `gap_events_v1` Parquet evidence.
- `OnboardingDataSourceReader` reads only an authorized contract/trade-date partition, verifies its hash, supports projected columns, applies no cross-session carry, resolves historical tick rules by the most specific matching contract/product rule, and reports unknown availability.
- `market_metadata_v1` now supports optional explicit price/size/turnover units and optional contract-specific tick rules. A contract rule overrides a product rule on dates it covers; product rules cover the remaining dates. Nonpositive ticks, overlapping same-scope rules and uncovered source partitions are rejected. The human template and validation command are in [configs/templates](../../../configs/templates/README.md).
- The onboarding producer now records volume and turnover reconciliation counts separately. For the already frozen report, a new read-only full-source pass derived and bound the same evidence without rewriting Step 02.

### Real adapted source

The selected accepted source is `cffex_treasury_l2_full79@2`.

| Field | Value |
|---|---|
| Data snapshot | `snapshot_196d8ba3203cebdc50f94420a373638c4b0db6f98f4d7b2fe87b3a0220465fc2` |
| Original split | `split_dac2cec9652419c9ca8b4c8955d89b718f8b53274be543b7a7d3a630036b9588f` |
| Data content hash | `sha256:73acc246fc198e1a32f3184a445e075af347954cba5f98769e8322e51299a949` |
| Instruments / accepted partitions | 103 / 16,636 |
| Accepted / quarantined / input rows | 257,069,701 / 337,676 / 257,407,377 |
| Session intervals | 49,800 |
| Gap events | 354,319 = 337,676 quarantined records + 16,636 unknown source-interval starts + 7 whole-date source gaps |
| Availability | `research_only`; `available_time` unknown |
| Market metadata | not supplied; no `market_metadata_ref`; `spread_ticks` blocked |

Artifacts are under `artifacts/step02/adapters/step02-02a-20261107-r3/cffex_treasury_l2-v2/`:

| Artifact | SHA-256 |
|---|---|
| `source.json` | `4c6a877304bd55707c43df01510324894bc085f621867af8124063c84797963b` |
| `data_source_manifest.json` | `aca12b769dfc19c491b9b5607e0ecf5694064c1107043b80c6a6f60eee036ca8` |
| `source_index.json` | `72284219374655016e93e9f56878f4c580ce282273b0a700c82323ef74a4b499` |
| `session_policy.json` | `e8e2cd8c22acd3a4d1c45bcf53d9e5d9eab6ce41690970f762d9c6d427a9483d` |
| `gap_manifest.json` | `1f9f7105ab2ab0e807f55d40d0dc65487af6a360970e4c9295cf985c02cb8333` |
| `gap_events.parquet` | `4165e12129f7c563c61358aa26e8485d8c76de30101fa6a47a14fb9b118c88ca` |
| `increment_reconciliation.json` | `e48599fc1b40e349d42b88792d8638955f5b9799025f374bb0802b26b326220b` |
| `access_policy.json` | `c6ddaab1c288b9ada844a1e72d2a3bf6b5c989393be47a138e92372b38a4c1b8` |
| `quality_summary.json` | `6c9f2bef7a6157e50a865447078641e2e011a829b51c82eb081093abb408de4e` |
| `research_only_split.json` | `7d22ac6d5efa35988932cc0b5cd23723c481794913774af4c06e34c670217fb3` |

The read-only increment reconciliation scan verified all 79 frozen raw hashes and independently found 23,768 volume mismatches plus 23,768 turnover mismatches. Their exact sum is the frozen combined count 47,536; volume and turnover cumulative-reset counts are both zero. These remain warnings and were not used to alter, quarantine or null any accepted value.

### Verification evidence

The local owner registered both source versions into `artifacts/research/registry.duckdb`; version 2 is the selected complete gap-evidence version. Owner stop, restart, and RPC read-back succeeded. The actual one-partition reader command was:

```bash
/data/src/research/.venv/bin/python -m examples.onboarding_source \
  --source artifacts/step02/adapters/step02-02a-20261107-r3/cffex_treasury_l2-v2/source.json \
  --instrument T2603 --date 2026-01-05
```

It read 24,274 accepted rows using only published refs. Mid was `107.9225`, price-unit spread was `0.005`, the opening-auction/morning/afternoon ranges remained separate, `tick_size` was null and `spread_ticks` was `BLOCKED_MARKET_METADATA_MISSING`. The same partition reported three quarantined source observations and one explicit unknown continuity start; none were mixed back into accepted quotes.

Targeted and affected regression command:

```bash
/data/src/research/.venv/bin/python -m pytest -q \
  tests/test_onboarding_adapter.py tests/test_research_management.py \
  tests/test_run_planner.py tests/test_onboarding_stage.py tests/test_onboarding_consumer.py
/data/src/research/.venv/bin/ruff check quantsage_discovery tests examples
```

At the time of this increment record: 112 affected tests passed, the complete suite passed 247 tests, and full-project Ruff passed. The sandboxed first full-suite attempt reached 240 passes but could not bind the existing local owner socket; the same suite passed outside that socket restriction.

### Limits and handoff

- Real tick, multiplier and unit metadata must come from the user. Once supplied, validate it with `quantsage validate-market-metadata`, create the next DataSource version with `--metadata`, and register it. This must not rerun onboarding.
- `available_time` remains unknown; no point-in-time cross-source causal claim is enabled.
- The 223,963 accepted depth-invalid rows remain available with `DEPTH_BOOK_INVALID`; Step 03 owns per-metric masks and must not treat unknown depth as zero.
- The two preliminary adapter runs remain audit history only. `r3`/v2 supersedes them because it adds explicit event-schema validation and separate reconciliation evidence; the underlying frozen source hash is unchanged.

Next increment: **03-A, basic primitives with real source consumption**.
