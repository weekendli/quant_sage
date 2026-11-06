# Codex Implementation Task — Step 02: Data Onboarding & Quality

## 当前收敛范围：Step 02–05 数据与特征闭环

本步交付可复用的 DataSource：以现有 onboarding.json 与 artifact manifests 表达源版本、标准化行情、质量、日历/session 和血缘。Study/Run 通过 refs 消费；DataSource 是管理层映射，不要求重写已冻结的 onboarding 文件。新 feature、size、L/W 或 statistic 选择不触发重新接入。只有源数据/接入规则变化或结果不可用时才需要新的接入产物；扩展 Run 日期先检查已有源覆盖，覆盖外明确阻断或另行接入。

本步不承担 FeatureSet 解析、指标计算或发现模型。已完成证据见 RESULTS.md；本次仅收敛消费边界，不改变历史交付及验收结论。

统一管理字段见 [domain.detail.md](../../domain.detail.md)，当前调度边界见 [WORKFLOW_STAGES.md](../../WORKFLOW_STAGES.md)。旧 Registry 名称表示 Feature/FeatureResult 的分类视图，不要求新增独立领域仓库。Study、FeatureSet、Run、Review 的通用管理由 Step 01 承担，领域步骤只实现本步算子与结果契约。


**Milestone:** M1  
**Upstream dependencies:** Step 01  
**Scope:** implement only this stage, not downstream algorithms.

## Required reading

1. [`../../AGENTS.md`](../../AGENTS.md) — repository-wide rules.
2. [`../../COMMON_CONTRACTS.md`](../../COMMON_CONTRACTS.md) — strict data/time/provenance contract.
3. [`SPEC.md`](SPEC.md) — full stage design.
4. [`VERIFY.md`](VERIFY.md) — this stage's verifiable exit gate.
5. [`DELIVERY.md`](DELIVERY.md) — POC consumer scenarios, delivery entry point, local data sources, calendar access and release rules.
6. [`../../data.md`](../../data.md) — verified paths, Python/DuckDB examples and current calendar limitations.

## What to build

1. Implement the `l2_snapshots` Parquet/CSV adapter using the actual uppercase physical columns in [`L2 Schema`](../../references/gov_bond_l2.schema.md) and its JSON. The local sample at `/data/marketdata-backup/futures/treasury_futures/parquet/l2_snapshot/v1/year=2026/month=01/part-0.parquet` contains 3,178,989 rows and 66 columns (62 L2 fields plus four ingestion metadata fields), not a summary table. Validate date/timestamp/decimal/string types without replacing them with Word declarations.
2. Deliver the files defined in DELIVERY.md. Embed the minimal DatasetPassport and QualityReport in onboarding.json; expose normalized data and frozen dependencies through ArtifactManifest references. Reuse the existing source schema JSON plus the explicit mapping/derived-column rules.
3. Validate timestamp semantics, sessions, source sequences and quote data.
4. Quarantine bad rows with raw-record provenance and reason codes.
5. Supply independently sorted contract/session streams and the alignment constraints needed by downstream as-of joins. POC handoff tests exercise staleness and availability flags without materializing an aligned cross-contract table or implementing Step 03.
6. Preserve `product`, `source_file`, `source_row`, `ingest_run_id` and physical Parquet row provenance in accepted and quarantined outputs. Distinguish event-key duplicates, identical business payloads, conflicting payloads and source-record duplicates; never treat `source_row` as an exchange sequence. For this POC, quarantine all rows of a duplicate event-key group without a reliable exchange sequence; do not merge sources or silently select one record.
7. Validate `TRADINGTIME` against `UNIX` in milliseconds under an explicit source timezone. Retain unknown availability; neither UNIX nor the ingestion batch ID supplies per-record receipt time.
8. Stream row groups/batches for the sample and full dataset. Audit physical ordering before sorting; scope cumulative differences, deduplication and time gaps by source, actual contract, date and confirmed session semantics.
9. Read local market data from the verified partition root in SPEC.md using Python/DuckDB, with bounded file selection and explicit physical schema checks. Resolve the local calendar catalog through `/data/marketdata/catalog/duckdb/current.json` and open its database read-only; alternatively use the Marketdata LAN API. Pin the release and resolved file dependencies for local queries; for API responses validate positional rows/types, truncated=false and a consistent release. Both paths must validate the selected calendar ID and coverage, and freeze calendar.parquet with provenance. The verified release has only CN_STOCK; the independent CFFEX evidence in `configs/reference/cffex_calendar_evidence.json` confirms exact date membership for 2020-01-02 through 2026-07-31. Validate that evidence, its actual input hashes and range at runtime; do not extrapolate or silently treat the provider ID as a universal default. Avoid the calendar directory wildcard, which doubles date keys. The trading_sessions and settlement_calendars views are empty placeholders; require separately verified, versioned intraday rules before publishing.
10. Publish onboarding.json last after artifact validation and row reconciliation. The downstream consumer fixture must use only this entry point and its artifact refs. See DELIVERY.md for required files, fields, unknown-value handling and empty/error paths.

## Supplied sample and evidence

Read [`part-0.profile.json`](../../references/part-0.profile.json) for observed results, tool versions and source hash. These are descriptive file checks, not passing adapter tests. Reproduce with the read-only documentation utility from the repository root:

```bash
python3 - <<'PYTHON'
import json
import runpy
from pathlib import Path

module = runpy.run_path("docs/references/inspect_l2_sample.py")
sample = Path("/data/marketdata-backup/futures/treasury_futures/parquet/l2_snapshot/v1/year=2026/month=01/part-0.parquet")
print(json.dumps(module["audit"](sample), ensure_ascii=False, indent=2))
PYTHON
```

The utility's default CLI expects `docs/references/part-0.parquet`, which is not present in the repository checkout. The invocation above calls its existing `audit(path)` function against the local source without copying or modifying data.

Use the recorded `pyarrow` and `pandas` versions. The utility scans all fields in batches for null/L1 checks, and loads only nine identity/time/provenance columns for grouping. It is a single-sample audit, not the full-dataset streaming adapter. Its physical-order gap quantiles include intraday breaks and must not be reused as a session-filtered staleness distribution.

Use small serialized fixtures for failure branches, then integrate with the supplied real sample. Missing `TRADINGTIME` or required book columns must still produce `MISSING_FIELD`. Calendar access may use the verified local read-only catalog or the Marketdata LAN API; bounded calendar applicability and historical sessions now have the independently checked evidence linked above; their runtime selection, sentinel policy, tick size and multipliers still require explicit provenance/configuration. The 2026-11-02 documentation check successfully queried local Parquet, the calendar catalog, and HTTPS health/calendar endpoints; this is access evidence, not a completed Stage 02 integration test. The CA and optional client remain outside the repository; see data.md for their server-local locations.

## Input contract

Use the precise inputs declared in `SPEC.md`. Load upstream artifacts via manifest references. When a prerequisite stage is not yet implemented, **test with serialized fixture artifacts that obey the contract**, never with fake production success or untracked in-memory mocks.

## Required outputs

Implement the stage's declared persistable output artifacts with stable IDs, `schema_version`, input hashes, `available_time` semantics where applicable, status and quality flags. Integrate with the common stage interface; artifact file formats can follow `SPEC.md` and shared contracts.

## Codex prompt (paste into Codex)

> Read AGENTS.md, COMMON_CONTRACTS.md, steps/02_data_onboarding/SPEC.md, steps/02_data_onboarding/IMPLEMENT.md, steps/02_data_onboarding/DELIVERY.md and steps/02_data_onboarding/VERIFY.md. Implement **only Step 02: Data Onboarding & Quality** and dependencies strictly required to make its documented contract runnable. Create actual code and synthetic fixtures; add unit tests, an integration test and time-leakage or quality tests as applicable. Do not leave algorithmic stubs. Run tests, report exact command and results, update IMPLEMENTATION_STATUS.md with the true status, and stop before implementing the next stage.

## Exit criterion

Stage implementation is not complete until every item in [`VERIFY.md`](VERIFY.md) is checked by real automated tests or a documented reproducible CLI run; no manufactured test claims.
