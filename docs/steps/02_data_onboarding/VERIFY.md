# Acceptance / Verification — Step 02: Data Onboarding & Quality

## 当前收敛范围：Step 02–05 数据与特征闭环

本步交付可复用的 DataSource：以现有 onboarding.json 与 artifact manifests 表达源版本、标准化行情、质量、日历/session 和血缘。Study/Run 通过 refs 消费；DataSource 是管理层映射，不要求重写已冻结的 onboarding 文件。新 feature、size、L/W 或 statistic 选择不触发重新接入。只有源数据/接入规则变化或结果不可用时才需要新的接入产物；扩展 Run 日期先检查已有源覆盖，覆盖外明确阻断或另行接入。

本步不承担 FeatureSet 解析、指标计算或发现模型。已完成证据见 RESULTS.md；本次仅收敛消费边界，不改变历史交付及验收结论。

统一管理字段见 [domain.detail.md](../../domain.detail.md)，当前调度边界见 [WORKFLOW_STAGES.md](../../WORKFLOW_STAGES.md)。旧 Registry 名称表示 Feature/FeatureResult 的分类视图，不要求新增独立领域仓库。Study、FeatureSet、Run、Review 的通用管理由 Step 01 承担，领域步骤只实现本步算子与结果契约。


**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## POC delivery contract

Use [DELIVERY.md](DELIVERY.md) as the handoff contract. All checks below are requirements, not completed evidence.

- [ ] One published onboarding.json resolves quotes, quarantine, calendar.parquet and session_rules.json; embeds Passport/QualityReport; all data artifacts have valid common ArtifactManifests. Source schema is referenced by hash and mapping, not redefined manually.
- [ ] All retained and quarantined rows preserve physical source identity; POC does not merge duplicates, and input_rows = accepted_rows + quarantined_rows. Zero-row quarantine retains its schema.
- [ ] Resolve the local catalog from `/data/marketdata/catalog/duckdb/current.json`, open it read-only, pin its release and dependent Parquet files, and verify calendar ID, CFFEX applicability and coverage. Freeze SQL, parameters, range, release, source file hashes and output hash. For the alternative API path, parse positional rows by columns/types and reject truncated or mixed-release responses.
- [ ] Exercise the observed calendar limitations: CN_STOCK is the only discovered ID and is not implicitly approved for CFFEX; the two physical calendar files contain duplicate date keys across files; query only the catalog-selected file set. Empty trading_sessions / settlement_calendars views do not satisfy session-rule requirements.
- [ ] Local missing/stale release pointers, broken Parquet dependencies, duplicate calendar keys, unknown/unapproved calendar_id, inadequate coverage and missing session rules are exercised with serialized fixtures. For API access also exercise truncation, mixed releases, API error and verified frozen-snapshot fallback. Run real integration for the selected provider; documentation access checks do not count as adapter integration, and skipped cases must be reported.
- [ ] A trading-day list alone cannot assign intraday sessions. Frozen session rules classify boundary times explicitly; ambiguous/out-of-session rows are quarantined, absent rules block successful publication.
- [ ] Consumer fixture reads only the published entry and referenced artifacts; verifies hand-calculated mid, price-unit spread, L1 OBI and session-local intervals. Unknown tick size disables spread_ticks; unknown availability remains research_only. No Step 03 factory is required for this handoff test.
- [ ] Failure before final publication leaves no readable READY entry. Empty input or zero accepted records produces an explicit diagnostic and cannot trigger downstream processing.

## Minimal test fixture

后续 DataSource 管理适配还须验收用户 metadata 的版本/hash、tick 正值/生效范围/冲突检查，确认补充 metadata 不改写冻结行情；成交量未验证与对账不一致仅警告。按实施设计 §6.4 推荐机制派生缺口证据时，需验证隔离事件、未知区间起点和 reset 的 lineage。本段为后续适配要求，不改写已发布 Step 02 的历史验收记录。

S0 + S4 + S5: 2 instruments, irregular updates, one session boundary, one invalid quote.

## Required acceptance checks

- [ ] Enumerate the real L2 input files under `/data/marketdata-backup/futures/treasury_futures/parquet/l2_snapshot/v1/year=*/month=*/*.parquet`, freeze the selected file list and hashes, and reconcile footer/manifest row counts against processed rows. The 2026-11-02 inventory (79 files, 257,407,377 footer rows) is a reference observation, not a hardcoded future acceptance constant or full quality certification.
- [ ] The local sample `/data/marketdata-backup/futures/treasury_futures/parquet/l2_snapshot/v1/year=2026/month=01/part-0.parquet` matches the recorded SHA-256, 3,178,989 rows, 26 row groups and ordered 66-column physical schema (62 uppercase business fields plus four ingestion metadata fields); nullable declarations are distinguished from observed null counts.
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

## 渐进执行与结果消费验收（待实现/验证）

- [ ] Consumer 从现有 onboarding.json 和 manifests 构造 DataSource 引用，不修改既有产物 schema。
- [ ] 同一源版本供多个 feature runs 复用；只改变指标组合或范围不重复接入。超出覆盖明确报告缺口。
- [ ] 已有 RESULTS 的历史通过记录不自动证明新增管理适配已通过。
