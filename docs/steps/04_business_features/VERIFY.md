# Acceptance / Verification — Step 04: Business Features & Time Windows

## 当前收敛范围：Step 02–05 数据与特征闭环

本步消费 Step 03 结果和冻结 FeatureSet 中选中的业务组合，生成 Feature(kind=business) 与 FeatureResult。只实现确定业务公式、合法 space/size/回看 L、窗口端点/reset/有效性；不要求自动创造公式或运行全部 432 个组合。

432 是目录合法名称数，不是每次必须计算的数量。未选中的 pending_semantics 不阻塞已选 ready 子集；选中的未明确公式阻断对应任务并报告原因，不改变公式。只有声称“完整 pack 数值交付”时才要求所有相关语义明确。

统一管理字段见 [domain.detail.md](../../domain.detail.md)，当前调度边界见 [WORKFLOW_STAGES.md](../../WORKFLOW_STAGES.md)。旧 Registry 名称表示 Feature/FeatureResult 的分类视图，不要求新增独立领域仓库。Study、FeatureSet、Run、Review 的通用管理由 Step 01 承担，领域步骤只实现本步算子与结果契约。


**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## Minimal test fixture

S0 + S6; compare explicit hand-computed rolling-window values and prefix invariance.

## Required acceptance checks

- [ ] Equivalent ASTs canonicalize to same feature ID; unequal units are rejected.
- [ ] Rolling clock 30s differs correctly from rolling 30 updates in irregular fixture.
- [ ] Every computed feature depends only on rows available at decision time.
- [ ] Window never crosses session boundary silently.
- [ ] Business pack expansion respects family domains and configured budgets, logs skipped items, and runs without Step 05 Atlas/baseline artifacts.

## L2 feature-pack decomposition checks

- [ ] The full definition registry has counts `8,40,40,120,120,24,24,24,8,8,8,8` for F01–F12, totaling 432 unique legal names; no spread d/q or execution-cost d combinations are generated.
- [ ] Decomposition counts reconcile as 352 specified snapshot means, 48 F07/F08 combinations pending aggregation semantics, and 32 window-result composites; no uniform rolling-mean template is applied to F09–F12.
- [ ] Explicit subsets, pre-submit BUDGET_EXCEEDED, missing prerequisites and pending semantics have distinct status/reasons; no budget-driven partial submission; runtime failures retain partial results. Definition count is not reported as computed valid-value count.
- [ ] Varying-depth irregular fixtures distinguish mean-of-ratios (F02/F03/F05) from aggregate ratios and verify snapshot-specific target sizes for F04–F08.
- [ ] F06 averages only jointly fillable snapshots and reports valid count/ratio; empty windows and insufficient session history remain NA.
- [ ] F09 uses cumulative endpoint differences; F10 uses summed native interval volume divided by mean reference depth; F11 uses past mid movement; F12 combines window quantities in the specified order with `NF=0` NA.
- [ ] Mid-window resets invalidate flow windows even if endpoint values recovered; session start, lunch boundary, exactly-at-window-start timestamps and unavailable/stale endpoint cases have explicit fixtures.
- [ ] F09/F11/F12 match the latest valid endpoint at `<= t-L`, including equal-time observations; missing max_endpoint_age is rejected and exceeded age yields NA.
- [ ] Volume-unverified/reconciliation warnings alone do not invalidate values; known relevant gaps yield NA until complete L history recovers. Source-interval lineage and quarantine/reset evidence are preserved.
- [ ] F07/F08 never silently acquire a rolling-mean definition. Frozen aggregation and endpoint policies are required before affected values are published; unresolved policies prevent full-pack acceptance.
- [ ] Serialized PrimitiveRegistry fixtures permit Step 04 testing before upstream implementation; real integration checks equivalent values, masks, parameter meaning and availability lineage when Step 03 is runnable.
- [ ] The fixed pack does not change formulas/windows based on Atlas recommendations; the source reference remains unchanged and no future labels are required.

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

- [ ] FeatureSet 选择与 Run 解析结果一致，只计算所选具体定义与授权依赖；未选项不进入任务总数。
- [ ] 扩大日期只补缺失精确分区；增加指标复用匹配上游；改变定义/源/fit 不错误复用。
- [ ] 缺依赖且禁止补跑时 BLOCKED，失败可同配置 resume，attempt 历史保留。
- [ ] 输出 FeatureResult 包含精确定义、源/输入 refs、scope、artifact/hash、可用时间、处理覆盖和 n_total/n_valid。
- [ ] 未执行、失败、结果丢失/损坏、全 NA、部分覆盖分别可查；空选择为 NO_WORK。
- [ ] 下游只通过结果 refs 消费，不依赖本 Run 的临时目录；原始 L2 公式不变。
