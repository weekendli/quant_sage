# Acceptance / Verification — Step 03: Research Dimensions & Primitives

## 当前收敛范围：Step 02–05 数据与特征闭环

本步只按 Run 已解析的 FeatureSet 依赖闭包计算基础序列。输入是 Step 02 的冻结数据源 refs、所选 primitive 定义及输出/历史范围；输出为 Feature(kind=primitive) 对应的 FeatureResult、有效性和 lineage。维度目录是数据可用性说明，不要求自动研究规划、全空间扫描或把每个内部节点都持久化。

L2 公式目录保留完整合法域，实际只算选中项及必要依赖。当前重点为已选 L2 组合需要的快照量、累计量/reset 与时间信息；跨合约因子、activity bars 和自动维度发现列为后续扩展，不作为此最小闭环的交付门槛。

统一管理字段见 [domain.detail.md](../../domain.detail.md)，当前调度边界见 [WORKFLOW_STAGES.md](../../WORKFLOW_STAGES.md)。旧 Registry 名称表示 Feature/FeatureResult 的分类视图，不要求新增独立领域仓库。Study、FeatureSet、Run、Review 的通用管理由 Step 01 承担，领域步骤只实现本步算子与结果契约。


**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## Minimal test fixture

S0 + S5; 8 manually calculable quote snapshots and missing-field variant.

## Required acceptance checks

- [ ] Hand-calculated 8-row fixture matches mid, spread and OBI exactly.
- [ ] Zero denominator for imbalance is null/flag, never +/- infinity.
- [ ] A primitive never reaches into a previous exchange session unless specified.
- [ ] Trade-only input disables LOB dimensions with a reason rather than fabricating data.
- [ ] Append future rows does not alter previously computed prefix primitives.
- [ ] 用户行情 metadata 通过精确版本/hash 提供历史 tick；补充 metadata 不改写冻结行情，依赖身份变化不得误命中缓存。
- [ ] 深档异常不禁用有效 L1 指标；依赖缺失档位或完整五档目标规模的指标正确失效，不跳过未知档凑数。
- [ ] 成交量未验证及对账不一致仅警告；隔离行造成的缺口、原始区间起点与 reset 证据可追溯，不能用保留行顺序冒充源连续性。

## L2 feature-pack decomposition checks

- [ ] Historical TF/TS tick transitions and longest-prefix TL/TF/TS/T parsing match the source spec; unknown mappings yield explicit errors/invalid results.
- [ ] A gapped-price five-level book distinguishes tick-distance space from level count; `d5` is not assumed to contain every level.
- [ ] Unequal bid/ask depth fixtures hand-check all q25/q50/q100 targets and d0/d1/d2/d3/d5 OBI, WOBI, coverage and capped capacity outputs.
- [ ] The F01–F08 snapshot-description catalog has family counts `1,5,5,15,15,3,3,3` (50 names), with shared computation and explicit public/private-node distinction; no horizon expansion occurs here.
- [ ] VWAP consumes only the required size, including partial use of the final level; fill-distance and walk-cost signs agree with F07/F08.
- [ ] Missing/sentinel levels, invalid touch/order, zero denominators and insufficient visible depth have per-primitive masks; no L6 extrapolation or hidden-liquidity assumption.
- [ ] Native TRADEVOLUME is unchanged, cumulative fields retain reset markers even when subsequent values recover, and session boundaries do not borrow prior-session state.
- [ ] Persisted artifacts expose all required Step 04 terminal primitives and lineage without any final horizon factor, Atlas fit or downstream dependency.
- [ ] Input reference `gov_bond_l2_feature_spec.md` remains unchanged by the decomposition/implementation.

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
