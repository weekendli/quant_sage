# Acceptance / Verification — Step 05: Statistical Features & Quality

## 当前收敛范围：Step 02–05 数据与特征闭环

本步消费选定的 Step 04 业务结果或 Step 03 primitive 结果，按 FeatureSet 的 input + W + sampling + metrics 生成 Feature(kind=statistical) 与 FeatureResult。只依赖实际选中输入；primitive-only 配置不要求先运行 Step 04。保留 catch22 22 项及精选 tsfeatures 8 项的完整定义/对照目录，每次仅执行被明确选择的合法统计量。

当前必需交付为统计数值、采样/时间语义、质量/coverage 和可复用结果索引。完整 Atlas、条件正常分布拟合、异常分数、依赖图、null/control、事件/状态发现均为后续扩展，不作为本步核心验收前置；详细数学资料保留供显式扩展使用。

统一管理字段见 [domain.detail.md](../../domain.detail.md)，当前调度边界见 [WORKFLOW_STAGES.md](../../WORKFLOW_STAGES.md)。旧 Registry 名称表示 Feature/FeatureResult 的分类视图，不要求新增独立领域仓库。Study、FeatureSet、Run、Review 的通用管理由 Step 01 承担，领域步骤只实现本步算子与结果契约。


**Important:** These are **tests to create and run**, not claims that tests or code already exist.

## Minimal test fixture

S1 + S4; seasonal null data with injected shocks and asynchronous instruments.

## Required acceptance checks

- [ ] A small frozen FeatureSet computes only selected metrics and dependencies; catalog support for 22+8 does not cause unselected computations.
- [ ] Primitive-only input executes without Step 04; selected business input consumes verified Step 04 artifacts.
- [ ] Finite/constant/short/NA inputs produce correct values or explicit reasons; units and valid counts are preserved.
- [ ] Conditional baseline, anomaly, Atlas and null/control artifacts are not mandatory and are not fabricated.

## Statistical feature-set acceptance checks

- [ ] catch22 以 0.4.5 long-name 清单精确匹配 22 项；不依赖 short names 或列表顺序。
- [ ] 8 项 tsfeatures 候选的参数、适用输入和对应官方输出有显式映射；Python 移植与固定版本 R fixtures 数值对照。
- [ ] 对数值稳定性、极短/常量/近常量、重尾、NA、stale、reset 和异步时间建立 fixtures；不可估状态不得被 masking 掩盖。
- [ ] 不跨午休、隔夜、真实合约或 NA 缺口错误连接序列；split 按读/fit 权限校验，评价窗口可读取已授权的过去历史；事件轴与 clock 轴在可控例子中产生不同且正确的解释。
- [ ] ARCH 返回 R²；谱熵算法、crossing ties、stability/lumpiness 的缩放/分块语义与参考实现一致。
- [ ] 用序列化 Step 04 business-feature artifacts 和 Step 03 primitive fixtures 可独立验证本步骤；实际选定业务序列进入计算，但不强制全量 432 × metrics 展开。
- [ ] trailing 模式记录 L、W、stride、采样间隔和底层依赖；重叠窗口不当作独立样本，所有数值输出严格在依赖可用后发布。
- [ ] 非法 space/size/metric 组合不生成；跳过原因、候选数、实际运行数和耗时可查。
- [ ] 添加未来数据不改变已结束窗口的结果；输入选择不得通过 holdout 调整。
- [ ] 默认 trailing 的 30m 窗口中间缺 1s 时输出 NA，恢复完整有效历史后恢复；显式子段画像使用独立 ID/范围，不冒充完整 trailing 值。
- [ ] 修改 Run 起点不改变 session 对齐的评估网格及共同时间点的值；结果按真实合约/交易日分区。
- [ ] 成交量未验证/对账不一致只传递警告；真实缺口、reset 和非法数值仍按有效性处理。
- [ ] 交付包含精确固定的 catch22/tsfeatures 版本与源码/依赖锁，禁止自动升级。
- [ ] session 摘要不会被标成 session 开始时可用；没有可用时间证据时保留研究用途标记。
- [ ] 合成敏感性和真实数据覆盖率各自记录，不将合成测试等同于市场发现；完成相应验证后再更新实施状态。
- [ ] The input whitelist, metric versions/parameters and output fields are resolved directly from SPEC.md; no external reference is required to determine a formula or validity policy.
- [ ] Business features with pending semantics are excluded with explicit reasons; segment summaries cannot be used before their end/availability time.
- [ ] Persisted original-unit input refs and statistical vectors are jointly consumable by Step 06/07 without future impact labels or reverse dependencies into Step 04.

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
