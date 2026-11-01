# Step 15 — Codex Build Order, Prompts & Acceptance

## 目标

将 00–14 拆成**可独立执行的 Codex 开发任务**。每项必须是可运行垂直切片，而非一堆未连接的空类。

## 推荐里程碑

| Milestone | Codex 实施任务 | 可证明的完成标准 |
|---|---|---|
| M0 · Scaffold | 工程结构、config/schema、CLI、pytest、合成数据生成器 | `pytest` 与 `quantsage --help` 正常 |
| M1 · Engine/Data | 01+02；DAG、manifest、Parquet ingest、质量审计 | 从输入到 Data Passport；缓存与恢复可测 |
| M2 · Understand | 03+04；Dimensions/Primitives、Atlas、Baseline | 真实输出统计画像与条件基准，无未来标签 |
| M3 · Construct | 05；Feature Grammar/AST、compiler、registry | 10+ 有用特征、clock/event windows 数值测试 |
| M4 · Observe | 06+07；事件、状态检测 | 可对合成异常触发 episode，online 无未来数据 |
| M5 · Discover | 08+09；现象发现、六种生成器 | 结构化 Phenomenon/Hypothesis Card，能生成竞争解释 |
| M6 · Experiment | 10+11；Cube、对照、验证、ledger | 有效果/区间/样本数/多重检验、holdout 门控 |
| M7 · Loop | 12+13；研究规划与端到端报告 | 单次 run 生成全血缘、下一实验和复现报告 |

**原则：先跑通 M0–M2；不要一开始把 M0–M7 全做完再测试。**

## 每个 Codex Task 的固定指令

可复制以下提示并替换 `<STEP_FILE>`：

> 阅读 `ARCHITECTURE.md`、`COMMON_CONTRACTS.md`、`WORKFLOW_STAGES.md` 和 `<STEP_FILE>`。仅实现本 Step 声明的能力及其必需依赖，以最小可运行增量提交。用真实 Polars/DuckDB/Arrow 操作 Parquet，禁止用未实现 TODO/stub 替代核心逻辑。新增单元测试、至少一个合成数据集端到端/集成测试，并验证 point-in-time 语义。运行 pytest 和 CLI smoke test，列出变更、测试结果、限制，并更新 IMPLEMENTATION_STATUS.md。

## 独立任务列表（适合逐个交给 Codex）

1. **Task 01 — Engine**：执行 `steps/01_workflow_engine/SPEC.md`；交付 stage plugin interface、DAG/toposort、cache、manifest、resume、CLI。
2. **Task 02 — Intake**：执行 `steps/02_data_onboarding/SPEC.md`；交付 schema adapter、quality、session parser、Parquet 输出。
3. **Task 03 — Primitives**：执行 `steps/03_research_dimensions_and_primitives/SPEC.md`；自动推导可用研究维度，计算 mid/spread/OBI/update-intensity。
4. **Task 04 — Atlas**：执行 `steps/04_statistical_atlas_and_baselines/SPEC.md`；条件基准、统计指纹、跨时段/合约依赖。
5. **Task 05 — Grammar**：执行 `steps/05_feature_grammar/SPEC.md`；typed AST、time-aware operators、registry、lazy execution。
6. **Task 06 — Events**：执行 `steps/06_event_detector/SPEC.md`；极端/背离、事件片段、online detected time。
7. **Task 07 — Regimes**：执行 `steps/07_regime_detector/SPEC.md`；L1 状态，L2 optional adapter，过滤式状态时序。
8. **Task 08 — Phenomena**：执行 `steps/08_phenomenon_discovery/SPEC.md`；候选现象、对照、证据与去重。
9. **Task 09 — Hypotheses**：执行 `steps/09_hypothesis_generation/SPEC.md`；六 generator、结构化卡片、反证规则。
10. **Task 10 — Cube**：执行 `steps/10_discovery_cube/SPEC.md`；F×C×H cells、正确 horizon labels、预算化搜索。
11. **Task 11 — Validate**：执行 `steps/11_falsification_and_validation/SPEC.md`；split/embargo、区块检验、多重测试、独立验证。
12. **Task 12 — Planner**：执行 `steps/12_research_planner_and_knowledge/SPEC.md`；下一实验、知识注册表、失败留存。
13. **Task 13 — Integration**：按 `WORKFLOW_STAGES.md` 打通模式、报告、可恢复完整运行。

## 必须有的合成测试场景

`S0`: 单一合约不规则更新（时间间隔不固定）。  
`S1`: 无信号但有强日内季节性与自相关。  
`S2`: 高活动时段的深度冲击和无价格响应。  
`S3`: 强价格冲击但部分反转，注入相反的条件机制。  
`S4`: 跨合约异步报价+stale quote。  
`S5`: 交易日边界、停盘、累计成交量重置、合约切换。  
`S6`: 训练期前缀固定，未来追加信息；历史特征/状态/事件不能变化。  
`S7`: 多重检验：1000 个无信号候选不应被当作确认规律。  
`S8`: final holdout 被修改时，上游选择/阈值/候选不变化。

## 最小演示（M7）

命令：

```bash
quantsage validate --config examples/quant_discovery.yaml
quantsage plan --config examples/quant_discovery.yaml
quantsage run --config examples/quant_discovery.yaml --mode observatory
quantsage run --config examples/quant_discovery.yaml --mode validation
quantsage report --run-id <run_id>
```

展示：Data Passport → Primitives → Atlas → `bid_depth_depletion_30s` Feature → Liquidity Shock Event → Low Liquidity Regime → Depth-price Divergence Phenomenon → H1/H2 → 6 个示意 Cells → Falsification Report → Next Experiment。**上面是测试路径，不得把假定结果写成真实验证结论**。

## Definition of Done

- 文档中的全部核心输出具有实际的序列化产物；可对任意 artifact 查询 lineage。
- 在测试合成数据上跑通完整 DAG（包括没有显著规律时正确输出空或 INCONCLUSIVE）。
- 代码具备细粒度的 pytest（单元/集成/泄漏/性质测试），有简洁 README 和 CLI 使用示例。
- 不要求 CI 真跑 30GB，但至少提供基准脚本与内存预算说明，证明采用可扩展读取/分区策略。
- 所有统计结论按数据、split、sample unit 和多重搜索范围报告；区分“观察事实”“假设”“独立验证”“交易价值”。
- `IMPLEMENTATION_STATUS.md` 准确记录实际完成情况，不能直接将本设计文档视作已实现软件。
