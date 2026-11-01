# Step Index — Codex 单步实现与验证

每个 Step 有三个**独立的 Markdown 文件**：`SPEC.md`（技术设计）、`IMPLEMENT.md`（Codex 工作指令）、`VERIFY.md`（单模块验收）。

规则：仅在当前模块验收完成后推进下一模块。可以用符合契约的 fixture artifacts 独立开发未完成的下游模块，但不能把 fixture 测试当作全链路真实集成。

| Step | 模块 | 里程碑 | 上游依赖 | 实现入口 | 验证入口 | 规格 |
|---|---|---|---|---|---|---|
| 01 | Workflow Engine | M1 | — | [IMPLEMENT](steps/01_workflow_engine/IMPLEMENT.md) | [VERIFY](steps/01_workflow_engine/VERIFY.md) | [SPEC](steps/01_workflow_engine/SPEC.md) |
| 02 | Data Onboarding & Quality | M1 | 01 | [IMPLEMENT](steps/02_data_onboarding/IMPLEMENT.md) | [VERIFY](steps/02_data_onboarding/VERIFY.md) | [SPEC](steps/02_data_onboarding/SPEC.md) |
| 03 | Research Dimensions & Primitives | M2 | 02 | [IMPLEMENT](steps/03_research_dimensions_and_primitives/IMPLEMENT.md) | [VERIFY](steps/03_research_dimensions_and_primitives/VERIFY.md) | [SPEC](steps/03_research_dimensions_and_primitives/SPEC.md) |
| 04 | Statistical Atlas & Baselines | M2 | 03 | [IMPLEMENT](steps/04_statistical_atlas_and_baselines/IMPLEMENT.md) | [VERIFY](steps/04_statistical_atlas_and_baselines/VERIFY.md) | [SPEC](steps/04_statistical_atlas_and_baselines/SPEC.md) |
| 05 | Feature Grammar | M3 | 03, 04 | [IMPLEMENT](steps/05_feature_grammar/IMPLEMENT.md) | [VERIFY](steps/05_feature_grammar/VERIFY.md) | [SPEC](steps/05_feature_grammar/SPEC.md) |
| 06 | Event Detector | M4 | 04, 05 | [IMPLEMENT](steps/06_event_detector/IMPLEMENT.md) | [VERIFY](steps/06_event_detector/VERIFY.md) | [SPEC](steps/06_event_detector/SPEC.md) |
| 07 | Regime Detector | M4 | 04, 05 | [IMPLEMENT](steps/07_regime_detector/IMPLEMENT.md) | [VERIFY](steps/07_regime_detector/VERIFY.md) | [SPEC](steps/07_regime_detector/SPEC.md) |
| 08 | Phenomenon Discovery | M5 | 04, 05, 06, 07 | [IMPLEMENT](steps/08_phenomenon_discovery/IMPLEMENT.md) | [VERIFY](steps/08_phenomenon_discovery/VERIFY.md) | [SPEC](steps/08_phenomenon_discovery/SPEC.md) |
| 09 | Hypothesis Generation | M5 | 08 | [IMPLEMENT](steps/09_hypothesis_generation/IMPLEMENT.md) | [VERIFY](steps/09_hypothesis_generation/VERIFY.md) | [SPEC](steps/09_hypothesis_generation/SPEC.md) |
| 10 | Discovery Cube | M6 | 05, 07, 09 | [IMPLEMENT](steps/10_discovery_cube/IMPLEMENT.md) | [VERIFY](steps/10_discovery_cube/VERIFY.md) | [SPEC](steps/10_discovery_cube/SPEC.md) |
| 11 | Falsification & Validation | M6 | 10 | [IMPLEMENT](steps/11_falsification_and_validation/IMPLEMENT.md) | [VERIFY](steps/11_falsification_and_validation/VERIFY.md) | [SPEC](steps/11_falsification_and_validation/SPEC.md) |
| 12 | Research Planner & Knowledge | M7 | 08, 09, 11 | [IMPLEMENT](steps/12_research_planner_and_knowledge/IMPLEMENT.md) | [VERIFY](steps/12_research_planner_and_knowledge/VERIFY.md) | [SPEC](steps/12_research_planner_and_knowledge/SPEC.md) |
| 13 | End-to-End Integration | M7 | 01, 02, 03, 04, 05, 06, 07, 08, 09, 10, 11, 12 | [IMPLEMENT](steps/13_end_to_end_integration/IMPLEMENT.md) | [VERIFY](steps/13_end_to_end_integration/VERIFY.md) | [SPEC](steps/13_end_to_end_integration/SPEC.md) |
