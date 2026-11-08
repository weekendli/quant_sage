# 已通过测试与需求的一致性审计

日期：2026-11-08；实现版本：`5c5dd17407448cea5225af9caf48443c713a40e3`。

**结论：现有测试不能整体认定为已经充分验证需求。** 多数引擎、数据接入、管理和恢复断言与其局部需求一致，但领域算法存在漏测、无法区分正确/错误公式的样例，以及需求文档之间的冲突。本次还复现了一处未被白名单测试发现的需求偏差。

最直接的证据：原实现全量测试 **314 passed / 85.84s**；在主测试进程中同时注入“移除 F11、把 F12 有效值改成零、把样本标准差改成总体标准差”后，全量测试仍 **314 passed / 88.92s**。

这说明当前测试对这些错误不敏感，不表示原实现已经犯了上述三个临时注入错误。原始业务代码和测试文件均未修改。

## 1. 范围与方法

- 建立全部 20 个 `tests/test_*.py` 文件、257 个测试函数的断言/异常预期索引，结合实际 pytest collection 核对 314 个参数化后的测试项。
- 对照 Step 01–05 的 SPEC/VERIFY、共同契约、当前领域模型与实施设计，重点复核数值、时间、gap、身份和缓存相关断言。
- 读取 fixture 与 golden 生成方式，区分手算预期、独立参考和基于同一实现生成的预期。
- 用进程内临时替换检查测试是否能识别明确错误；另用正常接口探查 `mid → catch22` 的冻结行为。
- 这是需求与断言的审计，不是行覆盖率或全量 mutation score。文件内通过数量不等于该文件所对应需求的覆盖率。

314 项中既有单元测试，也有序列化集成、CLI、进程恢复和本机数据访问测试，不能把 314 当作 314 项独立业务公式验证。`pytest.raises`、被调用验证器及辅助断言都可能构成有效断言；没有直接 `assert` 不代表空测试。

## 2. 全部测试文件的核对结果

“局部一致”表示已检查断言没有显见需求冲突，不表示整个模块全部需求已被证明。

| 测试文件 | 实际测试项 | 主要验证内容 | 审计判断 |
|---|---:|---|---|
| test_workflow_engine.py | 14 | DAG、缓存、失败、发布、CLI、恢复 | 局部一致；`changed_graph_or_code` 实际只直接测试 graph 变化 |
| test_registry_duckdb.py | 13 | owner、迁移、事务、锁、并发与关闭 | 局部一致，含真实持久化与回滚断言 |
| test_research_batches.py | 21 | 算术批次 A/B/C、选择、缓存、预算 | 局部一致，仅证明泛型算术批次 |
| test_research_management.py | 30 | 源版本、metadata 校验、Study 权限与修订 | 局部一致；不等于领域计算正确 |
| test_feature_sets.py | 20 | 冻结、规范化、版本、依赖与字段 | 局部一致；字段被保存不等于执行层履行该字段语义 |
| test_run_planner.py | 19 | 历史展开、split 权限、预算、幂等 | 当前模型下局部一致；旧 VERIFY 的省略选择规则冲突，见 D1 |
| test_research_execution.py | 18 | 算术数值、DAG、结果发布、恢复、修复 | 局部一致；其合成 provider 不能替代真实 Step 04/05 算子测试 |
| test_research_queries.py | 10 | coverage、health、分页、Review、跨 Run 比较 | 局部一致，能区分全 NA、零行、丢失结果；主要使用算术源 |
| test_research_owner_rpc.py | 8 | socket、CLI、限额、并发查询、SIGKILL 恢复 | 局部一致；主要证明传输与管理闭环 |
| test_onboarding_preflight.py | 6 | inventory、schema、snapshot/split 冻结 | 局部一致 |
| test_onboarding_schema.py | 4 | 字段映射、物理 schema、缺字段 | 局部一致；真实样本测试主要检查 schema，不是全量质量证明 |
| test_onboarding_calendar.py | 13 | 日历、历史 session、到期日边界 | 局部一致；含本机日历集成与模拟 API 解析 |
| test_onboarding_catalog_failures.py | 8 | 缺失/过期 catalog、重复日历、缺 session | 局部一致，使用真实序列化 fixture |
| test_onboarding_stage.py | 7 | 保留/隔离、重复、顺序、质量门控与原子发布 | 局部一致，包含独立 consumer 检查 |
| test_onboarding_consumer.py | 50 | artifact 篡改、原始行身份、质量对账、检查点 | 局部一致，反例与独立验证较完整；不能替代后续窗口 gap 验证 |
| test_onboarding_adapter.py | 6 | 源映射、metadata、gap 事件存储与读取 | 局部一致；gap 能读取不代表下游窗口正确失效 |
| test_l2_primitives.py | 44 | L1/五档手算、掩码、reset、冻结与序列化 | 手算样例有用；身份测试有混杂因素，见 T8；未逐项证明全部 50 个公开名称的真实数值 |
| test_business_windows.py | 5 | 目录计数、OBI 均值、前缀、F09/reset、F10、F12 零分母 | 明显不足，见 T1–T4 |
| test_statistical_windows.py | 8 | mean、常量 std、网格、stale、形态统计与白名单 | 局部断言正确，但缺口机制、数值区分和白名单限制不足，见 T1/T5/T9 |
| test_statistical_backends.py | 10 | 22+8 目录、选择、golden、ARCH、crossing 等 | 部分独立验证有效；目录精确性、ties 与 golden 独立性不足，见 T6/T7 |
| **合计** | **314** | | |

## 3. 发现与需要补强的断言

### T1 · P1：将 stale 测试当成已知源缺口测试

位置：`tests/test_statistical_windows.py:80`、`tests/test_onboarding_adapter.py:304`，以及业务窗口测试文件。

统计测试删除第 5 秒的观察，并将 `max_feature_age` 设置为 0.5s，断言 `STALE_DATA`。该断言符合 age 规则，但没有注入任何 gap manifest/event，无法验证“已知隔离记录即使处于 age 容忍范围内也不能跨越”。adapter 测试只验证事件存在、数量、schema 和证据字段。

需求依据：Step 04 SPEC §窗口有效性、Step 05 SPEC §4、IMPLEMENTATION_DESIGN §6.4。

**补强要求：** 分开测试无更新、超过 age、已知隔离点/区间、普通数值不可估、仅对账警告；将相关 gap 传到实际业务及统计执行链，检查 NA、原因、恢复和真实记录 lineage。原报告 F1 的真实 TS2606 反例必须成为回归断言。

### T2 · P1：历史 fixture 掩盖业务窗口输出起点错误

位置：`tests/test_business_windows.py:44`、`:76`；`tests/test_run_planner.py:155`；`tests/test_statistical_windows.py:197`。

业务 fixture 默认把输出起点设为第一条输入之前 60 秒，容易满足错误的 `t-L >= output_start` 条件。前缀测试只改变未来输入，没有改变输出起点。planner 测试正确证明历史已展开；统计网格测试也正确证明 Step 05 的起点不变性，但两者没有验证实际业务算子如何使用历史。

**补强要求：** 相同授权历史下分别从 session 开始及中途输出，比较四类业务算子的共同时间点；再通过 primitive → business → statistical 的真实管理执行链验证。原报告 F2 的 1/3、4/15 手算值必须直接断言，不能仅比较两组全 NA 是否相等。

### T3 · P2：目录数量正确不代表名称正确

位置：`tests/test_business_windows.py:52`。

测试断言 432 个条目、family counts、48 个 pending、名称唯一；这些断言都可以在 `obi__s0__h1s` 错误名称下通过，与原报告 F3 完全对应。

**补强要求：** 用独立于 `_entry` 的原规格预期集合逐项比较 432 个规范名，检查 d/q/h、合法域和参数顺序。保留数量测试，但不能用它代替名称契约测试。

### T4 · P1：没有验证 F11，F12 只断言零分母结果

位置：`tests/test_business_windows.py:121`。

该测试核对 F10 的一个有效值及 F12 最后一个点的零分母 NA；没有独立调用 F11，也没有断言 F12 的有效非零输出。F12 在较早时刻确实产生一个有效值，但测试没有检查它。

**实测：** 将 `_mid_move_window` 改为被调用就抛异常，业务测试仍 5/5，通过期间该函数调用次数为 0。将 F12 所有有效输出改为零，业务测试仍 5/5，其中已有 1 个有效值经过该错误替换。

需求依据：Step 04 SPEC 的 F11/F12 公式及 VERIFY 的计算顺序与端点要求。

**补强要求：** 加入 F11 的正/负/零位移、tick 与端点 age 样例；F12 覆盖正负 NF、不同 depth/mid/epsilon 的手算非零值，再保留 NF=0 测试。测试必须能识别符号、除数或计算顺序被改错。

### T5 · P1：常量 std 无法区分样本与总体标准差

位置：`tests/test_statistical_windows.py:100`、`:121`。

需求明确为样本 `std(ddof=1)`，当前直接数值断言仅使用常量 5，因此 ddof=0 与 ddof=1 都输出零。17 项基础描述的目录测试只检查 metric 名称；不能证明其他分位数、MAD、IQR、比例的数值定义。

**实测：** 将 std 改成 `statistics.pstdev`，统计窗口测试仍 8/8；三个 std 调用全部通过。

**补强要求：** 使用 `[1,2,3]` 等非恒定样例，样本 std 必须为 1；加入单点、常量、NA、有限性门控。为其余基础描述增加独立手算值，尤其区分 MAD 与缩放 MAD、分位数插值和比例分母。不是要求每个 Run 全量执行 17 项，而是已支持算子应有对应数值断言。

### T6 · P2：精确目录断言不足；golden 不是完全独立的参考链

位置：`tests/test_statistical_backends.py:49`、`:78`、`:95`；`scripts/generate_statistical_goldens.py`。

“exact locked 22 long names” 测试只检查长度、版本字符串和可调用性，未检查唯一性或与规范清单的集合相等。把常量替换成同一个合法名称重复 22 次，这一个具名测试仍通过；这不代表完整 suite 对该替换也全部通过。

golden 文件是有价值的冻结回归基线，但生成器调用的正是生产 `run_catch22_metric`、`run_tsfeature_metrics`。已有 wrapper 的系统性映射/参数错误可能被同步写入预期。当前读取 golden 的测试也没有核对其记录的输入 hash 与顺序绑定；`exact_integer_metrics` 记录了精确比较政策，通用 golden 比较实际仍统一使用 approx。

**补强要求：** 目录与独立清单精确相等并禁止重复；以不经过生产 wrapper 的固定官方后端调用生成参考，审查参数/单位/映射；测试输入 hash、顺序、后端锁和整数精确比较。禁止仅为让测试变绿自动重生成 golden。

已有 ARCH 测试通过 NumPy 手工回归核对 R²，是应保留的独立验证；不能把整个 backend 测试组一概判断为无效。

### T7 · P2：名为 ties 的测试没有中位数相等值

位置：`tests/test_statistical_backends.py:134`。

输入为交替 0/1，中位数是 0.5，没有任何值与中位数相等。它能验证 299 次交叉，但无法证明相等值归侧。

**实测：** 将 crossing 实现替换为错误的 `>= median` 归侧，具名 ties 测试仍通过，记录到的 median ties 为 0。此结论仅针对该具名测试，其他冻结 golden 可能检测相关变化。

**补强要求：** 增加 `[0,1,0,1,0,1,2,2,2]`，中位数为 1；按照相等值归低侧的规则有 1 次交叉，错误的 `>=` 归侧会给出 5 次。保留原交替序列作为普通 crossing 测试。

### T8 · P2：metadata 身份测试同时改变版本，存在混杂

位置：`tests/test_l2_primitives.py:405`。

测试同时把 source version 从 1 改为 2，并新增 market_metadata_ref，最后只断言 hash 不同。即使 hash 实现完全忽略 metadata，也可以因版本不同而通过。

**实测：** 在 `_source_identity_hash` 前删除 `market_metadata_ref`，这个具名测试仍通过。

**补强要求：** 对身份函数单因素变化：只改 metadata hash、只改版本、只改相关源/规则引用，分别检查影响；再加注册与实际缓存复用测试。身份函数层的同版本比较只是隔离影响因素，不允许在 Registry 原地修改已注册版本。当前生产 hash 函数实际包含 metadata，该发现不是声称现行实现漏 hash。

### T9 · P1：白名单测试漏掉需求要求关闭的 mid 形态扩展

位置：`tests/test_statistical_windows.py:219`；实现 `research/statistical_windows.py::INPUT_VIEW_WHITELIST`。

Step 05 SPEC §3 的价格水平行明确写明 `mid` 原值用于原单位描述、形态扩展关闭。测试检查未知视图、单位不匹配及部分方向性限制，却没有检查这个已知合法输入的禁止组合。

**正常接口实测：** 注册真实 L2/统计 catalog 后，`mid → c22.CO_f1ecac` 无额外视图变换或扩展理由即可冻结为 `frozen`；现有白名单测试依然通过。此项是本次新确认的需求偏差，不是人为注入的错误。

**补强要求：** 明确关闭原始价格水平的默认形态组合；若未来允许有审查的扩展，必须有显式视图/配置/身份。测试允许与禁止组合矩阵，并通过 FeatureSet.freeze 及执行验证。成交量 bin 求和、quote-event-index 与 clock-time 的差异也需要独立数值样例；现有算术 provider 的 event-time 测试不能证明真实统计算子实现了这些时间轴。

## 4. D1：需求文档冲突，不能通过改测试掩盖

`docs/steps/01_workflow_engine/VERIFY.md:22` 要求 omitted/empty 都为 NO_WORK；`docs/domain.detail.md:98`、IMPLEMENTATION_DESIGN §6 和 WORKFLOW_STAGES 的当前 Run 契约要求：省略 selected_item_ids 为全选，显式空数组为 NO_WORK。

`test_run_planner.py::test_omitted_selection_means_all_but_empty_selection_is_no_work` 与后者一致。Step 01 SPEC 本身也将新管理路径指向当前实施设计。因此本审计按当前 Run 契约判断该测试正确，标记旧 VERIFY 需同步，不能静默把省略改成 NO_WORK。

泛型旧 batch 的空 targets 规则属于另一个入口，应分别说明，避免同名“空选择”误导。文档冲突消除前，不能宣称所有 SPEC/VERIFY 文本已与测试完全一致。

## 5. 临时替换实验的实际结果

| 场景 | 运行范围 | 结果 | 可证明的局限 |
|---|---|---|---|
| 原始基线 | 全 suite | 314 passed，85.84s，无 skip | 原测试当前可通过 |
| 移除 F11 | 业务窗口文件 | 5 passed | 该组未调用 F11 |
| F12 有效值全改零 | 业务窗口文件 | 5 passed | 有效数值断言缺失 |
| std 改为 ddof=0 | 统计窗口文件 | 8 passed | 常量用例无法区分公式 |
| 上述三项同时注入 | 全 suite | 314 passed，88.92s，无 skip | 主进程的领域数值缺陷没有被整个测试集发现 |
| 同一 catch22 名称重复 22 次 | 精确目录具名测试 | 1 passed | 此测试没有证明 exact names/uniqueness |
| metadata 从身份输入中删除 | metadata 身份具名测试 | 1 passed | 版本变化掩盖 metadata 影响 |
| crossing 错误 ties 归侧 | ties 具名测试 | 1 passed | 此样例没有 ties |
| 正常冻结 mid→catch22 | 现有 whitelist 具名测试 | 1 passed；违规配置可冻结 | 需求组合约束漏测 |

注入只在运行脚本的主 Python 进程内生效；suite 创建的新 CLI/owner 子进程不会自动继承这些替换。因此这不是对每条跨进程路径的全系统变异证明。单项实验与观测计数提供了具体的弱断言证据。

没有重新生成 golden、修改测试预期、安装依赖或改写源数据库。已存在的 F1/F2/F3 失败证据保留不动。

## 6. 复现与整改优先级

[audit_test_strength.py](audit_test_strength.py) 会打印 `AUDIT_RESULT`。带临时错误的 case 返回零意味着所选测试未检测到该错误，不能解释为需求通过。

```bash
# 原始全量基线；RPC 测试需要支持本地 Unix socket 的运行环境。
python -m pytest -q --junitxml=/tmp/quantsage-test-audit-baseline.xml

# 每次命令均是独立进程，临时替换随进程退出而消失。
PYTHONPATH=. python docs/acceptance/2026-11-08/audit_test_strength.py --case missing_f11
PYTHONPATH=. python docs/acceptance/2026-11-08/audit_test_strength.py --case wrong_f12_finite
PYTHONPATH=. python docs/acceptance/2026-11-08/audit_test_strength.py --case std_ddof
PYTHONPATH=. python docs/acceptance/2026-11-08/audit_test_strength.py --case numerical_suite
PYTHONPATH=. python docs/acceptance/2026-11-08/audit_test_strength.py --case duplicate_catch22_names
PYTHONPATH=. python docs/acceptance/2026-11-08/audit_test_strength.py --case metadata_ignored
PYTHONPATH=. python docs/acceptance/2026-11-08/audit_test_strength.py --case wrong_crossing_ties
PYTHONPATH=. python docs/acceptance/2026-11-08/audit_test_strength.py --case mid_shape_freeze
```

建议先补充 T1/T2/T4/T5/T9 的失败回归，再按原 [三项修复方案](REMEDIATION_PLAN.md) 解决 F1/F2/F3，并修正新确认的 mid 组合限制。随后补强 T6–T8、统一 D1 文档口径。

签收应同时满足：正确实现通过；引入相应明确错误时，新回归测试能够失败；真实序列化产物与血缘可核对。测试敏感性验证不能只检查总通过数。

本次只完成审计与证据保存，未修改实现、既有测试或需求文档。审计不能代替尚未实现的 Step 06–12 验收，也不能证明所有合法组合已做真实数据计算。

## 7. 可审阅材料

- [test-audit/inventory.json](test-audit/inventory.json)：20 文件、257 个函数的断言/raises 与需求来源索引，含源码 hash。
- [test-audit/results.json](test-audit/results.json)：基线逐项结果、每文件数量、临时替换观测与日志 hash。
- [test-audit/baseline.xml](test-audit/baseline.xml)：实际 pytest JUnit 记录。
- `test-audit/*.txt`：基线及每次实验的原始输出。

以上索引用于定位证据，不把机械分配的需求来源视为每条需求已经覆盖。
