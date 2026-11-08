# QuantSage 验收缺陷修复复验

日期：2026-11-08。基线代码：`5c5dd17407448cea5225af9caf48443c713a40e3` 加本次工作树补丁。环境：Python 3.12.3、DuckDB 1.5.5、PyArrow 25.0.1、Ruff 0.16.1、固定 R/tsfeatures 后端。

**结论：F1、F2、F3、T9 及本次 T1–T9 测试补强已完成复验。完整 pytest 为 344 passed；针对性测试 144 passed；实现相关 Ruff 通过；错误替换均返回非零并使对应新增回归失败；隔离真实 Run、Review、A/B/C、NO_WORK 与 pending 语义通过。**

本报告是新增修复证据。原 [REPORT.md](REPORT.md)、[REMEDIATION_PLAN.md](REMEDIATION_PLAN.md)、[TEST_REQUIREMENTS_AUDIT.md](TEST_REQUIREMENTS_AUDIT.md)、历史 JSON/XML/文本结果均未覆盖。为适应当前公共窗口上下文并补强替换实验，仅更新了可执行入口 [probe_semantics.py](probe_semantics.py) 和 [audit_test_strength.py](audit_test_strength.py)，未改写原反例输入、目标时刻或历史输出。

## 1. 实际修改与需求对应

| 需求 | 实际修改 | 复验证据 |
|---|---|---|
| F1 缺口传播 | 新增 `gap_policy_v1` 共享上下文；按 affected fields、合约、交易日、session 和时间边界匹配隔离事件；业务窗口与 primitive-only/trailing 统计窗口输出 `KNOWN_SOURCE_GAP`；统计窗口继承父业务 audit 的缺口事件与源记录；审计新增 gap event refs、evidence record refs、manifest URI/hash 和 policy version | TS2606 真实反例、点/区间/空 session/字段隔离/普通无更新/stale/警告/F06 不可估相关测试；业务与统计 audit 序列化断言 |
| F2 历史边界 | 业务窗口显式要求 `session_start` 与 `authorized_start`；输出范围仅过滤发布时刻，不再替代历史边界；F02/F09/F10/F11 四类算子共同时间点不变；滚动求和改为窗口内 `math.fsum`，避免此前 prefix 求和的跨范围浮点抵消 | 1/3、4/15 手算反例；四类窗口宽/窄范围共同值测试；真实宽/窄 Run 共同时刻一致；无授权历史仍 NA |
| F3 名称 | `_entry` 显式使用 `d0/d1/d2/d3/d5`；432 名称与测试内独立规范集合逐项相等；新增 320 个历史 `__s` 名称到规范 `__d` 名称的一对一可追溯映射，新目录不输出旧名 | 完整 432 项集合、family counts、48 pending、唯一性、首项 `obi__d0__h1s`、参数顺序和 legacy alias 测试 |
| T9 mid 限制 | raw `mid` 白名单只允许原单位描述；catch22/tsfeatures 等默认形态扩展在 FeatureSet freeze 和执行边界均被拒绝；合法 `mean` 仍可执行 | freeze 拒绝/接受矩阵、compute 边界拒绝、合法 mid 原单位执行回归 |
| T1–T8 测试补强 | 缺口/stale/warning/无更新分测；F11/F12 非零手算；样本 `std(ddof=1)`；真实 median ties；catch22/tsfeatures 精确目录、golden 输入 hash/order/lock 和独立后端调用；metadata 单因素身份；新旧 implementation cache 隔离 | 见第 3、4 节 |
| D1 文档冲突 | 当前 Run 契约明确为：省略 `selected_item_ids` 全选，显式空数组 `NO_WORK`；legacy `research_batch_v1` 的空 target 规则单列，不混同 | 更新 Step 01 `IMPLEMENT.md`、`VERIFY.md`；真实 empty plan 为 `NO_WORK`，0 work units |

### F1 的原因与血缘语义

实现区分以下状态，而不是以一个“有缺口”标志代替：

- 完整历史不足：`HISTORY_INSUFFICIENT`。
- 相关已知源缺口：`KNOWN_SOURCE_GAP`，附 gap event ID、quarantined evidence record ID、gap manifest URI/hash。
- primitive/业务输入本身不可估：`NO_VALID_OBSERVATIONS` 或公式原因。
- trailing sample 超龄：`STALE_DATA`。
- 普通无更新：在年龄/覆盖规则允许时保持数值，不自动升级为源缺口。
- 成交量未验证或对账警告：保留数值并传播 warning flag。

点缺口按 `(t-L,t]` 判定；区间缺口按冻结 boundary 语义判定；累计量/mid 端点公式检查左端点证据；非相关字段、其他合约、其他日期或其他 session 不传播。统计 trailing 只要所需父样本遇到相关缺口或父 audit 的 `KNOWN_SOURCE_GAP`，完整 W 结果即为 NA；退出 W 并重新满足上游 L 后恢复。

## 2. 版本、定义、schema 与缓存

| 对象 | 新身份 |
|---|---|
| Gap policy | `gap_policy_v1` |
| Business provider | `quantsage_business_windows@2.0.0` / `business_windows_v2` |
| Business audit | `business_window_audit_v2` |
| Statistical provider | `quantsage_statistical_windows@2.0.0` / `statistical_windows_v4` |
| Statistical audit | `statistical_window_audit_v2` |
| Input view policy | `step05_input_whitelist@2` |
| Computation identity | `feature_computation_identity_v2`，显式包含 gap policy、gap manifest ref、source hash、read scope 和 implementation identity |

业务/统计 catalog 和 implementation version 均提升，旧错误定义不能命中新缓存。统计定义同时绑定新业务 Feature ref 和 audit schema，下游统计缓存身份随之变化。新增测试 `test_new_implementation_identity_does_not_reuse_old_cache_and_reuses_its_own` 验证旧 implementation 结果不复用、新 implementation 首次执行、再次执行命中新缓存。

历史研究数据库、旧业务/统计结果和原 `business_window_audit_v1` / `statistical_window_audit_v1` 产物未原地修改。真实复验使用全新 DuckDB 和 artifact 根目录；源行情内容 hash 保持 `sha256:73acc246fc198e1a32f3184a445e075af347954cba5f98769e8322e51299a949`。

真实缓存隔离另将原独立验收库复制到 `/tmp/quantsage-remediation-cache-20261108`，原库未改。复制库保留 14 个旧结果，并注册与旧 `obi_30s` 相同源、范围和公式的新版定义：

- 首次 Run `run_9c0b90bb13fa415196e0027e28c75370`：business `cache_candidate=false`、state `SUCCEEDED`，新 result `result_30dcab4b446a7ae1dbe3be35926ac24f`。
- 第二次 Run `run_d1313dd90be5414b954defcf8eedf0f6`：business `cache_candidate=true`、state `CACHED`、`cache_source_run_id=run_9c0b90bb13fa415196e0027e28c75370`，复用同一新 result。

这同时验证旧错误结果不能命中新定义，以及新版重复运行能稳定复用。复验入口：[verify_cache_isolation.py](verify_cache_isolation.py)；报告：`/tmp/quantsage-remediation-cache-20261108/cache-isolation.json`。

## 3. 针对性测试与完整验证

实际命令与结果：

```bash
/data/src/research/.venv/bin/python -m pytest -q \
  tests/test_business_windows.py tests/test_statistical_windows.py \
  tests/test_statistical_backends.py tests/test_l2_primitives.py \
  tests/test_run_planner.py tests/test_research_execution.py \
  tests/test_research_queries.py tests/test_acceptance_remediation.py
# 144 passed in 32.48s

/data/src/research/.venv/bin/python -m pytest -q \
  -o cache_dir=/tmp/quantsage-remediation-pytest-cache
# 344 passed in 87.53s

/data/src/research/.venv/bin/ruff check --no-cache \
  quantsage_discovery tests examples scripts \
  docs/acceptance/2026-11-08/audit_test_strength.py \
  docs/acceptance/2026-11-08/probe_semantics.py \
  docs/acceptance/2026-11-08/verify_remediation.py
# passed

PYTHONPATH=. /data/src/research/.venv/bin/python \
  docs/acceptance/2026-11-08/probe_semantics.py
# history.passed=true, real_gap.passed=true, catalog.passed=true
```

完整 pytest 在允许本地 Unix socket 的权限环境中运行；普通受限沙箱中 socket 测试会因 `Operation not permitted` 失败，该环境差异不作为实现失败。本项目没有新增 skip 来代替验收。

关键独立断言的来源：

- F02：窗口 `(t-1m,t]` 在 60s 为 `(0.0+0.4+0.6)/3=1/3`，在 80s 为 `(0.4+0.6-0.2)/3=4/15`；不调用实现生成预期。
- F11：`(102-100)/0.25=8 ticks`，负位移对照另行检查。
- F12：正净流/正价格位移 `8/(10/25)=20`；负净流/正价格位移 `-4/(10/25)=-10`；显式 epsilon=1 时正例为 `8/(10/25+1)=40/7`。
- 样本标准差：`std([1,2,3], ddof=1)=1`，总体标准差为 `sqrt(2/3)`，测试可区分。
- Crossing ties：官方后端对 `[0,1,0,1,0,1,2,2,2]` 的中位数是 1，按相等值归低侧得到 1 次 crossing；错误 `>=` 归侧为 5。
- MAD 为未缩放 `median(abs(x-median(x)))`；分位数使用固定线性插值 `(n-1)p`；ratio 分母逐项手算。

## 4. 错误替换检测

每个 case 在独立主进程中注入错误后运行对应新增测试。所有 case 均返回退出码 1，而不是沿用原始“错误注入仍全绿”的结果。替换不跨越独立 CLI/RPC 子进程；本表只声明所选主进程回归的敏感性。

| case | 注入错误 | 结果 |
|---|---|---|
| `missing_f11` | F11 调用即抛错 | exit 1；F11/F12 手算测试失败，观测 F11 调用 |
| `wrong_f12_finite` | 所有有限 F12 值改零 | exit 1；有限 F12 非零断言失败 |
| `std_ddof` | 样本 std 改总体 std | exit 1；两个 std 回归失败 |
| `duplicate_catch22_names` | 22 个目录名替换为同一名称 | exit 1；精确目录测试失败，唯一名称数 1 |
| `metadata_ignored` | 身份 hash 忽略 market metadata | exit 1；单因素身份测试失败 |
| `wrong_crossing_ties` | crossing 改用 `>= median` 归侧 | exit 1；真实 ties 生产窗口测试失败 |
| `mid_shape_freeze` | 绕过 freeze 的统计输入验证 | exit 1；raw mid 形态组合未被拒绝 |
| `gap_propagation_removed` | 业务和统计缺口匹配都返回空 | exit 1；3 个业务/统计/真实缺口测试失败 |
| `output_start_as_history` | 恢复旧的“输出起点=历史起点”判断 | exit 1；F2 输出起点不变性测试失败 |
| `wrong_space_prefix` | 恢复 `__s` 前缀 | exit 1；432 规范名称测试失败 |
| `numerical_suite` | 同时注入 F11、F12、std、gap、history 错误后跑全 suite | exit 1；9 failed / 335 passed，失败项均为目标数值、缺口或历史边界回归 |

原始实现的全 suite 为 344 passed；对应错误注入后新增回归失败。命令输出保存在 `/tmp/quantsage-remediation-prefix/mutation-*.txt`。

## 5. 真实隔离 Run 与反例

复验脚本：[verify_remediation.py](verify_remediation.py)。工作区：`/tmp/quantsage-remediation-20261108-v2`。创建了只改变 source id/version 和 manifest identity 的别名 `remediation_cffex_full79@1`；底层行情和 gap 文件仍引用冻结源，未改写原源文件或原研究库。

| Run | ID | 结果 / 用途 |
|---|---|---|
| F1 宽范围 | `run_635ace3d11df4da0aad6e276e8dccb01` | COMPLETE；OBI 业务 result `result_3612ee2552423128eae08ba86160bc11`，9 行，3 valid，5 history NA，1 known gap NA |
| F2 窄范围 | `run_60eb1d5c9d2a46248cec398514485bfa` | COMPLETE；result `result_b284b966e323e21c6ac4aec246f69654`，2 行；与宽范围两个共同时刻值一致 |
| F1 统计下游 | `run_d3fd495cf0ba42f7931e9e771621499d` | COMPLETE；result `result_a826eb36e93a19fa3515ab041d3246fe`，44 行；29 行继承 `KNOWN_SOURCE_GAP` |

真实 TS2606 / 2026-06-12 反例：

- 目标时刻：`2026-06-12T01:34:11.800000+00:00`。
- 输出：`value=null`、`invalid_reason=KNOWN_SOURCE_GAP`。
- 缺口事件：`767c1fecbf12259a834995654f3bb6753b04e8d5806f261a40824530a4c01592`。
- 隔离源记录：`5a17031a699f184f651d4eafcefdd65f58329739c48fd2eaa55b07164b5ad2f5`。
- Gap manifest hash：`sha256:1f9f7105ab2ab0e807f55d40d0dc65487af6a360970e4c9295cf985c02cb8333`。
- 正常对照 `01:31:24.800`：value `0.0`，无 invalid reason。
- 完整 L 后恢复 `01:36:17.300`：value `-0.7142857142857143`，无 invalid reason。
- 目标行不是零行、不是全 NA 假通过：同一次结果中 3 个非空正常/恢复值，目标 primitive 输入非空且隔离事件可追溯。

真实 F2 宽/窄 Run 的共同行：

```text
2026-06-12T01:36:17.300000+00:00  -0.7142857142857143
2026-06-12T01:36:24.300000+00:00  -0.7142857142857143
```

窄 Run 只发布从 `01:36:00` 起的两行，但读取 planner 展开的授权历史，值不再因输出起点变为 `HISTORY_INSUFFICIENT`。

可审阅机器报告：`/tmp/quantsage-remediation-20261108-v2/remediation-runs.json`。临时目录不是长期存储承诺；上述摘要和持久化报告保留关键引用。

实际命令：

```bash
PYTHONPATH=. /data/src/research/.venv/bin/python \
  docs/acceptance/2026-11-08/verify_remediation.py \
  --workspace /tmp/quantsage-remediation-20261108-v2 \
  --report /tmp/quantsage-remediation-20261108-v2/remediation-runs.json

PYTHONPATH=. /data/src/research/.venv/bin/python \
  docs/acceptance/2026-11-08/verify_cache_isolation.py \
  --old-database /tmp/quantsage-acceptance-20261108/registry.duckdb \
  --workspace /tmp/quantsage-remediation-cache-20261108 \
  --report /tmp/quantsage-remediation-cache-20261108/cache-isolation.json
```

## 6. 更新后的真实 A→Review→B→C

使用同一隔离库与新版 catalog/FeatureSet 执行 [research_end_to_end.py](../../../examples/research_end_to_end.py)：

| 对象 | ID / 实测 |
|---|---|
| Study | `real_abc_study@revision 2` |
| Review | `remediation-20261108-review-a` |
| A | `run_979612f3321f4f7da50f0eb4aedcb9c6`，COMPLETE，6 work units |
| B | `run_2a73644414724e1188ebc02e46bd9060`，COMPLETE，CACHED=4、SUCCEEDED=6、SKIPPED=2 |
| C | `run_926c31dd06f840ec9dfdb8b1482d6b7a`，COMPLETE，CACHED=8、SUCCEEDED=2、SKIPPED=4 |
| pending F07 | `run_afaef3f6d2144c4d8ff06d3f2dc08081`，BLOCKED / `PENDING_SEMANTICS` |
| explicit empty selection | plan `NO_WORK`，0 work units |

B 复用 A 的 requested results，C 只新增一个 Feature definition 并复用 B 的 8 个匹配结果；Review、coverage、B/C comparison 和 pending/no-work 均可查询。报告：`/tmp/quantsage-remediation-20261108-v2/real-abc.json`。

实际命令：

```bash
/data/src/research/.venv/bin/python -m quantsage_discovery.cli research owner serve \
  --database /tmp/quantsage-remediation-20261108-v2/research.duckdb \
  --socket /tmp/quantsage-remediation-20261108-v2/rpc/owner.sock

/data/src/research/.venv/bin/python -m examples.research_end_to_end \
  --database /tmp/quantsage-remediation-20261108-v2/research.duckdb \
  --socket /tmp/quantsage-remediation-20261108-v2/rpc/owner.sock \
  --source-ref remediation_cffex_full79@1 \
  --run-prefix remediation-20261108 \
  --report /tmp/quantsage-remediation-20261108-v2/real-abc.json
```

owner 在复验后已停止，未保留后台进程。真实命令因沙箱要求使用经批准的本机 Unix socket 权限。

## 7. 完整目录与 mid 限制

- `business_catalog_entries()` 与测试内独立规范生成器逐项比较，集合恰好 432 项；family counts 为 `8,40,40,120,120,24,24,24,8,8,8,8`，pending 恰好 48，错误 `__s` 名称不存在。
- 历史 v1 `s-space` 名称映射恰好 320 项、一对一，示例 `obi__s0__h1s -> obi__d0__h1s`。新目录不输出旧名，也不把不同定义合并。
- raw `mid + stat.mean` 可通过 freeze 并执行原单位滚动均值；`mid + c22.CO_f1ecac` 在 freeze 和执行边界均抛出 `ResearchValidationError`，details 明确 `input_view=mid` 与 unsupported metric。

## 8. 未完成项与边界

- F07/F08 的时间聚合语义仍为 `pending_semantics`；未用 rolling mean 冒充。
- 全 432 项目录合法不等于 432 项真实数值已执行；tick-dependent 定义仍受用户 metadata 缺失限制。
- retrospective `segment_summary` 未交付，Step 05 仍是部分完成。
- Step 06–12 未实现，本报告不宣称后续研究闭环或全项目完成。
- 真实结果仍为 `research_only`，不构成本文宣称的市场发现、可交易信号或科学显著性。
- `/tmp` 真实 Run 工作区用于本轮复验；持久审阅以本报告、源代码/测试和冻结源 hash 为准。
