# Step 01 — Workflow Engine

## 目的与边界

构建一个**执行科学发现流程的编排引擎**，而不是一个业务算法。它不理解 OBI 或价格，只负责各 Stage 的类型化输入输出、依赖关系、缓存、重试、血缘、资源预算和状态。

## 输入 / 输出

- **输入**：validated YAML config、DAG definition、Stage registry、源数据快照/manifest。
- **输出**：唯一 `run_id`、每个 stage 的 `stage_run_id`、状态与耗时、持久化 ArtifactManifest、调试日志和失败原因。
- **状态**：`PENDING | RUNNING | SUCCEEDED | FAILED | SKIPPED | CACHED | BLOCKED`。

## 设计的基础接口

```python
class Stage(Protocol):
    name: str
    version: str
    dependencies: tuple[str, ...]

    def validate(self, ctx: RunContext, inputs: dict[str, ArtifactRef]) -> None: ...
    def execute(self, ctx: RunContext, inputs: dict[str, ArtifactRef]) -> StageResult: ...

@dataclass(frozen=True)
class StageResult:
    artifacts: dict[str, ArtifactRef]
    metrics: dict[str, float | int | str]
    warnings: list[str]
```

建议 `RunContext` 最少包含 `run_id, config_hash, split_id, data_snapshot_id, as_of_cutoff, artifact_store, rng_seed, logger, resource_budget`。`as_of_cutoff` 是业务信息可用性截止，而不是处理器运行时间。

## 最小 DAG

```mermaid
flowchart TD
    A[onboard] --> B[primitives]
    B --> C[statistical_atlas]
    C --> D[features]
    D --> E[events]
    D --> F[regimes]
    E --> G[phenomena]
    F --> G
    C --> G
    G --> H[hypotheses]
    H --> I[discovery_cube]
    I --> J[falsification]
    J --> K[research_planner]
```

注意：`events` 和 `regimes` 可以并行；`features` 使用已在 `primitives/atlas` 计算好的训练期条件统计量。把所有上游阶段固化为 artifact，而非隐藏的 Python 内存状态。

## 调度和缓存

1. 拓扑排序；发现环则拒绝运行。
2. 计算 stage cache key：`stage version + normalized config subset + input artifact hashes + split/snapshot ids + code revision`。
3. 如果已有完整可验证 artifact，则命中缓存，否则执行；不可依据目录存在就跳过不完整任务。
4. 每个任务写到临时分区/文件，验证 schema + hash 后**原子发布** manifest；失败不应留下假成功。
5. 允许 `--from`, `--until`, `--only`, `--resume`, `--force-stage`，强制重算后应使相关下游缓存失效。
6. 任务应支持分区范围（合约×交易日）；按 partition id 复跑，跨分区汇总作为独立 stage。
7. 大量结果通过 `ArtifactRef(uri, schema_version, hash, partition_keys, row_count, time_min/max)` 传递，不能把整张 DataFrame 放到调度状态。

## 资源和日志

- v1 `LocalExecutor` 限定最大并发数、DuckDB memory_limit、临时目录、线程数、单阶段最大候选数。
- 日志结构包含 run/stage/partition/candidate id、开始结束时间、耗时、peak memory（可观测时）、输入输出 hash、异常栈。
- 参数校验失败与数据质量阻断不得悄悄继续；可选特征缺失允许带 warning 跳过。
- 重试仅用于幂等、瞬时失败；对 schema 错误、数据泄漏检查失败不自动重试。

## 推荐 CLI

```bash
quantsage validate --config examples/quant_discovery.yaml
quantsage plan --config examples/quant_discovery.yaml
quantsage run --config examples/quant_discovery.yaml --until features
quantsage run --config examples/quant_discovery.yaml --resume <run_id>
quantsage status --run-id <run_id>
quantsage artifacts --run-id <run_id> --stage events
quantsage explain --artifact-id <artifact_id>
```

## 验收

- 单元测试：DAG 依赖/环检测、cache key、输入缺失、失败恢复、重试分类、原子发布。
- 集成测试：用合成数据执行 `onboard→primitives→atlas`，第二次执行应产生 `CACHED`，修改阈值仅使受影响阶段及后继失效。
- 测试中模拟中途断电/异常；`--resume` 不重复写出成功分区，输出内容 hash 不变。
- 每个 Stage 能定位自己的参数版本、原始数据 snapshot 和上游输入。

**依赖**：`../../COMMON_CONTRACTS.md`。**下一步**：`../../steps/02_data_onboarding/SPEC.md`。
