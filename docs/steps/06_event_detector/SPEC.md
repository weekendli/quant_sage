# Step 06 — Event Detector

## 目标

将连续 Feature 或 Baseline residual 转成**可复现的离散事件**。Event 是某时点已可判定的特征变化，不必先证明具有预测力。

## 四种基础 Detector

| Detector | 触发机制 | 例子 |
|---|---|---|
| `extreme` | 训练期拟合的条件高/低分位、robust score | bid depth 急降、成交活跃骤升 |
| `surprise` | 只用过去拟合的预测分布出现极端残差 | 意外价差扩大 |
| `divergence` | 两变量原有依赖短时失效 | depth ↓ 但 mid 稳定 |
| `change_point` | **在线**累积证据说明局部统计分布变化 | 更新强度突变 |

MVP 优先 extreme + divergence；surprise 用简单季节性/自回归 baseline；在线 CUSUM 可作为 change-point 初版。离线 PELT/ruptures 仅做历史分析，必须标记其可检测时间不等于真正的历史触发时间。

## Event schema

```yaml
id: evt_...
detector_id: depth_depletion_v1
instrument_id: Txxxx
start_time: '...'
trigger_time: '...'
detected_time: '...'
end_time: null
severity_score: 3.1
trigger_feature_ids: [bid_depth_depletion_30s]
baseline_ref: baseline_...
quality_flags: []
```

`trigger_time` 是达到条件的行情时间；`detected_time` 是系统最早可识别时间；`end_time` 对尚未结束的事件可以 null，**不能将事后最终持续时长用作当时信号**。

## 事件片段 / 去重

- 用 `threshold_on` 与 `threshold_off` 提供迟滞，避免阈值附近抖动。
- `minimum_duration` 若非零，则 `detected_time` 晚于首次满足条件；真实可用时间必须反映这一延迟。
- 为相邻事件设置 `cooldown`；合并规则按 instrument×event type，写出 event episode 与成员 tick 引用。
- 同时保留原始 trigger 与 episode summary，计算独立事件数量以避免每个 tick 当作独立样本。
- 事件与交易会话切换必须明确边界，不让一个事件横跨停盘导致虚假的持续性。

## 评价指标（不使用未来收益）

触发率、每交易日事件数、平均持续时间（只作事后报告）、事件间隔分布、同日聚集度、条件基准误报率、跨合约和跨时期重复性。不要把“事件总数”当统计独立样本数。

## 示例

`bid_depth_depletion`：在当前交易时段背景的条件分位数低于阈值，且最近30s 深度变化足够大；`price_stickiness`：当前可用 mid 在该窗口内小幅变化；两个单独事件可以组成更高阶 `depth_drop_price_stable` composite event（在 Event Factory 只需定义组合触发，不先断言方向）。

## 验收

- 人工序列验证阈值触发、迟滞、cooldown、episode 合并与 `detected_time`。
- 改变**未来**的数据不应改变过去的事件触发结果（online detector）。
- 缺时戳、跨 session、stale quote、低样本条件基准应返回标志或不触发。
- Registry 能回答某事件何时触发、为何触发、由哪些原始字段和 baseline 推导。

**前置**：05；**下一步**：08。
