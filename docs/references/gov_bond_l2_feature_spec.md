# 国债期货 L2 Market-State Feature Specification

## 0. 目标

本规格用于让 agent 从国债期货逐条 L2 快照中生成用于 **market status identification** 的三维指标体系：

\[
\text{Feature Family} \times \text{Space/Size} \times \text{Time Horizon}
\]

设计目标：

1. 保持指标数量可控；
2. 每个指标有明确微观结构含义；
3. 所有市场数据输入来自给定行情 schema；
4. 唯一允许的外部静态参数为中金所合约 `tick_size`，由 `SYMBOL + TRADINGDATE` 映射；
5. 当前阶段只做可观察 feature，不直接把 feature 命名为 information / inventory / liquidity truth；
6. 输出供后续 Event Detection、State Classification、Impact Study 使用。

---

# 1. Schema 输入字段白名单

只允许直接使用以下行情字段：

| 类型 | Schema 字段 | 用途 |
|---|---|---|
| 时间 | `TRADINGDATE` | 交易日、tick-size 历史映射、窗口边界 |
| 时间 | `TRADINGTIME` | 交易所时间辅助核验 |
| 时间 | `UNIX` | 时间窗口主索引，毫秒 |
| 合约 | `SYMBOL` | 合约身份、品种前缀 |
| 成交 | `TRADEVOLUME` | 相邻快照区间成交量，**禁止再次差分** |
| 成交 | `BUYVOL` | 日内累计主动买成交量 |
| 成交 | `SELLVOL` | 日内累计主动卖成交量 |
| Bid Price | `BUYPRICE01..05` | 买一到买五 |
| Ask Price | `SELLPRICE01..05` | 卖一到卖五 |
| Bid Size | `BUYVOLUME01..05` | 买一到买五数量 |
| Ask Size | `SELLVOLUME01..05` | 卖一到卖五数量 |

暂不直接使用：

- `ORDERRATE`
- `ORDERDIFF`
- `BUYORSELL`
- `OPENCLOSE`

原因：

- `ORDERRATE` 是供应商提供的五档委比，不等价于任意 Space 定义下的 OBI；
- `BUYORSELL` / `OPENCLOSE` 是供应商推导字段，不是真实逐笔订单方向；
- 本规格统一从盘口与累计主动买卖量自行构造指标。

---

# 2. Tick Size 配置

## 2.1 当前产品 tick size

| Product | SYMBOL 前缀 | Tick Size |
|---|---:|---:|
| 2Y 国债期货 | `TS` | 0.002 |
| 5Y 国债期货 | `TF` | 0.005 |
| 10Y 国债期货 | `T` | 0.005 |
| 30Y 国债期货 | `TL` | 0.010 |

## 2.2 历史 tick-size 规则

必须按 `SYMBOL + TRADINGDATE` 映射，不允许对所有历史数据直接使用当前 tick。

### TF

\[
tick(TF,t)=
\begin{cases}
0.002,& TRADINGDATE < 2015\text{-}03\text{-}17\\
0.005,& TRADINGDATE \ge 2015\text{-}03\text{-}17
\end{cases}
\]

### T

\[
tick(T,t)=0.005
\]

适用于 2015-03-20 上市以来的有效交易数据。

### TS

\[
tick(TS,t)=
\begin{cases}
0.005,& TRADINGDATE < 2023\text{-}11\text{-}07\\
0.002,& TRADINGDATE \ge 2023\text{-}11\text{-}07
\end{cases}
\]

适用于 2018-08-17 上市以来的有效交易数据。

### TL

\[
tick(TL,t)=0.010
\]

适用于 2023-04-21 上市以来的有效交易数据。

### SYMBOL 前缀解析

必须按最长前缀优先：

```text
TL -> 30Y
TF -> 5Y
TS -> 2Y
T  -> 10Y
```

禁止先用 `startswith("T")`，否则会误识别 `TF/TS/TL`。

---

# 3. 三维网格

## 3.1 Space：5 个 tick distance

使用：

\[
d \in \{0,1,2,3,5\}
\]

命名：

```text
d0
d1
d2
d3
d5
```

含义：

- `d0`：只看 touch，即 L1；
- `d1`：距 touch 不超过 1 tick；
- `d2`：距 touch 不超过 2 ticks；
- `d3`：距 touch 不超过 3 ticks；
- `d5`：距 touch 不超过 5 ticks。

注意：当前数据最多只有五档，因此某个 `d` 内实际能覆盖多少档由价格决定，不保证 `d5` 一定包含五档。

---

## 3.2 Size：3 个相对订单规模

先定义五档可见深度：

\[
D_t^{B,5}=\sum_{i=1}^{5} BUYVOLUME0i_t
\]

\[
D_t^{A,5}=\sum_{i=1}^{5} SELLVOLUME0i_t
\]

定义参考容量：

\[
D_t^{ref}
=
\frac{D_t^{B,5}+D_t^{A,5}}{2}
\]

目标订单规模：

\[
q_t(\alpha)=\alpha D_t^{ref}
\]

其中：

\[
\alpha\in\{0.25,0.50,1.00\}
\]

命名：

```text
q25
q50
q100
```

含义：

- `q25`：当前平均五档可见容量的 25%；
- `q50`：50%；
- `q100`：100%。

目标是进行 **size-conditioned liquidity stress test**，不是模拟某个固定手数策略。

---

## 3.3 Time Horizon：8 个窗口

统一使用：

```text
1s
3s
1m
5m
10m
15m
30m
60m
```

记为：

\[
H=
\{1s,3s,1m,5m,10m,15m,30m,60m\}
\]

窗口定义：

\[
W_h(t)
=
\{u:
UNIX_t-h < UNIX_u \le UNIX_t,\,
SYMBOL_u=SYMBOL_t,\,
TRADINGDATE_u=TRADINGDATE_t
\}
\]

### Session 规则

- 禁止跨交易日；
- 禁止把午间/收盘后的无交易时间当成有效 active-time observations；
- 实现时应在同一连续交易 session 内计算；
- 若当前 session 内历史长度不足 horizon，则该 factor 输出 `NA`，不要跨 session 拼接。

---

# 4. 基础派生量

以下量不是最终 factor，而是 12 个 feature family 的公共中间变量。

## 4.1 Mid Price

\[
m_t
=
\frac{BUYPRICE01_t+SELLPRICE01_t}{2}
\]

## 4.2 Tick-normalized Spread

\[
spreadTicks_t
=
\frac{SELLPRICE01_t-BUYPRICE01_t}{tick_t}
\]

## 4.3 每档距 touch 的 tick 距离

Bid：

\[
\delta_{i,t}^{B}
=
\frac{BUYPRICE01_t-BUYPRICE0i_t}{tick_t}
\]

Ask：

\[
\delta_{i,t}^{A}
=
\frac{SELLPRICE0i_t-SELLPRICE01_t}{tick_t}
\]

其中 \(i=1,\ldots,5\)。

## 4.4 指定 Space 内深度

Bid：

\[
D_t^B(d)
=
\sum_{i=1}^{5}
BUYVOLUME0i_t
\cdot
\mathbf{1}
(\delta_{i,t}^B\le d)
\]

Ask：

\[
D_t^A(d)
=
\sum_{i=1}^{5}
SELLVOLUME0i_t
\cdot
\mathbf{1}
(\delta_{i,t}^A\le d)
\]

## 4.5 窗口样本数

\[
N_h(t)=|W_h(t)|
\]

所有 snapshot-average 类型 factor 使用实际有效 snapshot 数量计算，不按名义秒数补零。

---

# 5. 12 个 Feature Family

## F01 — Spread

- **factor_name**: `spread_ticks__h{h}`
- **含义**: 时间窗口内 touch 宽度，即获取即时流动性的基础价格成本
- **feature**: `Spread`
- **space/size**: L1 / touch；无 size
- **time horizon**: 全部 8 个

\[
Spread_h(t)
=
\frac{1}{N_h(t)}
\sum_{u\in W_h(t)}
\frac{SELLPRICE01_u-BUYPRICE01_u}{tick_u}
\]

---

## F02 — Space OBI

- **factor_name**: `obi__d{d}__h{h}`
- **含义**: 指定价格空间内 displayed bid/ask liquidity 的方向性不平衡
- **feature**: `Order Book Imbalance`
- **space/size**: `d0,d1,d2,d3,d5`
- **time horizon**: 全部 8 个

\[
OBI_t(d)
=
\frac{
D_t^B(d)-D_t^A(d)
}{
D_t^B(d)+D_t^A(d)
}
\]

\[
OBI_h(t,d)
=
\frac{1}{N_h(t)}
\sum_{u\in W_h(t)}
OBI_u(d)
\]

分母为 0 时输出 `NA`。

---

## F03 — Distance-Weighted OBI

- **factor_name**: `wobi__d{d}__h{h}`
- **含义**: 离 touch 越近的 liquidity 权重越高，减少远端挂单对短期状态的过度影响
- **feature**: `Weighted Order Book Imbalance`
- **space/size**: `d0,d1,d2,d3,d5`
- **time horizon**: 全部 8 个

\[
w_{i,t}^{B}
=
\frac{1}{1+\delta_{i,t}^{B}}
,\qquad
w_{i,t}^{A}
=
\frac{1}{1+\delta_{i,t}^{A}}
\]

\[
WD_t^B(d)
=
\sum_i
w_{i,t}^B
BUYVOLUME0i_t
\mathbf 1(\delta_{i,t}^B\le d)
\]

\[
WD_t^A(d)
=
\sum_i
w_{i,t}^A
SELLVOLUME0i_t
\mathbf 1(\delta_{i,t}^A\le d)
\]

\[
WOBI_t(d)
=
\frac{
WD_t^B(d)-WD_t^A(d)
}{
WD_t^B(d)+WD_t^A(d)
}
\]

对 \(W_h(t)\) 内有效 snapshot 求平均。

---

## F04 — Minimum Size Coverage

- **factor_name**: `min_coverage__d{d}__q{q}__h{h}`
- **含义**: 指定空间内，两侧中更弱的一侧能覆盖目标订单规模的比例
- **feature**: `Minimum Size Coverage`
- **space/size**: 5 space × 3 size
- **time horizon**: 全部 8 个

令 \(q_t=q_t(\alpha)\)：

\[
Coverage_t^B(d,q)
=
\min
\left(
1,
\frac{D_t^B(d)}{q_t}
\right)
\]

\[
Coverage_t^A(d,q)
=
\min
\left(
1,
\frac{D_t^A(d)}{q_t}
\right)
\]

\[
MinCoverage_t(d,q)
=
\min
(
Coverage_t^B,
Coverage_t^A
)
\]

时间聚合：窗口均值。

---

## F05 — Size-Conditioned Capacity Imbalance

- **factor_name**: `capacity_imb__d{d}__q{q}__h{h}`
- **含义**: 只计算目标 size 真正需要的 capacity，识别这个 size 下哪一侧更脆弱
- **feature**: `Capacity Imbalance`
- **space/size**: 5 space × 3 size
- **time horizon**: 全部 8 个

\[
C_t^B(d,q)
=
\min(D_t^B(d),q_t)
\]

\[
C_t^A(d,q)
=
\min(D_t^A(d),q_t)
\]

\[
CapacityImb_t(d,q)
=
\frac{
C_t^B(d,q)-C_t^A(d,q)
}{
C_t^B(d,q)+C_t^A(d,q)
}
\]

时间聚合：窗口均值。

---

## F06 — Size-Conditioned Spread

- **factor_name**: `size_spread__q{q}__h{h}`
- **含义**: 成交目标 size 时，当前五档盘口显示出的双边执行宽度
- **feature**: `Size Spread`
- **space/size**: `q25,q50,q100`
- **time horizon**: 全部 8 个

Buy VWAP：依次消耗 `SELLVOLUME01..05`

\[
x_{i,t}^{buy}
=
\min
\left(
SELLVOLUME0i_t,
\max
\left[
0,
q_t-\sum_{j<i}x_{j,t}^{buy}
\right]
\right)
\]

若五档可完整覆盖 \(q_t\)：

\[
VWAP_t^{buy}(q)
=
\frac{
\sum_{i=1}^{5}
SELLPRICE0i_t x_{i,t}^{buy}
}{
q_t
}
\]

Sell VWAP 同理依次消耗 `BUYVOLUME01..05`：

\[
VWAP_t^{sell}(q)
=
\frac{
\sum_{i=1}^{5}
BUYPRICE0i_t x_{i,t}^{sell}
}{
q_t
}
\]

\[
SizeSpread_t(q)
=
\frac{
VWAP_t^{buy}(q)-VWAP_t^{sell}(q)
}{
tick_t
}
\]

时间聚合：仅对窗口内两侧都能完整覆盖 \(q_t\) 的 snapshots 求均值。

---

## F07 — Fill Distance Asymmetry

- **factor_name**: `fill_dist_asym__q{q}__h{h}`
- **含义**: 相同规模向上买与向下卖需要走多少 tick，识别 bid/ask 哪侧更脆
- **feature**: `Fill Distance Asymmetry`
- **space/size**: `q25,q50,q100`
- **time horizon**: 全部 8 个

\[
j_B^*
=
\min
\left\{
j:
\sum_{i=1}^{j}SELLVOLUME0i_t\ge q_t
\right\}
\]

\[
Dist_t^{buy}(q)
=
\frac{
SELLPRICE0j_B^*_t-SELLPRICE01_t
}{
tick_t
}
\]

\[
j_S^*
=
\min
\left\{
j:
\sum_{i=1}^{j}BUYVOLUME0i_t\ge q_t
\right\}
\]

\[
Dist_t^{sell}(q)
=
\frac{
BUYPRICE01_t-BUYPRICE0j_S^*_t
}{
tick_t
}
\]

\[
FillDistAsym_t(q)
=
\frac{
Dist_t^{buy}(q)-Dist_t^{sell}(q)
}{
1+Dist_t^{buy}(q)+Dist_t^{sell}(q)
}
\]

方向：
- \(>0\)：ask side 更脆
- \(<0\)：bid side 更脆

---

## F08 — Walk Cost Asymmetry

- **factor_name**: `walk_cost_asym__q{q}__h{h}`
- **含义**: 相同规模向上/向下立即执行时的 VWAP 成本差，是核心 static fragility 指标
- **feature**: `Walk Cost Asymmetry`
- **space/size**: `q25,q50,q100`
- **time horizon**: 全部 8 个

\[
m_t
=
\frac{BUYPRICE01_t+SELLPRICE01_t}{2}
\]

\[
Cost_t^{buy}(q)
=
\frac{
VWAP_t^{buy}(q)-m_t
}{
tick_t
}
\]

\[
Cost_t^{sell}(q)
=
\frac{
m_t-VWAP_t^{sell}(q)
}{
tick_t
}
\]

\[
WalkCostAsym_t(q)
=
\frac{
Cost_t^{buy}(q)-Cost_t^{sell}(q)
}{
Cost_t^{buy}(q)+Cost_t^{sell}(q)
}
\]

方向：
- \(>0\)：ask side 更 fragile
- \(<0\)：bid side 更 fragile

---

## F09 — Active Flow Imbalance

- **factor_name**: `active_flow_imb__h{h}`
- **含义**: 时间窗口内主动成交方向
- **feature**: `Active Flow Imbalance`
- **space/size**: 不适用
- **time horizon**: 全部 8 个

\[
\Delta Buy_h
=
BUYVOL_t-BUYVOL_{t-h}
\]

\[
\Delta Sell_h
=
SELLVOL_t-SELLVOL_{t-h}
\]

\[
ActiveFlowImb_h
=
\frac{
\Delta Buy_h-\Delta Sell_h
}{
\Delta Buy_h+\Delta Sell_h
}
\]

边界：
- 累计字段发生重置则输出 `NA`
- 分母为 0 输出 `NA`

---

## F10 — Volume-to-Depth Pressure

- **factor_name**: `volume_to_depth__h{h}`
- **含义**: 时间窗口成交活动相对于可见盘口容量的压力
- **feature**: `Volume / Depth Pressure`
- **space/size**: 五档 reference depth
- **time horizon**: 全部 8 个

`TRADEVOLUME` 已是相邻快照区间量：

\[
V_h(t)
=
\sum_{u\in W_h(t)}
TRADEVOLUME_u
\]

\[
\overline{D_h^{ref}}
=
\frac1{N_h(t)}
\sum_{u\in W_h(t)}
D_u^{ref}
\]

\[
VolumeDepthPressure_h
=
\frac{
V_h(t)
}{
\overline{D_h^{ref}}
}
\]

---

## F11 — Mid Move in Ticks

- **factor_name**: `mid_move_ticks__h{h}`
- **含义**: horizon 内 midpoint 实际移动多少 tick
- **feature**: `Mid Price Move`
- **space/size**: 不适用
- **time horizon**: 全部 8 个

\[
MidMove_h(t)
=
\frac{
m_t-m_{t-h}
}{
tick_t
}
\]

其中 \(t-h\) 使用同一 session 内 horizon 起点之前最近一条有效 snapshot。

---

## F12 — Flow Impact Efficiency

- **factor_name**: `flow_impact_eff__h{h}`
- **含义**: 单位“相对盘口容量”的净主动 flow 推动了多少 tick，是 Flow→Impact 核心指标
- **feature**: `Flow Impact Efficiency`
- **space/size**: flow size / visible depth
- **time horizon**: 全部 8 个

\[
NF_h
=
\Delta Buy_h-\Delta Sell_h
\]

\[
NormalizedFlow_h
=
\frac{
|NF_h|
}{
\overline{D_h^{ref}}
}
\]

\[
SignedMove_h
=
sign(NF_h)
\cdot
\frac{
m_t-m_{t-h}
}{
tick_t
}
\]

\[
FlowImpactEff_h
=
\frac{
SignedMove_h
}{
NormalizedFlow_h+\epsilon
}
\]

其中 \(\epsilon\) 仅为数值稳定常数，如 \(10^{-12}\)。

若 \(NF_h=0\)，输出 `NA`。

---

# 6. Factor Expansion Matrix

| Feature | Space 数 | Size 数 | Horizon 数 | Factor 数 |
|---|---:|---:|---:|---:|
| F01 Spread | 1 | 1 | 8 | 8 |
| F02 OBI | 5 | 1 | 8 | 40 |
| F03 Weighted OBI | 5 | 1 | 8 | 40 |
| F04 Min Coverage | 5 | 3 | 8 | 120 |
| F05 Capacity Imbalance | 5 | 3 | 8 | 120 |
| F06 Size Spread | 1 | 3 | 8 | 24 |
| F07 Fill Distance Asymmetry | 1 | 3 | 8 | 24 |
| F08 Walk Cost Asymmetry | 1 | 3 | 8 | 24 |
| F09 Active Flow Imbalance | 1 | 1 | 8 | 8 |
| F10 Volume-to-Depth | 1 | 1 | 8 | 8 |
| F11 Mid Move | 1 | 1 | 8 | 8 |
| F12 Flow Impact Efficiency | 1 | 1 | 8 | 8 |
| **Total** | | | | **432** |

---

# 7. Naming Convention

统一格式：

```text
{feature}__d{space}__q{size}__h{horizon}
```

只保留实际适用维度。

示例：

```text
spread_ticks__h1s
obi__d0__h3s
obi__d5__h5m
wobi__d2__h1m
min_coverage__d3__q50__h10m
capacity_imb__d5__q100__h15m
size_spread__q25__h3s
fill_dist_asym__q100__h1m
walk_cost_asym__q50__h30m
active_flow_imb__h3s
volume_to_depth__h5m
mid_move_ticks__h10m
flow_impact_eff__h1m
```

---

# 8. 输出表结构

建议最终 agent 输出 long-format factor table：

| 字段 | 含义 |
|---|---|
| `TRADINGDATE` | 原交易日 |
| `UNIX` | 当前 snapshot 时间 |
| `SYMBOL` | 原合约 |
| `factor_name` | 标准因子名 |
| `feature_family` | 12 个 family 之一 |
| `space` | `d0/d1/d2/d3/d5/NA` |
| `size` | `q25/q50/q100/NA` |
| `horizon` | 8 个 horizon |
| `value` | 因子值 |
| `is_valid` | 是否有效 |
| `invalid_reason` | 无效原因 |

也可同时生成 wide table 用于模型训练。

---

# 9. 数据质量与 NA 规则

## 9.1 报价有效性

必须满足：

\[
BUYPRICE01>0
\]

\[
SELLPRICE01>0
\]

\[
SELLPRICE01 \ge BUYPRICE01
\]

对应 depth 必须非负。

无效 snapshot 不参与窗口聚合。

## 9.2 价格层级顺序

\[
BUYPRICE01
\ge BUYPRICE02
\ge \cdots
\ge BUYPRICE05
\]

\[
SELLPRICE01
\le SELLPRICE02
\le \cdots
\le SELLPRICE05
\]

若某档为 0 / sentinel，则该档及更深档不得参与 walk/VWAP。

## 9.3 Five-level truncation

如果目标订单超过五档可见深度，则对应 VWAP / fill-distance / walk-cost 输出 `NA`。

禁止：

- 外推 L6+ 价格；
- 用 L5 重复填充；
- 假设隐藏流动性。

`invalid_reason = insufficient_visible_depth`

## 9.4 累计主动成交重置

若：

\[
BUYVOL_t<BUYVOL_{t-h}
\]

或：

\[
SELLVOL_t<SELLVOL_{t-h}
\]

则相关 flow factors 输出 `NA`。

## 9.5 TRADEVOLUME

正确：

\[
V_h=\sum TRADEVOLUME
\]

错误：

\[
\Delta TRADEVOLUME
\]

---

# 10. Agent 计算顺序

1. 按 `TRADINGDATE, SYMBOL, UNIX` 排序；
2. 用 `SYMBOL + TRADINGDATE` 映射 `tick_size`；
3. 计算 `mid / spread / tick distance / space depth / D_ref / q25,q50,q100`；
4. 计算单 snapshot 的 OBI / WOBI / Coverage / Capacity / VWAP / Fill Distance / Walk Cost；
5. 建立 `1s,3s,1m,5m,10m,15m,30m,60m` session-aware 时间窗口；
6. 聚合 snapshot features；
7. 计算 Active Flow / Volume-to-Depth / Mid Move / Flow Impact；
8. 按 naming convention 输出 432 个 factor；
9. 保留 `is_valid` 与 `invalid_reason`，禁止静默填值。

---

# 11. Market-State 使用原则

这些 factor 不能单独直接解释为多空信号。

## Ask Fragility

典型组合：

```text
capacity_imb__d3__q100 < 0
walk_cost_asym__q100 > 0
fill_dist_asym__q100 > 0
```

含义：large-size buy 比 sell 更容易穿透 ask。

## Buy Absorption Candidate

```text
active_flow_imb__h3s >> 0
volume_to_depth__h3s high
mid_move_ticks__h3s small
flow_impact_eff__h3s low
```

含义：大量主动买进入，但价格响应弱，存在 sell-side absorption 候选。

## Upside Liquidity Vacuum

```text
min_coverage__d1__q50 low
walk_cost_asym__q50 high positive
active_flow_imb__h1s positive
flow_impact_eff__h1s high
```

含义：近端 ask liquidity 不足，buy flow 对价格非常有效。

## Local Queue Imbalance

```text
obi__d0__h1s high positive
obi__d5__h1s near zero
```

含义：不平衡主要存在 touch，不是整个可见盘口的结构性失衡。

---

# 12. 重要限制

当前数据是 **逐条 L2 snapshot**，不是逐笔委托/order-log。

因此可以可靠构造：

```text
book state
space structure
size capacity
active-flow proxy
price response
book-state transitions
```

但不能精确识别：

```text
真实 cancel event
真实 add event
单笔订单生命周期
queue position
真实 refill order
hidden / iceberg liquidity
```

这些需要逐笔委托或更完整的 order-level feed。

---

# 13. 最终目标

第一阶段：

\[
Features
\times
Space/Size
\times
Time
\rightarrow
Market Status
\]

后续再构造：

```text
Event Detection
State Classification
Impact / Persistence Study
Information / Liquidity / Inventory latent-state inference
```

不要直接把 432 个 raw factor 变成多空规则。
