# replaylab — 可回放的重试放大定位工具

一次用户请求在 `edge → checkout → ledger` 三个本地服务间被放大成一串失败调用。
本工具把分散的尝试记录关联起来，区分**正常扇出 / 真实重试 / 重复采集**，
并支持把重试开关放在不同层做对照实验，最后从一笔超时请求展开因果树验证结论。

纯 Python 标准库，无第三方依赖，Python 3.11+。

## 架构

```
user ──▶ edge ──▶ checkout ──▶ ledger
        :18201     :18202        :18203（端口均为随机分配）
```

- 每次 HTTP 调用产生**两条**尝试记录（调用方 client 侧 + 被调方 server 侧，同一 `attempt_id`）——正常的双端采集；
- 飞行记录仪按 `--dup-rate` 模拟采集器 bug，把部分记录**原样重放**（同 `rec_id`）——病态的重复采集；
- 每层服务各有一个重试开关：`POST /admin/switches {"retry_max": N}`，运行时即可切换；
- `ledger` 容量有限（并发 4），超载时延迟上升，6% 请求进入慢窗口 —— 重试风暴会自我强化；
- `checkout` 对每个商品并行发起一次 `reserve`（正常扇出），每次调用带幂等键，ledger 对重复逻辑操作返回 `idempotent_replay`。

## 使用

```bash
# 1. 录制：起三个服务，跑一遍种子流量（基线：两层都重试 2 次）
python3 -m replaylab run --seed 7 --requests 48 \
    --edge-retries 2 --checkout-retries 2 --out recordings/baseline.jsonl

# 2. 离线分析：去重 → 关联 → 分类 → 指标
python3 -m replaylab analyze recordings/baseline.jsonl

# 3. 因果树：从一笔超时/失败请求展开（默认选放大最严重的）
python3 -m replaylab tree recordings/baseline.jsonl --trace t-0026

# 4. 回放：把录制里的同一批流量在新开关下重放
python3 -m replaylab replay recordings/baseline.jsonl \
    --edge-retries 2 --checkout-retries 0 --out recordings/replayed.jsonl

# 5. 实验：扫各层开关组合，对比成功率与下游流量，末尾自动用因果树验证
python3 -m replaylab experiment --seed 7 --requests 48

# 测试
python3 -m unittest discover -s tests -v
```

## 分析器如何区分三类记录

| 现象 | 判定 | 处理 |
|---|---|---|
| 重复采集（病态） | 同 `rec_id` 的逐字节重复记录 | 丢弃 |
| 双端采集（正常） | 同 `attempt_id`、一 client 一 server | 合并为一次调用 |
| 正常扇出 | 同父调用、同 op、**不同** `request_key`（不同商品） | 标记 `扇出` |
| 真实重试 | 同父调用、同 `request_key`、`try_seq` 递增 | 标记 `重试` |
| 无效重试 | 重试的前一次**服务端其实已成功** | 标记 `重试·无效` |
| 幂等重放 | 上游重试导致重复执行，被 ledger 幂等键拦截 | 单独计数 |

## 实验结论（seed 7 / 48 笔，可复现）

| 配置 | 成功率 | ledger 调用 | 放大 | p99 | 重试（无效） |
|---|---|---|---|---|---|
| edge=2 checkout=2（基线） | 95.8% | 263 | 5.5× | 862ms | 160(153) |
| edge=0 checkout=2 | 87.5% | 85 | 1.8× | 254ms | 9(8) |
| **edge=2 checkout=0** | **100%** | **97** | 2.0× | 609ms | 9(0) |
| edge=1 checkout=1 | 50.0%* | 189 | 3.9× | 543ms | 98(70) |
| edge=0 checkout=1 | 87.5% | 84 | 1.8× | 254ms | 8(7) |
| edge=0 checkout=0 | 85.4% | 76 | 1.6× | 253ms | 0(0) |

\* 中层重试把父调用拖过其 250ms 预算，系统进入亚稳态，不同种子下在 50%–88% 间摆动。

要点：

1. **重试要花在离用户最近的一层**。edge 重试整个 `/order` 时，已成功的 reserve 被幂等键
   拦截（4ms 返回），只有真正失败的部分重跑 —— 便宜且有效；
2. **下层重试在上层预算面前是无效的**：checkout 重试让 `/order` 超过 edge 的 250ms
   预算，edge 早已放弃，下层的努力只转化为 ledger 的拥塞；
3. 基线里 160 次重试有 153 次是**无效重试**（前一次服务端已成功）—— 超时的真实原因
   是重试自己造成的拥塞，因果树里 `inflight=24(其中重试11)` 直接可见。

## 录制格式

JSONL，每行一条尝试记录。关键字段：`rec_id`（采集级唯一）、`attempt_id`（调用级唯一）、
`side`、`trace_id`、`parent_attempt_id`、`idem_key`、`request_key`、`try_seq`、
`ts_start/ts_end`、`outcome`、server 侧另有 `inflight` / `inflight_retries` / `note`。
