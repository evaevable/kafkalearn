# 第 7 章实战：三次换班，亲眼看完

对应[第 7 章讲义](../../chapters/07-rebalance-lifecycle.md)，继续使用[单节点 Compose](../../infra/single-node/compose.yaml)与固定 `confluent-kafka==2.13.2`。

## 1. 这次要观察什么

实验导演三次真实的换班（Rebalance），每次记录"从触发到重新稳定"用了多久、分区怎么交接、读了多少条消息：

| 幕 | 动作 | 重点观察 |
| --- | --- | --- |
| 1 | member-a 独自上线 | 3 个分区全归一人 |
| 2 | member-b 加入 | 3 分区分成 2 + 1，约 2 秒 |
| 3 | member-b **猝死**（os._exit，不打招呼） | 要等约 6 秒会话超时才能换班 |
| 4 | member-b2 上班后又**正常下班**（close） | 约 0.8 秒秒级换班 |

每组一个消费者进程，全部使用 `subscribe()` 参加真正的组分配；脚本只中断**它自己启动的进程**，不动 Broker、不删数据。

**实验刻意全程不提交书签**：这样每次换班后，接手者都会从最早位置重读，"换班 + 没书签 = 重复读取"的效果可以直接数出来（实测 6 条消息累计被读 30 次）。

## 2. 运行方式

需要 Docker 正常运行、Docker Compose v2、Python 3.9+。复用第 4~6 章准备好的虚拟环境：

```bash
PYTHON_BIN="$PWD/.venv/bin/python" bash labs/ch07-rebalance-lifecycle/verify.sh
```

首次安装在仓库根目录：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r labs/ch07-rebalance-lifecycle/requirements.txt
PYTHON_BIN="$PWD/.venv/bin/python" bash labs/ch07-rebalance-lifecycle/verify.sh
```

依赖只装进虚拟环境，脚本不自动安装、不改全局配置。兼容 `docker compose` 与独立 `docker-compose` v2。

**运行中会有一个约 6 秒的"等待会话超时"阶段，属预期节奏。**不要与第 2 章容器重建实验并发。Topic、组、观察结果全部保留。

## 3. 关键配置与代码位置

消费者配置（完整程序见 [app.py](app.py)）：

```python
"group.protocol": "classic",
"partition.assignment.strategy": "range",
"enable.auto.commit": False,
"enable.auto.offset.store": False,
"auto.offset.reset": "earliest",
"session.timeout.ms": 6000,       # 失联判定线：本次约 6 秒
"heartbeat.interval.ms": 1000,
"max.poll.interval.ms": 30000,    # 太久不 poll 也会被踢
```

三个进程分工：

- **member-a**：第一幕独自获得全部分区；之后每次换班都记录 revoke/assign 时间点。
- **member-b**：第二幕加入；第三幕检测到 `crash-member-b` 文件后 `os._exit(42)`——不 close、不清理，模拟猝死。
- **member-b2**：第四幕加入；检测到 `stop-member-b2` 文件后走正常 `close()`，模拟正常下班。

编排程序每秒级轮询 Broker 侧 `describe_consumer_groups`，同时比对消费者本地的 `assignment()`，连续两次一致才算"稳定"，再记录耗时。

## 4. 什么叫"猝死"、什么叫"正常下班"

- **猝死**：进程直接 `os._exit(42)`，不发 LeaveGroup、不发最后心跳。Broker 只能等 `session.timeout.ms` 心跳超时，判定它离开——这就是第三幕必须等约 6 秒的原因。
- **正常下班**：`close()` 会主动发 LeaveGroup。Broker 立刻重新分配，不用等超时——这就是第四幕只要约 0.8 秒的原因。

两种退出码都会被父进程核对：member-b 必须是 42（且没有跑过 close），member-a 和 member-b2 必须是 0（且正常 closed）。

## 5. 结果文件

结果在 `labs/ch07-rebalance-lifecycle/results/<runid>/`（Git 忽略）：

- `run.env` / `topic.txt`：本次 Topic、组名与结构。
- `source.json` / `producer-deliveries.json`：6 条源事件（3 分区 × 2 条）与投递坐标。
- `member-a.json` / `member-b.json` / `member-b2.json`：各自的生命周期时间线（assign / revoke + 时间戳）、收到的记录、当前 assignment。
- `p1/p2/p3/p4.cli-members.txt`、`*.cli-state.txt`：四个稳定时刻的官方 CLI 成员/状态快照。
- `summary.json`：三次换班耗时、revoke→assign 交接间隙、重复读取统计。

## 6. 作者实跑验收

2026-10-13，macOS + Colima / Docker Engine 27.0.3、独立 Compose 2.29.1、Python 3.13.12、confluent-kafka/librdkafka 2.13.2、Kafka Broker 4.3.1 环境实跑通过：

| 场景 | 实测 | 说明 |
| --- | --- | --- |
| 新成员加入 | **1.97 秒** | member-a 得 [0,1]，member-b 得 [2]（range 分配） |
| 猝死换班 | **6.08 秒** | member-b 退出码 42；member-a 收回全部分区 |
| 正常下班换班 | **0.84 秒** | member-b2 退出码 0；member-a 收回全部分区 |

- 读者统计：6 条消息一共被读 **30 次**（member-a 26 次、member-b 2 次、member-b2 2 次），全部 6 条至少被读过两遍，2 条被不同进程读过——因为全程没有提交书签，每次换班从最早位置重读，属预期现象。
- member-a 的 4 次 revoke→assign 交接间隙为 0.039–0.066 秒。
- 8 项本地自测（分配覆盖/重叠拒绝、重复读取统计、交接间隙、耗时期望等）通过；四幕的成员/分配快照与本地 `assignment()` 一致。
- 已修复一次真实故障：初版 worker 未设置 `client.id`，所有成员都叫默认值导致无法区分，脚本如实失败并保留现场；补上 `client.id` 后重跑通过。

本记录只覆盖单节点、单消费者实例的换班时序；不测试多 Broker 集群、分区迁移、横向扩容吞吐等场景，也不把一次运行的数字当作生产基准。

## 7. 排错与停止

| 现象 | 先检查 |
| --- | --- |
| 等待超过预期 | 会话超时阶段本来就有约 6 秒；再确认容器健康、脚本是否还在运行 |
| 组状态长时间不 STABLE | 查看 `member-*.log`；是否有残留进程占着旧组 |
| 想看换班细节 | 打开 `member-a.json` 的 `lifecycle` 数组，revoke/assign 时间戳都在里面 |
| 重复运行 | 脚本每次使用全新 Topic 与组；直接重跑即可，不要复用旧结果目录 |

完成后停止，保留数据：

```bash
docker compose -f infra/single-node/compose.yaml stop kafka
```

下一章开始搭建三节点集群，研究副本、Leader 与故障。
