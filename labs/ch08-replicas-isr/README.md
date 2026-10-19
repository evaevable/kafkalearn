# 第 8 章实战：三节点集群，亲手停掉一台 Broker

对应[第 8 章讲义](../../chapters/08-replicas-isr.md)。这一章第一次使用**三节点**环境：[infra/three-nodes/compose.yaml](../../infra/three-nodes/compose.yaml)，端口 `19192 / 29292 / 39392`。

## 1. 这次观察什么

| 阶段 | 动作 | 预期 |
| --- | --- | --- |
| 基线 | 三个 Broker 全健康 | 主 Topic 每分区副本与 ISR 都是 [1,2,3] |
| 故障 | 停掉 P0 的 Leader（本次是 Broker 2） | 约 3 秒选出新 Leader，ISR 3→2，读写继续 |
| 对照 | 写一个 RF=2、要求 2 份确认的小 Topic | 写入被拒（NOT_ENOUGH_REPLICAS），读取正常 |
| 恢复 | 重新启动被停的 Broker | 约 3.6 秒 ISR 回到 3，小 Topic 恢复可写 |

脚本只会停**一台** Broker，并且全程保留 2/3 的控制器多数派。**不是断电、不是磁盘故障、不是同时挂两台。**

## 2. 运行方式

需要 Docker 正常运行、Docker Compose v2、Python 3.9+。复用之前的虚拟环境：

```bash
PYTHON_BIN="$PWD/.venv/bin/python" bash labs/ch08-replicas-isr/verify.sh
```

首次安装在仓库根目录：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r labs/ch08-replicas-isr/requirements.txt
PYTHON_BIN="$PWD/.venv/bin/python" bash labs/ch08-replicas-isr/verify.sh
```

脚本会启动三个 Kafka 容器（KRaft，1/2/3 号节点都兼任 controller 与 broker），跑完自动停止容器、保留数据卷。首次启动约 15 秒；整个实验约 20 秒。

## 3. 两个 Topic 的对照

| Topic | 分区 | 副本数 | min.insync.replicas | 用途 |
| --- | --- | --- | --- | --- |
| `...main` | 3 | 3 | 2（Broker 默认） | 证明挂一台仍可读写 |
| `...small` | 1 | 2 | 2（显式设置） | 证明少一份就拒写 |

小 Topic 的两个副本被**显式指定**为"P0 的 Leader + 另一台"，这样停掉那一台时，它必然只剩 1 份，低于 2 份确认要求。

Producer 全部使用 `acks=all`；小 Topic 的失败用例额外关闭幂等并设置 `retries=0`，让 Broker 的拒绝错误直接出现在投递回调里，而不是等到超时。

## 4. 作者实跑验收

2026-10-19，macOS + Colima / Docker Engine 27.0.3、独立 Compose 2.29.1、Python 3.13.12、confluent-kafka/librdkafka 2.13.2、Kafka Broker 4.3.1 实跑通过：

| 指标 | 实测 |
| --- | --- |
| 停掉的 Broker | 2 号（当时 P0 的 Leader） |
| 选出新 Leader（P0 换到 3 号） | **3.43 秒** |
| 主 Topic 的 ISR | [1,2,3] → [1,3] |
| 故障期间主 Topic 写入 | 3 条成功（累计 9 条全部可读） |
| 小 Topic 写入 | 拒绝：`NOT_ENOUGH_REPLICAS`（val=19） |
| 小 Topic 读取 | 正常（1 条） |
| 重启 Broker 到 ISR 恢复 | **3.61 秒**，之后小 Topic 写入成功、共 2 条 |

- 8 项本地自测（副本覆盖、ISR 收缩、Leader 不属于被停 Broker、拒写判定、恢复判定）通过。
- 停 Broker 用 `docker compose stop`（正常停机），所以选主较快；崩溃场景要等心跳超时，见第 7 章。
- 实跑中修了两个客户端接口问题：`replica_assignment` 必须搭配显式分区数、`describe_topics` 入参类型变化；最终改用"每次新建 Producer 取最新元数据"。

## 5. 结果文件

结果在 `labs/ch08-replicas-isr/results/<runid>/`（Git 忽略）：

- `run.env`：本次 Topic 前缀与三台 Broker 地址。
- `baseline.json`：故障前的主/小 Topic 副本与 ISR。
- `before.json`：6 条主 Topic 记录 + 1 条小 Topic 记录的读取。
- `after_stop.json`：停掉 Broker 后的元数据、新 Leader、耗时。
- `during_outage.json`：故障期间主 Topic 写入/读取、小 Topic 拒写错误、小 Topic 仍可读。
- `recovered.json`：恢复后的 ISR、耗时、小 Topic 最终读取。
- `summary.json`：全部关键数字。

## 6. 手动复现（可选）

实验本身是自动化的。想手动玩，先启动集群并建 Topic：

```bash
docker compose -f infra/three-nodes/compose.yaml up -d --wait
docker compose -f infra/three-nodes/compose.yaml exec -T kafka-1 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 \
  --create --topic demo.ch08 --partitions 3 --replication-factor 3
docker compose -f infra/three-nodes/compose.yaml exec -T kafka-1 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 \
  --describe --topic demo.ch08
```

停掉一台（挑一个当时是 Leader 的）：

```bash
docker compose -f infra/three-nodes/compose.yaml stop kafka-2
docker compose -f infra/three-nodes/compose.yaml exec -T kafka-1 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 \
  --describe --topic demo.ch08
docker compose -f infra/three-nodes/compose.yaml start kafka-2
```

`--describe` 里的 `Leader`、`Replicas`、`Isr` 三列就是本章的三个主角。

## 7. 排错与停止

| 现象 | 先检查 |
| --- | --- |
| 三个容器起不来 | `docker compose -f infra/three-nodes/compose.yaml logs kafka-1`；端口 19192/29292/39392 是否被占用 |
| 一直等不到新 Leader | 是否有两台以上被停掉（会失去多数派）；只停一台 |
| 小 Topic 一直写成功 | 确认它的副本确实是 [被停的那台, 另一台]，且 min.insync.replicas=2 |
| 想重跑 | 直接用新的一次运行；Topic 前缀带 runid，不会冲突 |

停止并保留数据：

```bash
docker compose -f infra/three-nodes/compose.yaml stop
```

不要用 `down -v`，那会删除三个数据卷。

下一章回到文件层面：日志段、保留与 compact。
