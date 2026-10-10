# 第 1 章实战：写一次、独立读取、从头重放

对应讲义：[第 1 章](../../chapters/01-why-kafka.md)。

## 1. 本次要证明什么

1. Producer 把 3 条订单事件写入一个 Partition。
2. `inventory` 和 `analytics` 是两个独立消费组，各自读到相同的 3 条记录。
3. 直接从分区 0、Offset 0 再读一遍，记录仍然存在。

本章只验证收发与重放，不模拟真正扣库存或落分析数据库，也不验证已提交 Offset、幂等处理或容灾。不同事件属于不同订单，不代表同一订单的完整生命周期。

## 2. 前置条件与网络视角

- 已安装 Docker，且 daemon 正常运行。
- Docker Compose v2，建议 v2.20+，支持 `up --wait`。
- macOS 可使用 Docker Desktop 或 Colima，任选其一，不要求两者同时运行。
- 实验镜像固定为 `apache/kafka:4.3.1`，宿主机无需安装 Java。
- 所有下列命令均从仓库根目录运行。

```bash
docker version
docker compose version
```

如果通过 Homebrew 安装后 `docker compose` 不可用，但 `docker-compose version` 显示 v2，则下文所有 `docker compose` 可替换为 `docker-compose`。验收脚本自动兼容两种入口，不改系统配置。

`docker version` 必须同时展示 Client 与 Server。只有 Client、提示不能连接 daemon，说明运行时没有启动；Colima 用户可执行 `colima start`，Docker Desktop 用户启动应用。

| 访问方 | Bootstrap 地址 | 用途 |
|---|---|---|
| Kafka 容器内 CLI / Compose 网络内客户端 | `kafka:9092` | 本章命令使用这个地址 |
| 宿主机应用客户端 | `127.0.0.1:19092` | 后续 Python 实验使用 |
| KRaft 控制器通信 | `kafka:9093` | 不供生产者、消费者连接 |

单节点同时运行 Broker 与 Controller 两个逻辑角色，不等于它们职责相同。第 2 章再拆解这些配置。

## 3. 一键验收

```bash
docker compose -f infra/single-node/compose.yaml up -d --wait
bash labs/ch01-hello-kafka/verify.sh
```

第一条拉取镜像、创建命名数据卷并启动 Kafka，等 Topic CLI 健康检查通过。第二条创建唯一 Topic 与两个新消费组，执行生产、独立消费和重放，然后逐字节核对输出是否与 [events.jsonl](events.jsonl) 一致。

预期输出的关键部分如下（时间和组名每次不同）：

```text
Creating isolated topic: shopflow.orders.ch01.<runid>
Producing 3 order events...
PASS: inventory group read the same 3 events in partition order.
PASS: analytics group read the same 3 events in partition order.
PASS: direct replay read the same 3 events in partition order.
PASS: chapter 01 acceptance checks completed.
```

输出保存在 `labs/ch01-hello-kafka/results/<runid>/`，该目录被 Git 忽略，不上传个人实验结果：

- `run.env`：本次 Topic 和消费组名称。
- `topic.txt`：分区与副本描述。
- `inventory.jsonl` / `analytics.jsonl`：两个组分别读到的记录。
- `replay.jsonl`：直接重放的记录。
- `offsets.txt`：带分区和 Offset 的输出。

每次运行使用新 Topic、新组，不删除已有实验数据。后续累计的 Topic 会占用资源，不适合无限制循环跑。

## 4. 拆开脚本，理解每个动作

如果一键实验通过，再手动做一次。设置一个不会重复的实验名：

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM"
TOPIC="shopflow.orders.manual.$RUN_ID"
```

### 4.1 建一条日志

```bash
docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1
```

创建一个 Topic，其中只有一条日志分区。本章单副本是为了简化观察，不能抵抗该节点的数据丢失。

### 4.2 追加三条记录

```bash
docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic "$TOPIC" \
  --command-property acks=all < labs/ch01-hello-kafka/events.jsonl
```

每一行 JSON 是一条消息。`-T` 关闭伪终端，方便输入重定向与脚本运行。`acks=all` 仅描述 Kafka 写入确认，单副本实验并没有“所有节点备份”的保障，更不代表下游业务完成。

### 4.3 库存组读三条

```bash
docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic "$TOPIC" \
  --group "inventory.manual.$RUN_ID" --from-beginning \
  --max-messages 3 --timeout-ms 30000
```

新组没有已提交位置，从当前保留的开头开始读。最多读三条后退出；30 秒内无法得到下一条时超时报错，避免无限等待。这里的 CLI 是观察工具，退出与自动提交的时机不作为本章验收条件。

### 4.4 分析组也读三条

```bash
docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic "$TOPIC" \
  --group "analytics.manual.$RUN_ID" --from-beginning \
  --max-messages 3 --timeout-ms 30000
```

变化只有消费组名称。它也应该读到三条，证明不同组独立消费，而不是第二个组只能收到“库存没抢走的记录”。

### 4.5 不改消费组，从指定位置直接重放

```bash
docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic "$TOPIC" \
  --partition 0 --offset 0 --max-messages 3 --timeout-ms 30000 \
  --formatter-property print.partition=true --formatter-property print.offset=true
```

预期能看到分区 0 内的 Offset 0、1、2，对应三条原始事件。这个命令是直接指定分区读取，不把某个已有组的位置重置为 0。

不要误记成“加 `--from-beginning` 就让任何已有组回头”：有有效已提交位置的组通常继续从提交位置读。真正的组位置重置会在第 6 章单独解释。

## 5. 加练：第四条记录

沿用手动实验同一个终端里的 `$TOPIC`，追加一条新事件：

```bash
printf '%s\n' '{"event_id":"evt-004","event_type":"order.created","order_id":"order-1004","amount_fen":12900,"schema_version":1}' | \
  docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic "$TOPIC"

docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic "$TOPIC" \
  --group "practice.$RUN_ID.$RANDOM" --from-beginning \
  --max-messages 4 --timeout-ms 30000 \
  --formatter-property print.offset=true
```

验收：新组读到 4 条，第四条位于 Offset 3。你发送了 4 条，不是因为多读几次就变成 12 条。

## 6. 常见错误

| 现象 | 先检查什么 |
|---|---|
| `Cannot connect to the Docker daemon` | Docker Desktop/Colima 是否启动，`docker context show` 是否指向正确运行时 |
| `proxyconnect ... connection refused` | Docker daemon 的代理是否指向已退出的临时代理；`docker info` 查看 HTTP/HTTPS Proxy，不把它误判为 Kafka 故障 |
| 拉取镜像超时 | 核对 registry 连通性、运行时代理和网络策略，不随意换成不明镜像 |
| `port is already allocated` | 宿主端口 `19092` 被占用；核对 Compose 中监听与对外声明地址再改端口 |
| 容器启动但 unhealthy | `docker compose -f infra/single-node/compose.yaml logs --tail 100 kafka` 查看日志 |
| 宿主机连接 `kafka:9092` 失败 | `kafka` 是 Compose 内部 DNS 名，宿主应用用 `127.0.0.1:19092` |
| 消费者读不到预期记录 | 核对 Topic 名称；手动是否仍在同一终端；有没有误用旧组；检查 stderr 超时错误 |
| `cmp` 报不一致 | 打开本次结果 JSONL，核对是否少消息或重复，不只看“行数对了” |

## 7. 停止，但保留数据

```bash
docker compose -f infra/single-node/compose.yaml down
```

这会移除本实验容器和网络，保留 `kafkalearn_kafka-data` 命名卷。下次启动仍可访问尚未被保留策略清理的历史。不要默认加 `-v`：它会删除实验卷及其日志。脚本不会删除 Topic，也不会自动清卷。

## 8. 作者验收记录

2026-10-10，在 macOS + Colima Docker Engine 27.0.3、独立 Docker Compose 2.29.1 环境中完成两次隔离实验，第二次使用本仓库最终参数版本：

- Kafka `4.3.1` 单节点 KRaft 健康启动。
- Topic 只有 1 个分区、1 个副本，Leader/ISR 为节点 1。
- 库存组、分析组分别完整读取 3 条事件，逐字节与源 JSONL 一致。
- 直接从分区 0 / Offset 0 重放 3 条，内容一致。
- 可见位置依次为 `Partition:0 Offset:0`、`Offset:1`、`Offset:2`。
- Bash 语法、Compose 配置、讲义 Mermaid 语法和仓库内文档链接校验通过。

此记录只证明本章实验在上述环境通过，不代表后续尚未交付章节或生产可靠性验收完成。

## 9. 实验边界

- 本地 PLAINTEXT、单节点、单副本，不用于生产。
- 消费并不删除记录，但保留策略和显式管理操作可以删除历史；配置默认按时间保留约 7 天，删除不是精确到秒即时执行。
- `inventory` / `analytics` 在本章只是组名，没有真正业务处理。后续章节再加入代码、去重、落库和故障恢复。
- 第 1 章通过后进入第 2 章，解释环境与 KRaft；别急着背可靠性参数。
