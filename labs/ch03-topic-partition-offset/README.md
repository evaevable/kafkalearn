# 第 3 章实战：把一个 Topic 拆成三条日志

对应 [第 3 章讲义](../../chapters/03-topic-partition-offset.md)。复用 [单节点 Compose](../../infra/single-node/compose.yaml)，不增加 Broker。

## 1. 要亲眼看到什么

- 一个 Topic 包含三个 Partition，不是复制三份全部消息。
- 三个订单各两条事件：先 `order.created`，后 `order.paid`，总共六条。
- 消息 key 明确设置为订单 ID；三个选定 key 各落一个分区，同一订单两条记录同分区。
- 每个分区都有自己的 Offset 0、1；三个分区可以各有一条 Offset 0，不是同一条消息。
- 每个分区 log end offset 为 2，即下一条可追加位置，不是最后一条消息的 Offset 1。
- 指定 Topic、Partition 1、Offset 1，准确读取那个分区的第二条事件。

**本章仍只有一个 Broker、单副本、PLAINTEXT，仅用于本地教学。**三个分区全部 Leader 都是节点 1；不是三台机器、不是三副本，也不演示多 Broker 吞吐提升或多个消费者分配。Consumer Group 并行实跑留到第 5 章。

## 2. 运行

需要 Docker 正常运行、Docker Compose v2、Python 3.9+。Python 只用于生成输入与核对结果，**`lab.py` 不是 Python Kafka 客户端**；真正收发消息的是容器内官方 CLI。

在仓库根目录执行：

```bash
bash labs/ch03-topic-partition-offset/verify.sh
```

脚本兼容 `docker compose` 与独立 `docker-compose` v2，自动启动共享本地 Kafka 环境，使用唯一 Topic `shopflow.orders.ch03.<runid>`。不会强制重建已有容器、删 Topic、删卷、重置消费组位置或自动停机。不要与第 2 章的重建实验同时运行。

Python 解释器可选择本机实际路径：

```bash
PYTHON_BIN="/absolute/path/to/python3" bash labs/ch03-topic-partition-offset/verify.sh
```

上面路径是说明占位，执行前替换为真实路径。不需要安装任何 Python 包或宿主 Java。

关键输出：

```text
PASS: 3 partitions / 1 replica; all leaders on the single broker.
PASS: Exactly 6 events; each key stays in its partition, created before paid.
PASS: Each partition has offsets 0,1; log end offset is 2.
PASS: Partition 1 / offset 1 locates the expected paid event.
PASS: Two constructed traversals differ while preserving each partition's order.
NOTE: Traversals are examples, not a live consumer interleaving experiment.
PASS: chapter 03 acceptance checks completed.
```

## 3. 先看实验生成的输入

每次结果在 `labs/ch03-topic-partition-offset/results/<runid>/`，已被 Git 忽略。运行输出会告诉你实际位置。

- `run.env`：本次 Topic 名。
- `input.txt`：六行 `key|JSON`，可直接输入生产者。
- `expected.json`：预期 key、分区、Offset 和事件。
- `topic.txt`：实际三分区、单副本、Leader 信息。
- `partition-0.txt` / `partition-1.txt` / `partition-2.txt`：按分区读取的实际结果。
- `end-offsets.txt`：各分区 log end offset。
- `seek-partition-1-offset-1.jsonl`：精确定位的单条 JSON。
- `coordinates.json`：把 Topic/Partition/Offset 与消息关联起来的坐标表。
- `two-traversals.json`：同一批已观察记录的两种**程序构造**遍历顺序。

输入像这样（实际 key 以生成文件为准）：

```text
order-3001|{"event_id":"order-3001-0","order_id":"order-3001","event_type":"order.created","schema_version":1}
```

竖线左边是 Kafka key，右边是消息 value。value 里面也有 `order_id`，但 Kafka 不会自己解析 JSON，把那个字段自动变成 key；这里是我们主动把二者设成相同字符串。

生成程序按固定版本 Java 客户端默认 `toPositive(murmur2(UTF8(key))) % 3` 选择三个 key，让每个分区都得到两条消息。这样避免某个分区没有数据而读不到预定两条。这是**精心构造的可复现实验**，不是证明任意三个订单自然均匀分布，也不是所有语言、客户端、自定义分区器的通用算法。

我们先输入三个订单的 created，再输入三个订单的 paid。每个订单都先创建再支付，且两个事件 key 相同。单个生产者、固定三分区、稳定序列化和默认分区策略是本次验证条件；应用多线程处理与跨生产者业务因果不在本次测试范围。

## 4. 手动拆解实验

自动验收通过后可以手动做一次。所有命令在仓库根目录执行；本机只有独立 Compose v2 时，把 `docker compose` 替换为 `docker-compose`。

### 4.1 生成六条输入并创建唯一 Topic

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM"
TOPIC="shopflow.orders.ch03.manual.$RUN_ID"
RESULTS="labs/ch03-topic-partition-offset/results/manual-$RUN_ID"
python3 labs/ch03-topic-partition-offset/lab.py generate "$RESULTS"

docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic "$TOPIC" --partitions 3 --replication-factor 1
```

创建的是一个 Topic、三个不同分区、每个分区一个副本。此实验不是改第 1 章的 Topic，也不搬动旧数据。

### 4.2 一个生产者写入带 key 的消息

```bash
docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 \
  --topic "$TOPIC" \
  --reader-property parse.key=true --reader-property 'key.separator=|' \
  --command-property acks=all --command-property enable.idempotence=true \
  < "$RESULTS/input.txt"
```

- `parse.key=true`：一行里既包含 key，又包含 value。
- `key.separator=|`：用第一个竖线分隔 key 和 JSON，不是让 Kafka 在 JSON 里自动找字段。
- `--reader-property`：配置输入解析器。固定 Kafka 4.3.1 的旧 `--property` 已废弃，本章使用新的入口。
- `--command-property`：配置真正的 Producer；幂等与 `acks=all` 用于稳定本实验发送，不能推导“外部库存业务 exactly-once”。单副本依然没有节点容灾能力。

生产者在客户端选择分区，再请求相应 Leader 写入；不是 Broker 看到 JSON 后按订单 ID 随机分配。

### 4.3 各分区从 Offset 0 读取两条

```bash
for p in 0 1 2; do
  docker compose -f infra/single-node/compose.yaml exec -T kafka \
    /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
    --topic "$TOPIC" --partition "$p" --offset 0 \
    --max-messages 2 --timeout-ms 30000 \
    --formatter-property print.partition=true \
    --formatter-property print.offset=true \
    --formatter-property print.key=true \
    --formatter-property print.value=true
done
```

每个分区应显示 Offset 0 / 1，key 相同，created 在 paid 前面。这是直接分区读取，不用 group 订阅分配，也没有重置任何已有组的位置。

`--max-messages 2` 防止读完预定条数后一直等，`--timeout-ms 30000` 防止缺少消息时无限等待；如果某分区没得到数据，会报错而不是伪装成通过。

### 4.4 查下一追加位置

```bash
docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 \
  --topic "$TOPIC" --time -1
```

每一行形如 `<topic>:<partition>:2`。`2` 表示当前日志末端的下一位置；最后两条记录的位置是 `0` 和 `1`。

这是 Broker 日志位置，不是某消费组已提交位置。**Offset 也不是永久的记录计数器**：在本次全新、无事务、未清理实验中恰好两条对应 log end 2；生产里清理、事务控制记录等会让可见记录数与位置不能简单等同。第 6、9、10 章分别展开。

### 4.5 定位某一条记录

```bash
docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic "$TOPIC" --partition 1 --offset 1 \
  --max-messages 1 --timeout-ms 30000
```

只读分区 1 的第二条事件，应为该分区订单的 `order.paid`。只说“Offset 1”不完整，另外两个分区也各有自己的 Offset 1。

## 5. 跨分区顺序：什么已观察，什么只是示意？

脚本实际读取三个分区，并分别验证每个分区的两条消息顺序。然后 `lab.py` 把相同的观测结果构造成：

- 先完整展示分区 0，再分区 1，再分区 2。
- 先完整展示分区 2，再分区 1，再分区 0。

两种展示都保持每个分区内 created 在 paid 前面，却有不同的总顺序。由此直观理解：**单靠三条分区日志，没有一个 Kafka 承诺的全局唯一读取顺序。**

但不要扩大实验结论：这不是两个真实订阅消费者的随机交错运行，也不证明任何多分区 Topic 每次都会乱序。全局顺序不被保证是协议/模型边界；程序构造遍历只是帮助理解这个边界的教材示例。

如果应用自己按事件时间合并排序，还需要处理迟到、时钟、因果和何时认为数据完整等问题；不是“有一个时间戳字段”就自动获得 Kafka 全局顺序。

## 6. 校验与加练

```bash
python3 labs/ch03-topic-partition-offset/lab.py self-test
```

六项标准库自测覆盖：Murmur2 空输入测试向量、fixture 分区覆盖、格式解析、非法格式拒绝、错误 Offset 拒绝、分区内颠倒顺序拒绝。真正 key 分区正确性还以 Broker 上实际读取结果核对为准。

加练，不改已经通过的自动验收 Topic：

1. 打开 `coordinates.json`，找到三个分区各自 Offset 0，解释为什么是三条不同记录。
2. 用手动实验的 Topic 改成 `--partition 2 --offset 1`，预期读到另外一个订单的 paid。
3. 只修改 JSON 里的 `order_id`，不改竖线左侧 key，预测是否改变路由；在新 Topic 验证。不要把 value 字段当成隐式 Kafka key。
4. 画一个“四个分区、两个 Consumer”的常规组分配示意：消费者可以各负责多个分区，不是超出消费者数的分区永远无人读取。第 5 章再观察真实分配。

暂不在共享实验 Topic 上增加分区：旧记录不会自动重分布，新 key 路由却可能改变；也不能用 alter 直接减少分区。第 4 章会从 Producer 行为进一步解释。

## 7. 作者验收记录

2026-10-10，macOS + Colima Docker Engine 27.0.3 / 独立 Compose 2.29.1 / Python 3.13.12，Kafka 镜像 `apache/kafka:4.3.1` 实跑通过：

- 6 项生成器与验收器自测全部通过。
- 三个分区、单副本，三个分区 Leader/Replicas/ISR 均为 Broker 1。
- 实际 key 路由：`order-3008` → 分区 0，`order-3001` → 分区 1，`order-3000` → 分区 2。
- 每个分区都读到同一订单的 created / paid 两条，Offset 为 0 / 1；六条消息无重复、无遗漏。
- 三个分区 log end offset 均为 2。
- 分区 1 / Offset 1 精确读到 `order-3001` 的 `order.paid`。
- 两种程序构造遍历总序不同、各分区内部顺序一致，未进行真实多消费者交错实验。
- Bash、Python、Compose、全仓库 Mermaid 与本地引用校验通过。

本记录不代表多 Broker 性能、高可用或 Consumer Group 分配验证。

## 8. 排错与停止

| 现象 | 先检查 |
|---|---|
| daemon 不可用 | Docker/Colima 是否启动，Docker context 是否正确 |
| Python 不存在 | 设置 `PYTHON_BIN`，确认版本至少 3.9 |
| 分区读超时 | Topic 是否正确、输入是否确实包含 key、parse.key 是否开启、分区策略是否被修改 |
| 同 key 不同分区 | 有没有显式指定分区、自定义分区器、ignore keys、变更分区数或不同序列化 |
| 格式解析失败 | `partition-N.txt` 是否含非数据行；检查 stderr 与固定CLI版本/formatter参数 |
| expected 与实际不同 | 不跳过验收；查看 input、Topic 描述、key/offset输出，定位实际差异 |

验收脚本保留容器与数据，失败也保留结果供检查。完成后可以停止，不删数据：

```bash
docker compose -f infra/single-node/compose.yaml stop kafka
```

不要默认用 `down -v` 或 `docker volume prune` 清理。下一章学习 Producer 的发送、key、序列化与批处理。
