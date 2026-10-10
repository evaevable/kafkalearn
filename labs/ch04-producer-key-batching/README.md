# 第 4 章实战：真正的 Python Producer 与批处理

对应 [第 4 章讲义](../../chapters/04-producer-key-batching.md)，复用 [单节点 Compose](../../infra/single-node/compose.yaml)。

## 1. 从辅助脚本到 Kafka 客户端

前两章 Python 只检查输出，第 3 章 `lab.py` 只是生成输入和验收。本章 [app.py](app.py) 真正通过 `confluent-kafka` 连接 Kafka，发布记录并处理发送结果。

固定依赖 `confluent-kafka==2.13.2`，底层使用 librdkafka；这是可复现学习基线，不声称是最新版，也不等同于 Broker 的 Kafka 版本号。

本章执行三组实验，每组使用全新的三分区、单副本 Topic：

| 名称后缀 | 输入 | 目的 |
|---|---|---|
| `.keys` | 三个订单、各 created/paid 两条，共 6 条 | 检查显式 key、Java/Python 分区策略一致、回调坐标与实际存储 |
| `.serial` | 一个订单 key，sequence 0～119，共 120 条 | 每条记录后 flush，主动破坏跨记录累积机会 |
| `.burst` | 与 serial 相同的 120 条 | 循环 produce + poll(0)，末尾统一 flush，观察真实多消息批次 |

单节点、单副本、PLAINTEXT，不测试高可用、故障重试下幂等或外部业务 exactly-once。消费代码只直接 assign 分区回读，无 subscribe 消费组分工，无业务副作用，无位置提交。

## 2. 先隔离安装依赖

Docker/Compose v2 正常运行，Python 3.9+。宿主无需 Java。所有命令从仓库根目录执行。

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r labs/ch04-producer-key-batching/requirements.txt
PYTHON_BIN="$PWD/.venv/bin/python" bash labs/ch04-producer-key-batching/verify.sh
```

安装包只进入 `.venv`，不安装到系统 Python。`PYTHON_BIN` 可以是其他已经准备好、装有固定依赖的虚拟环境解释器路径。macOS/Linux 示例使用 `.venv/bin/python`；Windows 对应 `.venv/Scripts/python.exe`，本仓库 shell 验收需要 Bash 环境。

脚本自动判断 `docker compose` / 独立 `docker-compose` v2，并启动共享本地环境。**不自动安装依赖、不改全局配置、不删 Topic 或数据卷。**不要与第 2 章容器重建实验同时运行。

缺少 wheel 的平台可能需要额外构建环境；优先按客户端官方说明选择支持的平台与 Python，不为了安装方便使用不明来源二进制。

## 3. 一次入队，不等于一次成功送达

`Producer.produce()` 是异步 API：

```python
producer.produce(topic, key=order_id.encode("utf-8"),
                 value=json_bytes, on_delivery=on_delivery)
producer.poll(0)
```

这段只是接口摘录，完整可运行代码见 `app.py`。

- `produce()` 正常返回表示客户端接收记录、进入本地队列，不表示 Broker 已写入。
- 后台线程负责网络通信；`poll()` 主要服务 Python delivery 和 stats 回调，不是“不 poll 就绝对不发包”。
- `on_delivery(err, msg)`：读取最终投递报告；成功时记录 `topic/partition/offset`，失败时收集错误并失败退出。
- `BufferError`：当前这条记录未成功进入本地队列。代码有界 poll 等待、重试入队，超时仍失败；不是无条件重新发已经确认的业务事件。
- `flush(timeout)`：等待队列未完成项并服务回调，返回未完成数量；代码检查 pending、错误报告和回调总数。返回零不代表库存已更新。
- 投递超时等终局错误的结果可能不确定，不把错误直接解释成“Broker 上肯定没有消息”，也不自动做不受控的业务重发。

脚本的成功标准不只看 flush：还直接读取 Broker 上的记录，逐项核对事件、key、顺序、Offset 和投递回调坐标，确认本次全新 Topic 内没有额外消息。回读 Consumer 禁用 auto commit 和自动 Offset store，不 join 订阅组。

## 4. Python 与 Java 的默认分区算法不能想当然

上一章 Java CLI 的非空 key 按 Murmur2 路由。本客户端默认 `consistent_random` 使用 CRC32 对非空 key 路由，空 key 和 null key 还有特殊规则。为让两章的**非 null UTF-8 key**映射一致，本实验显式配置：

```python
"partitioner": "murmur2_random"
```

因此实际验证的映射是：

| key | 预期分区 |
|---|---|
| `order-3008` | 0 |
| `order-3001` | 1 |
| `order-3000` | 2 |

正文的 `order_id` 和 record key 分别传入，并非 Kafka 自动解析 JSON。未给 key、空字节 key、自定义 partitioner、显式 partition、修改分区数或序列化都可能改变行为，不能把这个映射推广成所有客户端所有配置的一般保证。

相同订单两个事件在本实验单个 Producer 里按 created/paid 顺序发布；这不是跨生产者业务因果或消费者多线程完成顺序保证。

## 5. 批处理实验固定了哪些条件？

两组 bulk 只改变调用方式（每条 flush vs 末尾 flush），其他参数与消息内容相同：

```python
"partitioner": "murmur2_random",
"enable.idempotence": True,
"acks": "all",
"max.in.flight.requests.per.connection": 5,
"message.timeout.ms": 15000,
"linger.ms": 200,
"batch.size": 65536,
"batch.num.messages": 1000,
"compression.type": "gzip",
"queue.buffering.max.messages": 10000,
"queue.buffering.max.kbytes": 10240,
"statistics.interval.ms": 100,
```

这些是教学值，不是生产最佳参数。

- 120 条记录全部使用一个 key，进入同一分区，避免多分区稀释批次观察。
- 每条 value 有序号与 512 字节重复 padding，便于观察批次及压缩，不代表真实订单大小分布。
- `linger.ms` 给累积机会，flush 会促使排空。它不是端到端时延 SLA，也不是所有记录必须等满 200ms。
- `batch.size` 是批次字节限制，含协议开销；librdkafka 会允许首条大消息形成单条批次，消息总大小仍有其他限制。不把它理解成“够 65536 字节才准发”。
- `batch.num.messages` 也限制条数，队列条数/kbytes 则限制整个 Producer 队列，二者不能混为一谈。
- 批次按 Topic/Partition 累积。一个 Produce 请求可以携带发给同一 Broker 的多个分区批次，**批次数不必等于请求数**。
- gzip 是批次压缩，消费者仍能读到 120 条独立事件，不是被合成一条大业务消息。

## 6. 真实统计如何验收？

`stats_cb` 每 100ms 产生 librdkafka JSON，经 poll/flush 服务。程序在发送完成后额外有界收集一段统计，不把这个采样等待算入发送耗时；元数据准备和回读也不算进该耗时。

保存指标：

- `topics[topic].batchcnt`：批次消息条数的窗口统计。
  - `cnt` 是这个窗口采样到的批次数，不是消息条数。
  - `sum` 是这些批次消息条数的总和。
  - `max` 是最大单批消息条数。
  - 把不重叠窗口的 sum/cnt 汇总得到观察到的平均每批条数。
- `brokers[*].req.Produce`：累计 Produce 请求发送次数，可能包含重试。它不是仅成功请求数、不是业务消息数；程序取累计快照最大值，不把连续快照相加。
- `txmsgs`：累计发送记录数；可能含重发，不代替回读核对。
- 发送耗时 / records per second：本机本次正常路径的学习对照，不是完整端到端、生产容量或严格可推广基准。

验收要求 keys/serial/burst 的投递报告全部成功、pending=0、回读内容完全一致；burst 实际最大每批条数 > 1，且 Produce 请求数少于 serial。若本次统计不完整或对照不成立，程序报错并保留结果，不伪造固定数字或声称“批处理永远快多少倍”。

本客户端内部统计是异步的，各层指标可能有时点差异。采样只是投递与回读之外的观察证据；没有做网络断开、Leader 切换或压力边界测试。

## 7. 输出文件与加练

结果位于 `labs/ch04-producer-key-batching/results/<runid>/`，已被 Git 忽略：

- `run.env`：本次 Topic 前缀；后缀分别为 `.keys`、`.serial`、`.burst`。
- `<mode>.topic.txt`：实际三分区单副本描述。
- `keys.json` / `serial.json` / `burst.json`：配置、客户端版本、回调、实际回读、原始统计快照、汇总统计与耗时。
- `summary.json`：三组精简对照。

```bash
.venv/bin/python labs/ch04-producer-key-batching/app.py self-test
```

9 项自测不连 Broker：UTF-8 JSON 字节、六条样本、入队、BufferError 等待重试/超时、pending 错误、delivery 错误、缺回调、窗口与累计计数口径。它们验证分支逻辑，**不冒充真实断网或幂等故障实验**。

加练：

1. 对照 keys.json 的 callback 坐标和 readback，找出 `order-3001` 的支付位置。
2. 解释为什么 serial/burst 都是 120 条事件，Produce 请求数量却不同。
3. 在全新 Topic 和明确新运行中改变 linger 或 batch.size，比较请求与批次，不在原已写入 Topic 上重复跑自动验收。
4. 看看 default CRC32 与显式 Murmur2 对你的新 key 是否一致；不同算法也可能碰巧落同一分区，不以一个碰巧结果证明兼容。

## 8. 排错与停止

| 现象 | 检查 |
|---|---|
| `ModuleNotFoundError` | PYTHON_BIN 是否指向装有固定依赖的虚拟环境，不是系统 Python |
| 客户端版本不符 | 用该解释器安装 requirements.txt，别默默用其他版本跑 |
| 宿主连接失败 | Broker 启动健康、端口和 advertised.listeners；本章宿主用 127.0.0.1:19092 |
| 入队 BufferError | 本地队列压力，是否持续服务回调与设置有界等待；不要无限重发 |
| flush pending / delivery 错误 | 查看错误，不把失败退出当成功；排查网络/元数据/消息大小/超时 |
| key 映射不一致 | partitioner、key 字节、分区数、显式分区；默认 CRC32 与 Java Murmur2 差异 |
| bulk 统计比较失败 | 查看 summary、实际负载与请求重试；没有统计就不写实验结论 |
| 回读多出记录 | 是否误用了已有 Topic；自动验收每次唯一 Topic，不删数据掩盖问题 |

完成后停止，保留数据：

```bash
docker compose -f infra/single-node/compose.yaml stop kafka
```

## 9. 作者实跑验收

2026-10-10，macOS + Colima Docker Engine 27.0.3 / 独立 Compose 2.29.1 / Python 3.13.12，固定 Python 客户端及 librdkafka 均为 2.13.2，Broker 为 Kafka 4.3.1。两次隔离运行通过，以下为最终代码一次运行的实际值，不是每次固定预期：

| 模式 | 成功投递与实际回读 | 观察到批次数 | 最大每批记录数 | 平均每批记录数 | Produce 请求数 | 发送耗时 |
|---|---:|---:|---:|---:|---:|---:|
| keys | 6 | 3 | 2 | 2 | 3 | 约 4.05 ms |
| serial | 120 | 120 | 1 | 1 | 120 | 约 149.08 ms |
| burst | 120 | 2 | 100 | 60 | 2 | 约 7.08 ms |

- 三组均无投递错误，flush 后 pending=0；key、事件、分区内顺序和回调坐标与 Broker 回读完全一致。
- Python 显式 Murmur2 路由与上一章 key 映射一致。
- 最终统一 flush 前，burst 的成功回调数为 0；之后为 120。这是本次应用已服务的回调数量，不据此判断此前后台网络发送数量。
- 9 项假客户端/序列化/统计口径自测，以及全仓库图谱、Bash/Python/Compose 与本地引用校验通过。
- 最终输出保留 `statistics_snapshots` 原始统计快照，方便核对汇总值。

小样本、热身程度、单 key、重复 padding 与本地网络都会影响数据。我们只证明这两次正常运行中批次与请求有差异，不证明 200ms 是最佳 linger，不外推“生产一定快多少倍”。没有实测网络断开、协议重试去重或业务 exactly-once。

## 10. 官方参考

- [Python 客户端概览](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [固定 librdkafka 2.13.2 配置](https://github.com/confluentinc/librdkafka/blob/v2.13.2/CONFIGURATION.md)
- [固定 librdkafka 2.13.2 统计](https://github.com/confluentinc/librdkafka/blob/v2.13.2/STATISTICS.md)

下一章 Consumer Group，才开始真实组内并行分工。
