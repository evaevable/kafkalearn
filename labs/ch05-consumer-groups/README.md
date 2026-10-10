# 第 5 章实战：同组分工，组间独立

对应 [第 5 章讲义](../../chapters/05-consumer-groups.md)，复用 [单节点 Compose](../../infra/single-node/compose.yaml) 和前章固定客户端 `confluent-kafka==2.13.2`。

## 1. 本次不再手工 assign

前三章 CLI 观察、第四章 Consumer 回读时都没有做真正的订阅分配。这次每个工作进程创建一个 Consumer，并调用 `subscribe([topic])` 加入普通消费组。

| 消费组 | 进程/Consumer 实例数 | Topic 分区数 | 稳定分配的预期 |
|---|---:|---:|---|
| inventory | 4 | 3 | 三人各一个分区，一人空闲 |
| analytics | 2 | 3 | 一人两个分区，另一人一个 |

两个组都订阅同一个全新订单 Topic，但 `group.id` 不同，各自读取完整的 12 条事件。不是把消息复制成两份存储，也不是六个人一起分掉唯一的 12 条。

范围限定：本章普通消费组、`group.protocol=classic`、`partition.assignment.strategy=range`、仅订阅本次一个 Topic。不要把分区上限推广到 Share Group 等其他消费模型。Kafka 4.x 的新 consumer protocol 可以采用服务端分配，具体机制不与本章 classic 混用。

## 2. 准备环境与运行

要求 Docker/Compose v2 正常运行，Python 3.9+。若已经有第 4 章虚拟环境，可直接复用；否则从仓库根目录：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r labs/ch05-consumer-groups/requirements.txt
PYTHON_BIN="$PWD/.venv/bin/python" bash labs/ch05-consumer-groups/verify.sh
```

依赖只装在虚拟环境，不装到系统 Python。`PYTHON_BIN` 可指定已有的实际解释器路径。脚本兼容 `docker compose` 与独立 `docker-compose` v2，不自动安装依赖、不改全局 Docker 配置。

**这次会启动六个本地工作进程，仅用于教学；不要与第 2 章容器重建实验同时运行。**脚本不删 Topic、消费组或卷，不重建已有容器，不自动停止 Kafka。

每次 Topic 名为 `shopflow.orders.ch05.<runid>`，组名为 `shopflow.ch05.<runid>.inventory` / `.analytics`。唯一名称隔离旧数据与旧组进度，实际名称以 `results/<runid>/run.env` 为准。

## 3. 先等稳定分配，再发送消息

实验不是“六个进程一启动就立刻发，然后看到重复也算成功”。执行顺序是：

1. 创建唯一 Topic，三个分区、一个副本，Broker 为节点 1。
2. 启动 inventory-1～4 和 analytics-1～2 六个 Consumer 进程。
3. 控制程序用 `AdminClient.describe_consumer_groups` 查询 Broker 上真实的成员与分区分配。
4. 等待两个组均为 `STABLE`，成员完整，分区覆盖恰为 0、1、2且组内没有重叠；再次查询获得相同分配，并确认工作进程 `consumer.assignment()` 与服务端视图一致。
5. 保存成员仍在线时的 JSON 与 CLI 视图，复核分配后才发送 12 条事件。
6. 保持全部成员运行，直到两个组各读到 12 条，再保存现场、核对稳定阶段结果。
7. 验收后通知工作进程退出，调用 `Consumer.close()`，有限等待子进程；超时或非零退出报错。

启动期间可能因成员加入出现重平衡，这是正常建组过程；本章把它与“稳定后读取”分开。最后成员退出引起的重新分配仅是收尾，**不作为故障恢复或 Offset 恢复实验**。

## 4. Consumer 的关键配置

实际代码见 [app.py](app.py)：

```python
"bootstrap.servers": "127.0.0.1:19092",
"group.id": group_id,
"client.id": worker_name,
"group.protocol": "classic",
"partition.assignment.strategy": "range",
"auto.offset.reset": "earliest",
"enable.auto.commit": False,
"enable.auto.offset.store": False,
"allow.auto.create.topics": False,
"session.timeout.ms": 10000,
"heartbeat.interval.ms": 1000,
"max.poll.interval.ms": 60000,
```

- `group.id` 决定协作身份，`client.id` 只是辨识客户端的名字，不能替代组名。相同 client.id 不会自动加入相同组。
- `subscribe()` 参与组订阅与分区分配；第四章 `assign()` 是直接指定分区，不参与订阅组的自动分配。
- `auto.offset.reset=earliest` 只在没有有效已提交位置时决定起点，不强制旧组每次回头。本章组全新、历史完整，所以从头读。
- 自动提交与自动保存待提交位置都关闭，也没有显式 `commit()`。读完不会持久保存恢复进度；重启可能重读，不能把本章无重复当成故障情况下 exactly-once。
- 超时与心跳参数是便于本地实验的教学值。即使库后台发心跳，应用长期不调用 poll 也可能触发生命周期限制，详见第 7 章。
- `on_assign` / `on_revoke` 记录生命周期，默认让客户端完成实际赋予/撤销。回调不拿来重新指定分区或提交本章未处理位置。

## 5. 为什么不是每个进程读 12 条？

同组 Consumer 是协作，分配的是分区，不是每条消息随机抽签。只有 3 个分区，却有 4 个库存成员，不能给第四人分到“半个分区”。其 assignment 为空、没有本次记录，是预期结果，不是启动失败。

分析组只有 2 个成员，三个分区仍会全部分配：其中一个成员负责两个分区，不是第三个分区无人看管。

本实验每分区有 4 条记录，所以预期结果是：

```text
inventory 组：4 + 4 + 4 + 0 = 12 次读取
analytics 组：8 + 4 = 12 次读取
```

谁拿到哪个分区取决于成员 ID、排序与分配；不要把某个工作进程名字与特定分区永久绑定。不同组可以各自获得相同的分区，这是组间独立订阅，不是冲突。

## 6. 12 条事件怎么构造？

沿用前章三种订单 key，用单个 Producer 发布，每个订单依次：

```text
order.created → order.paid → order.packed → order.shipped
```

`partitioner=murmur2_random`、相同 UTF-8 key 与固定三分区，使 `order-3008/3001/3000` 分别进入分区 0/1/2。每分区四条 Offset 为 0/1/2/3。输入先发三个订单的创建，再发支付、打包、发货，保证每个订单本身的输入先后顺序。

每条 value 包括独立 `event_id`、`order_id`、`event_type`、`sequence` 与版本。这里只观察事件，没有真的扣库存、扣款或发货，因此读取成功不表示业务动作完成。

## 7. 保存真实成员视图

每次输出在 `labs/ch05-consumer-groups/results/<runid>/`，该目录不上传：

- `run.env` / `topic.txt`：实际 Topic、组名与分区描述。
- `inventory-N.json` / `analytics-N.json`：每个工作进程的 PID、组、当前 assignment、poll次数、生命周期与实际读取记录；收尾后 closed=true。
- 每个进程的 `.log`：库日志与错误现场。
- `before.groups.json` / `after.groups.json`：发布前后成员与分配的真实 Admin API 结果。
- `before.worker-reports.json`：发布前的worker基线；验收检查之后没有新增分配/撤销回调，避免相同分配重新发生却被误判为持续稳定。
- `before/after.<group>.members.txt`：CLI 成员、CLIENT-ID、分区分配。
- `before/after.<group>.state.txt`：CLI 协调器、assignor、状态和成员数。
- `producer-deliveries.json`：12 条消息的投递结果与坐标。
- `stable.worker-reports.json`：全部成员仍在线时的读取快照，避免把收尾撤销误当实验分配。
- `summary.json`：两个组的读数、分区归属与本次结论。

CLI 视图是脚本在成员仍在线时自动采集的。若手动观察，先从结果的 `run.env` 取出实际组名，并在工作进程仍运行时执行以下同等命令：

```bash
docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --describe --group "$INVENTORY_GROUP" --members --verbose

docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --describe --group "$ANALYTICS_GROUP" --state
```

这里只查询，不添加成员、不改 Offset。脚本返回后工作进程已经关闭，再查组可能是 Empty 或没有活跃成员，**不能拿收尾后的空组否定运行中证据**。本章没有提交 Offset，普通 `--describe` 的 CURRENT-OFFSET/LAG 可能缺失，不据此声称 lag=0 或消息处理已持久完成。

## 8. 验收与局限

程序断言：

- 库存组恰有4成员、分析组恰有2成员，稳定时组内分区0/1/2全部覆盖且不重叠。
- Worker 本地分区归属与 Admin 真实分配相符；实际读到的记录都来自当时拥有的分区。
- 每组恰12条，完整 key/value/Topic 分区坐标符合源事件，无重复、无遗漏，分区内观察顺序0/1/2/3。
- 库存成员读数为4/4/4/0，分析为8/4；两组都读到同一套事件，合计24次读取但只生产12条。
- 发布前后稳定分配保持一致，最后所有子进程正常关闭。

自测不连 Broker：

```bash
.venv/bin/python labs/ch05-consumer-groups/app.py self-test
```

八项测试检查样本、空闲成员、分配重叠/缺分区、重复记录、错误归属、乱序和不稳定组拒绝。这些是验收逻辑测试，不冒充 Broker 宕机或网络断开实验。

实验结果只说明**本次稳定阶段的分工与独立读取**。故障、撤销重分配、未提交进度、应用重复业务处理都可能导致重读；组机制不自动保证业务 exactly-once，也不是永久“不重复消费”的承诺。

## 9. 排错与停止

| 现象 | 先核对 |
|---|---|
| 无法连接宿主 Broker | 127.0.0.1:19092、容器状态、元数据对外地址 |
| 依赖缺失或版本不符 | PYTHON_BIN 是否指向第4章准备好的固定依赖虚拟环境 |
| 组无法达到稳定 | 各worker.log、客户端进程是否还活着、组名是否错误、Broker状态 |
| inventory 一个消费者零条 | 它在成员列表吗、assignment为空吗？三分区四成员时这是预期 |
| inventory与analytics各只读一部分 | 是否误用了同一个group.id导致两个业务被分进同一组 |
| 无提交位置、Lag为横线 | 本章故意没有commit，不据此认定未读取或读取可靠完成 |
| 同组出现重复 | 是否发生额外rebalance/重启、复用旧组或旧Topic；不要忽略断言 |
| 程序退出后成员列表为空 | 收尾已close，查看before/after运行中保存证据 |

正常/失败收尾都会通知本次启动的工作进程关闭。Topic和组元数据保留，Kafka服务仍运行。完成后可停止，不删数据：

```bash
docker compose -f infra/single-node/compose.yaml stop kafka
```

下一章提交 Offset，再验证重启与重复处理边界。

## 10. 作者实跑验收

2026-10-10，在 macOS + Colima Docker Engine 27.0.3 / 独立 Compose 2.29.1 / Python 3.13.12 环境中，两次隔离实验通过。固定 Python 客户端/librdkafka 2.13.2，Broker Kafka 4.3.1。

最终一次运行实际分配如下（具体成员名称不是跨运行保证）：

| 组 | Worker | 分区 | 实际读到记录 |
|---|---|---|---:|
| inventory | inventory-1 | 0 | 4 |
| inventory | inventory-2 | 1 | 4 |
| inventory | inventory-3 | 2 | 4 |
| inventory | inventory-4 | 无 | 0 |
| analytics | analytics-1 | 0、1 | 8 |
| analytics | analytics-2 | 2 | 4 |

- 两组各12条完整事件、共24次读取；只生产12条，回调坐标正确。
- 发布前后成员与分配稳定；发布后未发生新增 assign/revoke 回调，实际owner与服务端分配一致。
- 每组各分区Offset0/1/2/3顺序正确，key与完整事件均匹配，没有重复或遗漏。
- 库存空闲成员实际在CLI成员表中且分区数为0，不是缺席进程。
- CLI显示协调器为Broker1、assignor为range，两组Stable，成员数分别4和2；所有六个子进程正常关闭。
- 8项本地自测、Bash/Python/Compose、全部章节10张Mermaid以及文档引用通过。

未提交消费位置，未验证重启恢复、故障重平衡或外部业务exactly-once。所有分区仍在单一Broker上，未验证多机扩容性能。

## 11. 官方参考

- [Kafka 4.3 Consumer 配置](https://kafka.apache.org/43/configuration/consumer-configs/)
- [Python 客户端说明](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [固定 librdkafka 配置](https://github.com/confluentinc/librdkafka/blob/v2.13.2/CONFIGURATION.md)
