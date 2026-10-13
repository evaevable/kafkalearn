# 第 6 章实战：书签没存、存早了、存晚了，会怎样？

对应[第 6 章讲义](../../chapters/06-offset-commits.md)，继续使用[单节点 Compose](../../infra/single-node/compose.yaml)与固定 `confluent-kafka==2.13.2`。

## 1. 先看一个最小例子

一个全新 Topic，只有一个分区、三条消息：

```text
Offset 0：订单创建
Offset 1：订单支付
Offset 2：订单打包
日志末尾：3
```

读取 Offset 0 后，客户端的下一读取位置 `position` 为 1。**提交 1 才表示“恢复时从 1 开始”**；提交的不是“最后读到的记录位置 0”，也不是订单 ID。

读取进度、业务处理、Broker 上提交的书签是三件事。这次用四个独立组，把它们拆开验证。只生产三条源消息，各组有自己的进度。

## 2. 四个实验的预测

四组第一轮都先读 Offset 0，随后在精确位置中断自己的 Consumer 子进程。每组再启动一个新进程，使用**相同 group.id 和 subscribe**恢复，不 seek、不重置、不改成 assign。

| 场景 | 第一轮中断前做了什么 | Broker 保存的书签 | 重启第一条 | Offset 0 的模拟处理总次数 |
|---|---|---|---|---:|
| read_only | 只读，不处理，不提交 | 没有提交 | 0 | 1，来自恢复阶段 |
| commit_first | 提交 1，还没处理 0 | 1 | 1 | 0，漏处理 |
| process_first | 处理 0，还没提交 | 没有提交 | 0 | 2，重复处理 |
| process_then_commit | 处理 0，然后提交 1 | 1 | 1 | 1，本次窗口没有重复 |

恢复阶段统一逐条“模拟处理成功 → 同步提交下一位置”，最终每个组提交为 3。

**最后一行不是 exactly-once 的证明。**如果恰好死在“处理完成、提交之前”，就会退化成第三行的重复窗口。处理与提交不在一个原子事务里，顺序正确也不能让两步变成一步。

## 3. 启动方式

需要 Docker 正常运行、Docker Compose v2、Python 3.9+。已经准备第 4、5 章虚拟环境，可以直接复用：

```bash
PYTHON_BIN="$PWD/.venv/bin/python" bash labs/ch06-offset-commits/verify.sh
```

首次安装在仓库根目录：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r labs/ch06-offset-commits/requirements.txt
PYTHON_BIN="$PWD/.venv/bin/python" bash labs/ch06-offset-commits/verify.sh
```

依赖只进虚拟环境，脚本不自动安装、不修改全局配置。兼容 `docker compose` 和独立 `docker-compose` v2。

**本实验明确会中断自己启动的 Consumer 子进程，不中断 Broker，不重建共享 Kafka 容器。**不要与第 2 章容器重建实验同时运行。Topic、组和所有结果保留，不删日志或数据卷。单节点单副本 PLAINTEXT 仅本地教学。

## 4. 什么是真的，什么是模拟的？

真正在 Kafka 上执行的动作：

- 创建唯一 `shopflow.orders.ch06.<runid>`，单分区、单副本。
- 一个 Producer 写入三条独立事件，确认真实 Offset 0/1/2。
- 四个新组 `shopflow.ch06.<runid>.<scenario>`，通过 `subscribe()` 加入普通 Classic 组。
- `consumer.position()` 查看已交付给应用后的下一位置。
- `consumer.commit(message=msg, asynchronous=False)` 写入 Broker 的组进度。
- `consumer.committed()` 从 Broker 查询已提交位置。
- 首轮 Consumer 子进程调用 `os._exit(42)`，跳过 `close()`、异常收尾和 finally；父进程严格核对预期退出码42，再启动新的恢复进程。
- 新进程同组订阅，观察实际第一条记录与提交结果。失联成员需要会话过期后才能完成组恢复，使用有界等待，不靠固定睡眠假定已稳定。

模拟业务动作：把处理记录追加到各组的 `effects.jsonl`，flush/fsync 后作为已执行的可检查证据。**不是扣库存、扣款，也不是数据库事务。**文件同步用于复现消费者进程退出前后的动作，不证明主机断电或磁盘损毁的持久性。

副作用故意不去重，所以第三场景能观察 Offset 0 的两次追加。不安装真实业务数据库，也不把这份日志当成生产幂等实现。

## 5. 关键配置与代码

Consumer 的配置片段（完整程序见 [app.py](app.py)）：

```python
"group.protocol": "classic",
"partition.assignment.strategy": "range",
"enable.auto.commit": False,
"enable.auto.offset.store": False,
"auto.offset.reset": "earliest",
"session.timeout.ms": 6000,
"heartbeat.interval.ms": 1000,
"max.poll.interval.ms": 60000,
"socket.timeout.ms": 10000,
```

关闭自动提交和自动保存，确保读到一条时不会默默保存 Broker 书签。`earliest` 只是没有有效提交时的回退；第二/四场景已有提交1，重启不因 earliest 回到0。

同步提交示意：

```python
parts = consumer.commit(message=msg, asynchronous=False)
```

`message=msg` 自动提交 `msg.offset()+1`。同步调用返回后不只看“没有抛异常”，还核对返回分区、offset和错误字段；真实 `TopicPartition` 的 `error` 表示分区错误，API 文档有时称其为 `.err`，代码兼容检查但不忽略错误。异步提交会立即返回 None，本章不把 None 当同步成功。

本章不调用无参数 `commit()`，以免把已经读取但尚未处理的其他分区进度一起提交；多分区连续完成水位在讲义中介绍，实验保持单分区看清失败窗口。

读取位置不等于网络预取位置：客户端可能已经预取多条，`position()` 描述的是交付给应用的消费位置。读到0后我们只处理/提交这条，不把预取的1、2当成已完成。

## 6. 实际证据与输出文件

结果位于 `labs/ch06-offset-commits/results/<runid>/`，Git 忽略个人运行结果。

根目录：

- `run.env` / `topic.txt`：实际 Topic、组前缀与结构。
- `source.json` / `producer-deliveries.json`：三条源事件与投递坐标。
- `original-records-replay.json`：所有场景完成后仍能直接回读0/1/2，证明消费提交不删源消息。
- `summary.json`：四组书签、重启第一条、模拟处理次数的总对照。

每个场景目录：

- `first.json`：首轮 PID、读到的0、position1、读取后无提交、中断点书签、处理和提交动作。closed=false 是预期硬退出，不是遗漏收尾 bug。
- `resume.json`：新 PID、同组起点、读到的序列、每条成功处理后的提交位置，正常 closed=true。
- `effects.jsonl`：第一轮/恢复阶段追加的模拟业务动作。
- `broker-committed.json`：独立 observer 消费者查询的 before / after_cut / after_resume 位置；observer 只调用 committed，不 subscribe、不 assign，不参与组分配。
- `after_cut.cli.txt` / `after_resume.cli.txt`：官方 CLI 查询组提交位置。
- `first.log` / `resume.log`：子进程日志，失败时先查这里。
- `summary.json`：该组的精简验收结论。

没有提交用 JSON null 表达，对应客户端 `OFFSET_INVALID`，**不是 Offset 0**。两个状态都可能在配置 earliest 时从0读，但含义不同。

## 7. Lag 为零，不代表业务没漏

最后四个组均提交3、日志末尾3；CLI 通常显示 CURRENT-OFFSET=3、LOG-END-OFFSET=3、LAG=0。

但 commit_first 的副作用日志没有 Offset 0。它的源消息仍在 Broker 上，只是组书签跳过它，正常恢复不再读这条。

因此不能说“Lag 清零，所以所有库存操作都完成了”。这是日志进度对照，不是业务数据库审计。生产里还要考虑记录空洞、事务可见性、错误提交等边界，不能把Offset差额简单当未处理订单数。

手动查询示意：从本次 `run.env` 取得真实 `GROUP_PREFIX`，例如选择 commit_first 组，设置 `GROUP_ID` 后：

```bash
docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --describe --group "$GROUP_ID"
```

这里只查询，不执行 reset-offsets，不改变已保存书签。工作进程结束后组没有活跃成员是正常现象，提交位置仍可查询。

## 8. 验收与自测

```bash
.venv/bin/python labs/ch06-offset-commits/app.py self-test
```

本地纯逻辑测试覆盖四场景期望、恢复起点错误拒绝、提交下一位置、分区提交错误/无同步结果拒绝、无提交与0不可混淆。它们不连接 Broker，不冒充真实网络故障实验。

联网验收核对：

1. 每个组起始没有提交，首轮读0后 position1，但 Broker 仍无提交。
2. 首轮中断代码42，报告与独立查询的书签符合场景。
3. 新 PID 同 group.id 恢复，实际首条为0或1，不手工修改起点。
4. 正常恢复逐条处理并提交，最终 Broker 书签3。
5. 模拟处理次数按事件与Offset核对，分别复现未处理、重复与本次完整处理，不只看总行数。
6. 所有原始三条记录仍可审计回读。

本章不会宣称生产端到端 exactly-once，也不做 Broker 宕机、断电、消费者多线程或分区移交竞争实验。恢复等待与子进程有截止时间；异常返回非0并保留现场，不无限等待或删除数据“重试”。

## 9. 停止与下一步

首轮子进程已经明确硬退出，恢复子进程正常 close；没有留后台消费者。Kafka 服务保留运行，完成后可以停止：

```bash
docker compose -f infra/single-node/compose.yaml stop kafka
```

不加删除卷参数，不自动删任何 Topic 或组进度。下一章 Rebalance，研究成员离开、超时和分区移交时如何处理生命周期。

## 10. 作者实跑验收

2026-10-13，在 macOS + Colima / Docker Engine 27.0.3、独立 Compose 2.29.1、Python 3.13.12、confluent-kafka/librdkafka 2.13.2、Kafka Broker 4.3.1 环境实跑通过。

| 场景 | 首轮读取后 position | 中断后 Broker committed | 同组重启首条 Offset | 事件0/1/2的模拟处理次数 | 最终 committed / Lag |
|---|---:|---|---:|---|---|
| read_only | 1 | 无提交 | 0 | 1 / 1 / 1 | 3 / 0 |
| commit_first | 1 | 1 | 1 | 0 / 1 / 1 | 3 / 0 |
| process_first | 1 | 无提交 | 0 | 2 / 1 / 1 | 3 / 0 |
| process_then_commit | 1 | 1 | 1 | 1 / 1 / 1 | 3 / 0 |

- 四组首轮均为不同的真实 Consumer 子进程，按指定位置退出码42；恢复使用同组另一个进程的真实 subscribe，没有 seek/reset。
- 读0后四组 Broker 尚无提交；显式提交才出现1，恢复逐条处理/提交后四组皆为3。
- `commit_first` 的 CLI 真实显示 CURRENT-OFFSET=3、LOG-END-OFFSET=3、LAG=0，但 effects 缺失0。
- `process_first` 的 effects 含首轮和恢复的两次Offset0，符合重复窗口预测。
- 最终全部三个原始事件仍在日志内，可从0直接回读；没有消费触发删除。
- 9项本地自测、全部12张讲义Mermaid、Bash/Python语法、Compose及本地文档引用通过。

此证据仅覆盖消费进程指定退出与正常Broker上的恢复，不证明断电、Broker故障、真实数据库事务或生产exactly-once；模拟动作只有结果目录中的JSONL追加。先提交的遗漏也不是日志永久丢失，保留范围内可以另行评估受控补偿/重放。

## 11. 官方参考

- [Kafka Consumer 配置](https://kafka.apache.org/43/configuration/consumer-configs/)
- [Python API 说明](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- [固定 librdkafka 2.13.2 配置](https://github.com/confluentinc/librdkafka/blob/v2.13.2/CONFIGURATION.md)
