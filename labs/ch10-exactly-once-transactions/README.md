# 第 10 章实验：投递语义、幂等与事务

这个实验在一个单节点 Broker 上做三组观察：

1. **事务可见性**：提交 3 条、回滚 2 条，对比 `read_committed` 与 `read_uncommitted`。
2. **幂等生产者**：同一个 key/value 由应用发两次，看会不会被自动去重。
3. **EOS 闭环**：读 source 3 条，在同一个事务里写 sink 3 条并提交 offset。

## 运行

复用第 4 章已安装依赖的虚拟环境，在仓库根目录：

```bash
PYTHON_BIN="$PWD/.venv/bin/python" bash labs/ch10-exactly-once-transactions/verify.sh
```

脚本自动启动 `infra/single-node/compose.yaml`，创建带随机后缀的 Topic，不删除已有数据。

## 实验一：提交与回滚

事务生产者先写 3 条并 `commit`，再写 2 条并 `abort`。

| 读取方式 | 实际读到 |
| --- | --- |
| `read_committed` | **3 条**（offset 0、1、2） |
| `read_uncommitted` | **5 条**（回滚的两条在 offset 4、5） |

回滚记录不是 offset 3、4，因为 offset 3 被**提交标记**占用了。控制标记写在日志里，但普通消费者读不到。

## 实验二：幂等不等于业务去重

生产者开启 `enable.idempotence=true`，应用连续发送两条完全相同的 key/value。

结果：**读到 2 条**。

幂等生产者只防止“同一次发送因重试写两遍”，不会把两次独立的 `produce()` 合并。

## 实验三：读-处理-写闭环

在一个事务里：

- 从 `source` 读 3 条；
- 往 `sink` 写 3 条；
- 通过 `send_offsets_to_transaction` 提交 `source` 的 offset=3；
- `commit_transaction`。

实际结果：

| 观察项 | 结果 |
| --- | --- |
| sink 记录 | 3 |
| 已提交 offset | 3 |
| 同组新消费者再读 | 0 条 |

这说明结果和消费进度一起生效，重启不会重做已经提交的那一批。

## 结果文件

`results/<run-id>/summary.json` 保存三组实验的条数、offset、读取隔离级别和客户端版本。

## 手动观察

把 `<run-id>` 换成 summary 里的实际值：

```bash
docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic shopflow.orders.ch10.<run-id>.tx --from-beginning \
  --isolation-level read_committed --timeout-ms 5000

docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic shopflow.orders.ch10.<run-id>.tx --from-beginning \
  --isolation-level read_uncommitted --timeout-ms 5000

docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --describe --group shopflow.ch10.eos.<run-id>
```

第一条应只打印 3 条，第二条打印 5 条。

## 排错

- 读取结果仍是 5 条：检查消费者是否设置了 `isolation.level=read_committed`。
- 事务初始化报错：`transactional.id` 不能与其他活跃实例冲突；本实验每次使用随机 ID。
- `send_offsets_to_transaction` 失败：确认消费者属于该 group，且 offset 是该分区下一个位置。
- 回滚数据“看不见”但 offset 有空洞：这是控制标记和事务语义的正常现象。
- 事务超时：检查 `transaction.timeout.ms`，不要让事务跨很长时间。

## 边界

- 单节点、单副本，不验证 Broker 故障时的事务恢复。
- 只验证 Kafka 内部读写；没有数据库、HTTP 等外部副作用。
- `exactly-once` 指 Kafka 读-处理-写链路，不等于业务全局 exactly-once。
- 实验没有注入进程崩溃，只观察 commit/abort 的可见性差异。

## 官方参考

- [Kafka 事务设计](https://kafka.apache.org/documentation/#transactions)
- [confluent-kafka Python 客户端](https://docs.confluent.io/kafka-clients/python/current/overview.html)
