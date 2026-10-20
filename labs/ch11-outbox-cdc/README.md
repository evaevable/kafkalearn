# 第 11 章实验：双写缺口、Outbox 与幂等消费

这个实验用 SQLite 当业务库、真实 Kafka 收事件，分三步：

1. 制造两种双写不一致；
2. 用 Outbox 在同一个数据库事务里写订单和事件；
3. 让中继在发送后崩溃，重试造成重复，再用幂等消费只生效一次。

## 运行

复用第 4 章已安装依赖的虚拟环境，在仓库根目录：

```bash
PYTHON_BIN="$PWD/.venv/bin/python" bash labs/ch11-outbox-cdc/verify.sh
```

脚本自动启动单节点 Kafka，创建带随机后缀的 Topic，并在 `results/<run-id>/shopflow.db` 建库。不会改动已有数据。

## 第一步：双写缺口

| 做法 | 实际结果 |
| --- | --- |
| 写库成功，未发事件 | `order-db-only` 在库中，Topic 里 0 条 |
| 先发事件，写库回滚 | Topic 里有 `order-ghost` 1 条，库里 0 行 |

两种失败窗口都真实存在，重试也无法同时修复两边。

## 第二步：Outbox 原子写入

`create_order_with_outbox()` 在同一个 SQLite 事务里：

```sql
INSERT INTO orders(order_id, status) VALUES ('order-outbox-1', 'created');
INSERT INTO outbox(event_id, aggregate_id, event_type, payload, published)
VALUES ('evt-outbox-1', 'order-outbox-1', 'order.created', '...', 0);
COMMIT;
```

结果：订单 1 行、Outbox 1 行。

## 第三步：中继崩溃与幂等消费

中继流程：读 `published=0` → 发 Kafka → 标记 `published=1`。

实验故意在“发完、还没标记”时抛异常：

| 观察项 | 结果 |
| --- | --- |
| 第一次中继 | 发出 1 条，`published` 仍为 0 |
| 重试 | 又发 1 条，Topic 共 **2 条** |
| 朴素消费 | 扣库存 **2 次** |
| 幂等消费 | 生效 **1 次**，跳过 1 次 |
| 最终库存 | **1** |

幂等消费的关键：`processed_events.event_id` 是主键，插入成功才执行扣减，两者在同一个事务里。

## 数据库里有什么

用 `sqlite3` 打开 `results/<run-id>/shopflow.db`：

```sql
SELECT * FROM orders;
SELECT event_id, published FROM outbox;
SELECT * FROM processed_events;
SELECT * FROM inventory;
```

预期：

- `orders` 里有 `order-db-only` 和 `order-outbox-1`，没有 `order-ghost`；
- `outbox` 里 `evt-outbox-1` 的 `published=1`；
- `processed_events` 只有一行 `evt-outbox-1`；
- `inventory` 里 `order-outbox-1` 的 `deducted=1`。

## 手动看 Topic

```bash
docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic shopflow.orders.ch11.<run-id>.outbox --from-beginning --timeout-ms 5000
```

会看到两条内容相同的 `evt-outbox-1`，这就是中继重试造成的重复。

## 排错

- 中继第一次没有抛异常：检查 `crash_before_mark` 分支是否被执行。
- 幂等消费没跳过：确认 `event_id` 是主键，且插入和业务动作在同一个事务。
- 库存扣了 2 次：去重插入失败后要回滚，不能继续执行更新。
- Topic 读不到消息：先确认 Kafka 容器健康、Topic 名来自本次 `run-id`。
- `database is locked`：本实验是单进程单连接；多进程写入需要更细的并发控制。

## 边界

- 崩溃是异常模拟，不是真的杀进程或断电。
- SQLite 只用于本地演示；生产要换成真实数据库和对应的 Outbox 实现。
- 中继是单实例轮询，没有演示多实例抢锁、批量发送和顺序保证。
- 没有部署 Debezium；CDC 只讲原理和与 Outbox 的关系。
- Outbox + 幂等消费保证“不丢 + 重复不产生额外效果”，不是“只发一次”。

## 官方参考

- [Transactional Outbox 模式](https://microservices.io/patterns/data/transactional-outbox.html)
- [Debezium 文档](https://debezium.io/documentation/)
