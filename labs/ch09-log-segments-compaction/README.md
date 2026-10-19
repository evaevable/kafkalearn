# 第 9 章实验：日志段、保留与 compact

这个实验只用一个单节点 Broker，做两组对照：

1. **delete**：把 90 条消息写成 3 个日志段，等保留策略把最老的整段删掉。
2. **compact**：同样的 180 条记录写进普通 Topic 和 compact Topic，比较读回条数。

## 运行

复用第 4 章已安装依赖的虚拟环境，在仓库根目录：

```bash
PYTHON_BIN="$PWD/.venv/bin/python" bash labs/ch09-log-segments-compaction/verify.sh
```

脚本会自动启动 `infra/single-node/compose.yaml`，创建带随机后缀的四个 Topic（delete、compact、control 以及 control 的对照），不会删除已有 Topic 或数据卷。

> 本章把 `log.retention.check.interval.ms` 和 `log.cleaner.backoff.ms` 调成了 2 秒，方便几十秒内看到效果。生产默认慢得多，不要把这个值当推荐配置。

## 实验一：delete 按段删除

Topic 配置：1 分区、1 副本、`retention.ms=12000`、`segment.ms=1000`、`segment.bytes=1048576`。

分三批各写 30 条，实际观察：

| 观察项 | 结果 |
| --- | --- |
| 日志段 | 3 个 → 2 个 |
| 最早可读 offset | 0 → 30 |
| 末尾 offset | 90 |
| 后台清理等待 | 约 8.2 秒 |

删除前的文件：

```text
00000000000000000000.log
00000000000000000030.log
00000000000000000060.log
```

删除后只剩后两个。再从 offset 0 读取，会得到“offset 超出范围”；从 30 开始仍能读到 30～89。

## 实验二：compact 按 key 留最新

写 180 条记录：`key-A`、`key-B`、`key-C` 各 20 个版本（seq 0～19）。普通 delete Topic 作为对照。

| Topic | 策略 | 读回条数 |
| --- | --- | --- |
| `.control` | delete | 180 |
| `.compact` | compact | 63 |

三个 key 的最新值都是 seq=19。

63 不是 3，因为：

- compact 只清理**封存的段**，每个段里每个 key 保留最新值；
- active 段不参与清理，仍保留最新 20 个版本；
- 因此最终是“老版本被清掉，活跃段还在”。

这正说明 compact 不是“去重成一条”，而是异步收敛。

## 结果文件

每次运行写到 `results/<run-id>/`：

- `summary.json`：两次实验的水位、段文件、条数、耗时和采样。
- `delete-segments-before.txt` / `delete-segments-after.txt`：删除前后的段文件名。

## 手动查看

把 `<run-id>` 换成结果中的实际值：

```bash
docker compose -f infra/single-node/compose.yaml exec -T kafka \
  sh -c 'ls -l /var/lib/kafka/data/shopflow.orders.ch09.<run-id>.delete-0'

docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --describe --topic shopflow.orders.ch09.<run-id>.delete

docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 \
  --topic shopflow.orders.ch09.<run-id>.delete --time -2
```

`--time -2` 查最早可读位置，`--time -1` 查末尾位置。

## 排错

- `segment.bytes` 最小是 **1048576**（1 MiB），设小了 Topic 创建会报 `INVALID_CONFIG`。
- 保留删除只看**封存段**；如果只有一个 active 段，等再久也不会删。
- 后台检查间隔是 Broker 配置，不是 Topic 配置；本章已在 Compose 里调快。
- compact 需要封存段；数据太少或写入太快时，可能暂时看不到明显变化。
- 从被删位置读取会报 offset 超出范围，这是预期，不是数据“又丢了”。

## 边界

- 单节点、单副本，只验证清理行为，不验证高可用或断电持久性。
- 清理是后台异步过程，具体时刻受检查间隔、段滚动和文件删除延迟影响。
- compact 的最终形态还受 active 段和段边界影响，不承诺“只剩一条”。
- 实验会真实删除自己创建的 Topic 的历史段，不影响其他 Topic；不要在生产环境照搬这些教学参数。

## 官方参考

- [Topic 配置](https://kafka.apache.org/43/configuration/topic-configs/)
- [日志清理与 compact](https://kafka.apache.org/documentation/#compaction)
