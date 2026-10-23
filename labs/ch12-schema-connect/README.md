# 第 12 章实验：Schema 演进与 Kafka Connect

这个实验分两部分：

1. **契约**：发 v1/v2/v3 三种订单事件，用旧、新两个消费者读，得到兼容矩阵；再跑一个小型兼容性检查器。
2. **接入**：启动真实的 `connect-standalone`，用文件 Source 把 3 行文本写进 Kafka，再用文件 Sink 写回文件。

## 运行

复用第 4 章已安装依赖的虚拟环境，在仓库根目录：

```bash
PYTHON_BIN="$PWD/.venv/bin/python" bash labs/ch12-schema-connect/verify.sh
```

脚本会启动单节点 Kafka、创建带随机后缀的 Topic，并用 `docker run` 临时启动一个 Connect 容器。Connect 启动较慢，通常需要 **60～90 秒**，请耐心等待。

## 第一部分：兼容矩阵

| 事件 | 内容 | 旧消费者 | 新消费者 |
| --- | --- | --- | --- |
| v1 | `order_id` + `amount` | 正常 | 失败：缺 `currency` |
| v2 | 增加可选 `currency` | 正常 | 正常 |
| v3 | `amount` 改名 `total` | 失败：缺 `amount` | 正常 |

结论：

- 加可选字段通常是**向后兼容**（新消费者读旧数据），但不一定向前兼容。
- 改名等价于删除旧字段，旧消费者会失败。
- 代码里的检查器对 v2→v3 报出：删除 `amount`、`currency` 变必填、新增 `total` 无默认值。

这不是完整 Schema Registry，只是一个演示规则的教学检查器。

## 第二部分：Kafka Connect 文件管道

脚本在 `results/<run-id>/connect/` 生成：

- `input.txt`：3 行订单文本；
- `worker.properties`：连 `kafka:9092`，字符串 Converter，offset 存本地文件；
- `source.properties`：`FileStreamSource`，读 `/data/input.txt`，写到指定 Topic；
- `sink.properties`：`FileStreamSink`，读该 Topic，写到 `/data/output.txt`。

然后启动：

```bash
connect-standalone.sh worker.properties source.properties sink.properties
```

实际结果：

| 观察项 | 结果 |
| --- | --- |
| 输入 | 3 行 |
| Topic | 3 条 |
| 输出 | 3 行 |
| 启动到完成 | 约 73 秒 |

Topic 内容与输出文件都应等于输入的三行。

## 手动查看

```bash
cat labs/ch12-schema-connect/results/<run-id>/connect/output.txt

docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic shopflow.connect.ch12.<run-id> --from-beginning --timeout-ms 5000
```

Connect 容器实验结束后会被删除，不会常驻。

## 排错

- Connect 启动慢：文件连接器要扫描插件，60～90 秒属正常。
- Topic 不存在导致 Source 失败：本实验的 Broker 关闭了自动建 Topic，脚本会先创建。
- 容器连不上 Kafka：确认它加入 `kafkalearn_default` 网络，并用 `kafka:9092`。
- 端口 8083 冲突：standalone 默认开 REST 端口；本实验不映射到宿主机。
- 输出文件为空：看 `docker logs <container>`；脚本失败时会自动打印末尾日志。
- 权限问题：挂载目录必须可写，offset 文件和输出文件都写在里面。

## 边界

- 没有部署 Schema Registry，兼容性检查是教学用的简化规则。
- 只用了文件 Source/Sink，不代表生产 JDBC、S3 等连接器的行为。
- 单 Worker、单 Task，没有演示分布式 Connect 的再均衡和故障转移。
- Connect 的投递语义取决于连接器与目标，不自动等于 exactly-once。
- 兼容矩阵只覆盖三种示例事件，不是完整 Schema 演进测试。

## 官方参考

- [Kafka Connect 文档](https://kafka.apache.org/documentation/#connect)
- [Schema Registry 概念](https://docs.confluent.io/platform/current/schema-registry/index.html)
