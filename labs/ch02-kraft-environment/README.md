# 第 2 章实战：KRaft 身份、两套地址与持久化

对应 [第 2 章讲义](../../chapters/02-kraft-environment.md)，复用 [单节点配置](../../infra/single-node/compose.yaml)。

## 1. 本章实验范围

- 观察 KRaft 元数据仲裁组：集群身份、Leader、投票成员与复制位置。
- 验证内部 `kafka:9092` 与宿主 `127.0.0.1:19092` 不仅 TCP 连通，还能完成 Kafka 协议请求。
- 确认宿主获得的元数据声明地址是宿主可达地址，并再次通过声明地址发送请求。
- 创建唯一 Topic、发送一条唯一订单事件；重建容器后核对容器 ID 改变，但命名数据卷、集群身份、Topic ID、存储身份与事件保持不变。

**操作影响：脚本会重建第 1、2 章共享的本地 Kafka 容器，短暂中断其连接。请停止其他实验后运行；不能用于生产。**脚本不删 Topic、不删卷、不修改全局 Docker 设置。结果目录与 Topic 保留供观察，不无限循环运行。

## 2. 前置条件与启动

- Docker 正常运行、Docker Compose v2（支持 `up --wait`）。
- Python 3.9+，只用标准库，无额外依赖；Kafka CLI 位于容器内，无需宿主 Java。
- 本章仍固定 `apache/kafka:4.3.1`，不是最新版承诺。

在仓库根目录：

```bash
docker compose -f infra/single-node/compose.yaml up -d --wait
bash labs/ch02-kraft-environment/verify.sh
```

如果本机只安装独立 Compose v2，将第一条替换为 `docker-compose -f infra/single-node/compose.yaml up -d --wait`。脚本自动判断两个入口，不需要改 Docker 全局配置。

Python 不在 PATH 时，可显式指定执行文件：

```bash
PYTHON_BIN="/absolute/path/to/python3" bash labs/ch02-kraft-environment/verify.sh
```

`/absolute/path/to/python3` 是说明用路径，执行前替换成你本机真实可执行文件。

## 3. 先观察控制层

```bash
docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka:9092 describe --status

docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka:9092 describe --replication
```

第一条通过 Broker 入口查询元数据 quorum（仲裁组）的状态；第二条列出控制节点复制位置。不是读取订单 Topic 的业务副本状态。

关键输出：

```text
ClusterId:        MkU3OEVBNTcwNTJENDM2Qk
LeaderId:         1
LeaderEpoch:      <随重启变化>
HighWatermark:    <运行中变化>
CurrentVoters:    [{"id": 1, "endpoints": ["CONTROLLER://kafka:9093"]}]
```

- `ClusterId`：来自实验配置，存储初始化后应稳定。
- `LeaderId=1`：当前控制层 Leader 是节点 1，不表示“生产集群永远只能有一个分区 Leader”。
- `CurrentVoters`：当前只有一个投票节点；它停机没有其他控制器可接替，不是高可用。
- `HighWatermark`：元数据日志的提交边界，不是订单消费组的 Offset。
- 单节点复制视图只有一行 Leader，Lag 为 0 是因为没有其他投票节点，不证明高可用或性能优异。

## 4. 亲眼看到两套声明地址

内部 CLI：

```bash
docker compose -f infra/single-node/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092
```

输出起始应为 `kafka:9092 (id: 1 ...)`，后面列出支持的 Kafka API。这个工具先获得 Broker 信息，再查询 API，证据强于“端口能连”。

宿主机：

```bash
python3 labs/ch02-kraft-environment/probe_metadata.py
```

输出 JSON 的 `bootstrap.brokers[0]` 和 `advertised_followup.brokers[0]` 应包含：

```json
{"node_id":1,"host":"127.0.0.1","port":19092,"rack":null}
```

探针向宿主端口发出只读 Metadata v8 请求，检查响应，再按响应里的声明地址重连并再次请求。它只请求 Broker 信息，Topic 数组为空，禁用自动创建标志，不创建业务对象。对旧组位置也没有影响。

这不是 HTTP：不要用浏览器或 `curl http://127.0.0.1:19092` 来验 Kafka。TCP 连通也不足以证明返回的声明地址可用。

### 探针的边界

`probe_metadata.py` 是为了无需安装客户端库就展示网络原理而写的教学探针，**不是通用 Kafka 客户端**：固定 Metadata v8，不做 API 版本协商，不支持 TLS/SASL、生产或消费，不替代成熟应用客户端。解析器检查帧大小、数组边界、correlation ID（请求响应配对编号）、截断和异常数据，具备 6 个自测：

```bash
python3 labs/ch02-kraft-environment/probe_metadata.py --self-test
```

协议字段按固定版本源码核对：[MetadataRequest](https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/MetadataRequest.json)、[MetadataResponse](https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/MetadataResponse.json)。不需要读协议代码才能学完本章。

## 5. 看一眼数据在哪里

```bash
docker compose -f infra/single-node/compose.yaml exec -T kafka \
  cat /var/lib/kafka/data/meta.properties

docker compose -f infra/single-node/compose.yaml exec -T kafka \
  ls -1 /var/lib/kafka/data
```

`meta.properties` 中能看到 `cluster.id`、`node.id`，可能还有存储格式相关字段。目录里可看到本实验订单分区目录，以及 `__cluster_metadata-0` 元数据日志目录。二者职责不同，但本实验放在同一持久化位置。

Compose 指定 `log.dirs=/var/lib/kafka/data`，未另设 `metadata.log.dir`，元数据日志使用第一个日志目录。命名卷挂到这一目录，因此不是只保住业务消息、丢掉集群身份。

不要用编辑 `meta.properties`、删文件或重新 format 的方式“修复”身份不匹配。已有卷对应已初始化的集群；随意改 `CLUSTER_ID` 或 `node.id` 可能让启动失败或产生不一致。

## 6. 自动重建验收到底做了什么

`verify.sh` 依次执行：

1. 启动单节点并等待健康，运行探针自测。
2. 创建唯一 `shopflow.orders.ch02.<runid>`，1 分区、1 副本；发布一条唯一订单事件。
3. 保存 `before` 状态：容器检查、数据卷、控制层状态、Topic ID、`meta.properties`、数据目录、内外连接结果、事件回读。
4. 执行以下同等操作：

```bash
docker compose -f infra/single-node/compose.yaml up -d \
  --force-recreate --no-deps --wait --wait-timeout 180 kafka
```

这是**平稳停止旧容器并创建新容器**，不是强制断电，更不是删数据。`--no-deps` 不启动其他服务；命名卷仍挂回新容器。

5. 保存 `after` 状态，从分区 0 / Offset 0 再读唯一事件。
6. 程序断言容器 ID 不同，卷名、身份、Topic ID、消息完全一致，且重建前后均可通过两个监听路径查询协议。

预期关键输出：

```text
PASS: Metadata, internal/external endpoints, and initial event verified.
PASS: Container ID changed; named data volume and meta.properties unchanged.
PASS: Cluster identity, topic identity, metadata directory, and order event persisted.
PASS: Single-voter quorum and both listener paths verified before/after recreation.
PASS: chapter 02 acceptance checks completed.
```

结果保存在 `labs/ch02-kraft-environment/results/<runid>/`，已被 Git 忽略，不推送个人运行记录。包含 source/before/after 事件、容器与地址 JSON、quorum 视图、Topic 描述、身份文件和目录列表；打开文件逐项对照，而不是只看 `PASS`。

实验只证明**有命名卷时，平稳重建容器后数据与身份保持**。它不证明副本故障接管、突然断电、主机磁盘损坏、异地备份或 exactly-once。

## 7. 排错顺序

| 现象 | 要检查的证据 | 不要误判 |
|---|---|---|
| daemon 无法连接 | `docker version` 的 Server、Docker context、运行时是否启动 | Kafka 服务配置还未执行 |
| 启动不健康 | `docker compose -f infra/single-node/compose.yaml logs --tail 100 kafka` | 不先删卷重试 |
| 内部可用、外部不可用 | ports 的绑定；宿主探针报错；声明 host/port | 内部 healthcheck 不证明外部可达 |
| bootstrap 能连、后续连接失败 | 元数据里的 Broker 地址从客户端网络能否访问 | bootstrap 不是持续代理 |
| 身份不匹配 | Compose 与原卷 `meta.properties` 是否一致 | 不把旧卷当成新集群随便 format |
| 多个实验并发失败 | 是否另一个实验在重建同一服务 | 停掉并发实验再运行 |
| 控制层有 Leader、业务查询失败 | Broker 服务与业务分区状态 | quorum 健康不等于业务一定可用 |

想改宿主端口为 `29092` 时，不要直接改当前配置然后运行本验收。先理解至少要同步调整端口映射的宿主端、`EXTERNAL` 的声明端口；监听端可以仍是容器 `19092`。本脚本固定验收原始 `19092`；探针单独支持 `--port` / `--expected-port`，不是全局配置自动修复器。

## 8. 作者验收记录

2026-10-10，在 macOS + Colima Docker Engine 27.0.3 / 独立 Compose 2.29.1 / Python 3.13.12 环境实跑通过：

- 6 个探针解析与地址检查自测通过。
- 控制层 ClusterId 为固定实验身份，LeaderId 为 1，只有投票节点 1。
- 重建前后内部 CLI 返回 `kafka:9092`；宿主 Metadata v8 返回 `127.0.0.1:19092`，按声明地址再次查询成功。
- 容器 ID 确实改变，挂载卷仍为 `kafkalearn_kafka-data`；`meta.properties` 与 Topic ID 保持。
- `__cluster_metadata-0` 与唯一订单分区目录均存在，原始事件重建前后逐字节一致。
- Bash、Python、Compose、全仓库 Mermaid 与文档引用校验通过。

未做故障域、断电、磁盘损毁或高可用测试，不能扩大解释这份验收。

## 9. 停止与数据保留

```bash
docker compose -f infra/single-node/compose.yaml stop kafka
```

停止服务，容器和命名卷都保留。需要移除实验容器和网络时可使用不带 `-v` 的 `down`。**不要默认执行 `down -v` 或 `docker volume prune`**，它们可能删除你想保留的数据；本章没有任何清理步骤。

下一章在这个可解释环境上学习 Topic、Partition 与 Offset。
