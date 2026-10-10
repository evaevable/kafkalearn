# Kafka 逐章学习与实战

从“为什么需要 Kafka”开始，逐步掌握可重放事件日志、可靠消息处理、实时数据管道与生产运维。每次只学习一章：**讲原理 → 做实验 → 核对现象 → 自测 → 再进入下一章**。

## 课程约定

- 不是一次堆出整本书。第 1～5 章已交付；回到对话说“继续”，再讲、写、验证和推送下一章。
- 每章包含：学习目标、分块讲解、图谱、可运行实战、预期现象、常见错误、自测与答案。
- 贯穿案例 **ShopFlow**：订单服务发布事件，库存与分析服务独立消费；后续加入可靠落库、去重、CDC 和实时分析。
- 先用 CLI 看清 Kafka，再引入应用代码。应用客户端优先用 Python（方便分析实验），协议与原理不绑定语言；Kafka Streams 的特有能力会单独说明。
- 使用现代 **KRaft** 路线，不从 ZooKeeper 部署起步。Kafka 4.x 已移除 ZooKeeper 支持；历史版本仅作为迁移背景介绍。
- 当前可复现实验固定使用官方镜像 **`apache/kafka:4.3.1`**，不使用浮动 `latest` 或 RC。固定版本是学习基线，不代表承诺它永远是最新版或生产推荐版本。

## 学习路线

| 阶段 | 章 | 学习内容 | 本章实战与验收 | 状态 |
|---|---|---|---|---|
| 建立直觉 | 01 | [Kafka 到底解决什么问题](chapters/01-why-kafka.md) | 发送 3 条订单事件；库存与分析组分别读全量；从 offset 0 重放 | 已交付 |
| 建立直觉 | 02 | [KRaft、Broker、Controller 与本地环境](chapters/02-kraft-environment.md) | 查询控制层、验证内外声明地址；重建容器后身份与事件仍在 | 已交付 |
| 建立直觉 | 03 | [Topic、Partition、Offset 与顺序](chapters/03-topic-partition-offset.md) | 三分区发送6条keyed事件；核对位置与精确读取，展示跨分区遍历边界 | 已交付 |
| 收发消息 | 04 | [Producer、消息 key、分区与批处理](chapters/04-producer-key-batching.md) | Python 实际投递回读；key路由对齐；逐条flush与集中入队的真实批次/请求对照 | 已交付 |
| 收发消息 | 05 | [Consumer Group 与并行消费](chapters/05-consumer-groups.md) | 6进程真实订阅；库存4成员/分析2成员，各组完整读取12条，验证分区归属与空闲成员 | 已交付 |
| 收发消息 | 06 | Offset 提交、重启与重复消费 | 对比先提交/后处理与先处理/后提交，复现丢处理和重复 | 待讲 |
| 收发消息 | 07 | Rebalance、超时与消费者生命周期 | 停止、扩容消费者，观察分区重新分配和处理暂停 | 待讲 |
| 可靠性 | 08 | 副本、Leader、ISR、acks 与故障 | 三节点实验：停掉 Leader，核对可用性和写入边界 | 待讲 |
| 可靠性 | 09 | 日志段、保留、压缩与磁盘 | 分别观察 delete 与 compact；辨析 compact 不是即时去重 | 待讲 |
| 可靠性 | 10 | At-most/at-least/exactly-once 与事务 | 幂等生产、事务可见性；说明外部数据库不自动参与 Kafka 事务 | 待讲 |
| 数据管道 | 11 | 业务幂等、Outbox 与 CDC | 复现数据库/Kafka 双写缺口，再实现事务 Outbox 和去重消费 | 待讲 |
| 数据管道 | 12 | 消息契约、Schema 演进与 Kafka Connect | 增加字段、处理旧消息、构建数据接入链路 | 待讲 |
| 数据管道 | 13 | 实时分析、窗口、迟到与状态 | 汇总订单计数、乱序实验；辨析事件时间和处理时间 | 待讲 |
| 生产实践 | 14 | Lag、监控、背压与性能调优 | 制造消费积压，用指标定位慢消费者；跑基准而不是背“最佳参数” | 待讲 |
| 生产实践 | 15 | TLS、认证、ACL 与上线检查 | 验证允许/拒绝的生产消费操作，形成上线清单 | 待讲 |
| 综合验收 | 16 | ShopFlow 端到端总实战与故障演练 | 事件发布、库存处理、分析聚合、去重、重放、故障恢复全链路验收 | 待讲 |

后续章节的代码随课程进度交付，上表是学习计划，不是“已完成”功能清单。

## 第 1 章快速实战

前置：Docker Engine 正常运行，且支持 `docker compose` v2（建议 v2.20+）。macOS 可以使用 Docker Desktop 或 Colima。无需先安装宿主机 Java，Kafka 和 CLI 均运行在容器内。

在仓库根目录执行：

```bash
docker compose -f infra/single-node/compose.yaml up -d --wait
bash labs/ch01-hello-kafka/verify.sh
```

如果本机仅有独立的 Compose v2 命令，将第一条中的 `docker compose` 替换成 `docker-compose`；验收脚本自动兼容两者。

成功应显示 `PASS`：生产 3 条记录，`inventory` 和 `analytics` 两个独立消费组各读取相同的 3 条记录，并从分区 0 / offset 0 再读出全部事件。

完整步骤、输出示例与停止方法见 [第 1 章实验说明](labs/ch01-hello-kafka/README.md)。每次脚本使用独立 Topic 和组 ID，不删除原有数据，也不依赖同一组的自动提交时机。

## 第 2 章快速实战

第 1 章环境基础上增加 Python 3.9+（只用标准库）：

```bash
bash labs/ch02-kraft-environment/verify.sh
```

脚本自动启动环境，观察 KRaft quorum、内部地址与宿主声明地址，创建唯一事件后平稳重建 Kafka 容器，再核对身份与消息保持。**这会短暂中断共享的本地 Kafka 服务，不要与其他实验同时运行。**不删除 Topic 或数据卷。

完整操作、Python 路径选择、验证边界见 [第 2 章实验手册](labs/ch02-kraft-environment/README.md)。容器重建成功不代表突然断电或磁盘损毁不丢数据。

## 第 3 章快速实战

```bash
bash labs/ch03-topic-partition-offset/verify.sh
```

脚本创建唯一的三分区、单副本 Topic，以三个订单 key 发布六条事件。逐分区核对 key、Offset、事件，检查每分区日志末尾为 2，并精确读取分区 1 / Offset 1。

复用单节点和 Python 3.9+ 标准库；`lab.py` 只生成输入和验收，收发由官方 CLI 完成。两种跨分区遍历是程序构造的教学展示，不是多消费者随机交错实测。操作与边界见 [第 3 章实验手册](labs/ch03-topic-partition-offset/README.md)。不强制重建容器、不删数据；不要与第 2 章重建实验并发。

## 第 4 章快速实战

从辅助脚本升级到真正的 Python Kafka 客户端，先在虚拟环境安装固定依赖：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r labs/ch04-producer-key-batching/requirements.txt
PYTHON_BIN="$PWD/.venv/bin/python" bash labs/ch04-producer-key-batching/verify.sh
```

固定 `confluent-kafka==2.13.2`，显式 Murmur2 分区策略对齐第 3 章 Java CLI。六条 key 样例实际投递并回读；两种方式各发送120条，检查投递回调和批次/Produce请求统计。单机教学对照非生产基准，不测试业务去重或高可用。细节见 [第 4 章实验手册](labs/ch04-producer-key-batching/README.md)。

## 第 5 章快速实战

复用第 4 章固定客户端虚拟环境（首次运行先安装本章 requirements.txt）：

```bash
PYTHON_BIN="$PWD/.venv/bin/python" bash labs/ch05-consumer-groups/verify.sh
```

启动库存组4个、分析组2个独立 Consumer 进程，用 subscribe 真正参加分配。稳定后发布12条订单事件，每组各读12条：库存4/4/4/0，分析8/4。保存成员仍在线时的 Admin/CLI 分区证据；关闭自动提交、不显式commit，稳定无重复不代表故障恢复 exactly-once。详见 [第 5 章实验手册](labs/ch05-consumer-groups/README.md)。

## 仓库结构

```text
chapters/
  01-why-kafka.md                  # 事件日志直觉、独立消费与重放
  02-kraft-environment.md          # 数据面、控制面、网络与持久化
  03-topic-partition-offset.md     # 日志分片、坐标、key 与顺序
  04-producer-key-batching.md      # 异步投递、序列化、批次与背压
  05-consumer-groups.md            # 同组分工、组间独立、分区分配
infra/single-node/
  compose.yaml                    # 第 1～5 章单节点 KRaft 学习环境
labs/ch01-hello-kafka/
  README.md                       # 实验步骤和排错
  verify.sh                       # 可重复运行的自动验收
  events.jsonl                    # 3 条订单事件
labs/ch02-kraft-environment/
  README.md                       # 控制层、双地址、重建持久化实验
  verify.sh                       # 自动环境验收
  probe_metadata.py               # 标准库只读元数据探针与自测
labs/ch03-topic-partition-offset/
  README.md                       # 三分区事件与 Offset 实战
  verify.sh                       # 官方 CLI 收发与自动验收
  lab.py                          # 输入生成、结果断言与自测
labs/ch04-producer-key-batching/
  README.md                       # Python 客户端实战与统计口径
  requirements.txt                # 固定 confluent-kafka 依赖
  app.py                          # 投递、回读、批处理对照与自测
  verify.sh                       # 三个独立 Topic 的完整验收
labs/ch05-consumer-groups/
  README.md                       # 真实成员、分区归属与验收边界
  requirements.txt                # 复用固定客户端依赖
  app.py                          # 多进程订阅、管理查询、完整性断言
  verify.sh                       # 独立 Topic 与两个组的完整验收
```

## 学习环境的边界

单节点、单副本、PLAINTEXT 只用于本地入门实验，没有高可用或生产安全保证。服务宿主端口只绑定 `127.0.0.1:19092`，不向局域网开放。正常停止保留命名数据卷，不默认删除卷；每次实验新建少量 Topic，长期练习时需要按明确清单自行清理实验数据。

## 参考资料

- [Apache Kafka 官方文档](https://kafka.apache.org/documentation/)
- [Apache Kafka 下载与发布信息](https://kafka.apache.org/downloads/)
- [官方 Docker 使用指南](https://github.com/apache/kafka/blob/trunk/docker/README.md)
- [固定实验镜像](https://hub.docker.com/r/apache/kafka/tags?name=4.3.1)

## 后续学习方式

读完当前章并做完实验后，可以说：

- “继续”：交付第 6 章 Offset 提交、重启与重复消费，不跳章。
- “我没看懂 offset”：先解释、举例；必要时补入对应章节并推送。
- “我的实验报错了”：贴出错误，先排错再进入下一章。
