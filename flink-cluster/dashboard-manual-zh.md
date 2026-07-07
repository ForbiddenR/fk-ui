# Flink 仪表盘 — 用户手册

**版本：** Apache Flink 2.3.0  
**网址：** [http://localhost:8081](http://localhost:8081)  
**自动刷新：** 每 3 秒

---

## 目录

1. [概述](#1-概述)
2. [仪表盘标签页](#2-仪表盘标签页)
   - [概览标签页](#21-概览标签页)
   - [运行中作业标签页](#22-运行中作业标签页)
   - [已完成作业标签页](#23-已完成作业标签页)
   - [任务管理器标签页](#24-任务管理器标签页)
   - [作业管理器标签页](#25-作业管理器标签页)
   - [提交作业标签页](#26-提交作业标签页)
   - [指标标签页](#27-指标标签页)
3. [关键指标参考](#3-关键指标参考)
   - [集群级指标](#31-集群级指标)
   - [JVM 指标](#32-jvm-指标)
4. [常见操作](#4-常见操作)
   - [一眼看出集群健康状态](#41-一眼看出集群健康状态)
   - [监控运行中的作业](#42-监控运行中的作业)
   - [排查失败的作业](#43-排查失败的作业)
   - [提交新作业](#44-提交新作业)
   - [扩缩集群](#45-扩缩集群)
5. [故障排查](#5-故障排查)
   - [TaskManager 无法连接](#51-taskmanager-无法连接)
   - [作业卡住或运行缓慢](#52-作业卡住或运行缓慢)
   - [内存溢出](#53-内存溢出)
   - [仪表盘无法加载](#54-仪表盘无法加载)

---

## 1. 概述

Flink 仪表盘是 Flink 集群的 Web 控制中心。通过它可以：

- 查看集群健康状态（TaskManager 数量、可用槽位数、运行中的作业数）
- 以算子级别的粒度监控运行中和已完成的作业
- 查看每个算子的吞吐量、背压和检查点统计信息
- 检查 JobManager 和每个 TaskManager 的 JVM 指标（堆、GC、线程、CPU）
- 浏览 JobManager 和 TaskManager 的日志
- 提交新作业（上传 JAR）
- 构建自定义指标图表

---

## 2. 仪表盘标签页

### 2.1 概览标签页

这是仪表盘的首页，显示整个集群的紧凑摘要：

| 字段 | 含义 |
|---|---|
| **Task Managers** | 已连接的 TaskManager 容器数量 |
| **Slots** | `可用 / 总计` — 槽位是 Flink 的并行处理单元 |
| **Jobs** | `运行中 / 已完成 / 失败 / 已取消` |
| **Flink Version** | 版本号和提交哈希 |

> 对于本仓库默认启动的集群：1 个 TaskManager，4 个槽位，0 个作业 — 健康的空闲集群。

**Jobs** 中的四个数字是可点击的 — 点击后会过滤下方的作业列表。

---

### 2.2 运行中作业标签页

当有作业在运行时，此标签页显示作业算子的有向无环图（DAG，source → transformation → sink）。每个算子节点显示：

| 指标 | 含义 |
|---|---|
| **Parallelism** | 该算子运行的并行子任务数 |
| **Status** | 绿色 = 运行中，黄色 = 正在结束，红色 = 失败 |
| **Throughput** | 每个算子每秒处理的行数和字节数 |
| **Backpressure** | 颜色指示：**蓝色**（正常）、**黄色**（较高）、**红色**（背压严重） |

点击某个算子会展开详情面板：

**子任务列表** — 每个并行子任务的表格：
| 列 | 含义 |
|---|---|
| Attempt | 执行尝试次数（0 = 首次尝试） |
| Status | `RUNNING`、`FINISHED`、`FAILED`、`CANCELLING` |
| Host | 子任务运行在哪个 TaskManager 上 |
| Input / Output | 接收和发送的记录数 |
| Duration | 任务已运行时长 |
| Backpressure | 子任务级别的压力值 |
| Acc. Backpressure | 启动以来处于背压状态的累计时长（ms） |

**每个子任务的指标**（点击子任务行）：
- `numRecordsIn` / `numRecordsOut` — 已处理记录数
- `numBytesIn` / `numBytesOut` — 数据量
- `currentInputWatermark` — 事件时间水位线（延迟指标）
- `checkpointAlignmentTime` — 等待 barrier 对齐的时间

**检查点标签页**（在算子详情旁边）：
| 列 | 含义 |
|---|---|
| ID | 检查点序号 |
| Trigger Time | 检查点触发时间 |
| Latest Acknowledgment | 最后一个子任务确认的时间 |
| End to End Duration | 检查点总耗时 |
| State Size | 状态后端中持久化状态的大小 |
| Buffered During Alignment | Barrier 对齐期间缓冲的字节数 |
| Status | `COMPLETED`、`IN_PROGRESS`、`FAILED` |

检查点慢或失败是 Flink 作业出问题最常见的信号。

**异常标签页** — 显示该作业中任何失败子任务的完整堆栈跟踪。

---

### 2.3 已完成作业标签页

显示已完成、已取消和失败的作业：

| 列 | 作用 |
|---|---|
| Job Name | 提交时指定的作业名称 |
| Duration | 运行时长 |
| Status | `FINISHED`、`FAILED`、`CANCELED` |
| Start / End Time | 运行起止时间 |
| Task Status | 用条形图显示成功/失败的任务数 |

点击某个作业可查看其 DAG（不再实时更新，但完整的执行计划和指标会被保留）。对于失败的作业，在此进入**异常标签页**查找根本原因。

---

### 2.4 任务管理器标签页

列出所有已注册的 TaskManager。点击某个 TaskManager 可查看详情：

**JVM 与内存** — 实时仪表盘：
- 堆已用 / 已提交 / 最大值
- 非堆内存
- 直接缓冲区（用于网络通信）
- 映射缓冲区 / Metaspace

**垃圾回收：**
- `G1 Young Generation` — 次数和累计时间
- `G1 Old Generation` — 次数和累计时间
- `All` — 合计
- GC 时间过高 → 内存压力信号

**网络 / Shuffle：**
- `Netty Shuffle Memory` — 可用 / 已用 / 总计（段数）
- 可用段数过低 → 背压瓶颈

**硬件** — 宿主机的物理 CPU 核数和内存（JVM 视角）

**日志链接** — 直接在浏览器中查看 TaskManager 的 stdout/stderr 和 .log 文件。

---

### 2.5 作业管理器标签页

结构与 TaskManager 详情相同，但针对 JobManager 的 JVM：

| 指标组 | 关注点 |
|---|---|
| **Heap** | 已用 / 已提交 / 最大值 — 堆持续增长且没有 GC 平台期可能表示内存泄漏 |
| **GC Time** | `G1_Old_Generation.Time` 升高 → 堆压力或对象创建过多 |
| **CPU Load** | 系统级 CPU 使用率 |
| **Class Loader** | 加载与卸载的类数 — 频繁变动可能表示动态类加载问题 |
| **File Descriptors** | 打开数 / 最大值 — 达到上限会阻止新建连接 |
| **Task Slots** | `taskSlotsAvailable` / `taskSlotsTotal` — 集群容量一览 |
| **Registered TMs** | `numRegisteredTaskManagers` — 应与 compose 的 scale 数量一致 |
| **Running Jobs** | `numRunningJobs` — 应与"运行中作业"标签页一致 |

**日志链接** — JobManager 的日志在此查看。崩溃、配置错误或部署失败时首先查看这里。

---

### 2.6 提交作业标签页

提交新作业的表单：

1. 点击 **Add Jar**，选择你的 JAR 文件
2. 仪表盘会解析 JAR 并显示可用的入口类
3. 在文本框中输入程序参数（可选）
4. 点击 **Submit**

> 如果配置中禁用了 `web-submit`，此标签页不会显示。在本地 compose 配置中默认启用。

也可以从命令行提交：

```bash
# 将 JAR 复制到 jobmanager 容器中
docker cp my-job.jar flink-jobmanager:/job.jar
docker exec flink-jobmanager flink run /job.jar --my-arg value
```

或使用辅助脚本：

```bash
cd flink-cluster && ./submit-job.sh path/to/my-job.jar
```

---

### 2.7 指标标签页

自定义图表生成器。从下拉菜单中选择一个或多个指标，选择时间范围（最近 1 分钟 / 5 分钟 / 15 分钟 / 1 小时），仪表盘会绘制实时图表。

可用的指标类别：

| 类别 | 示例指标 |
|---|---|
| `taskSlots*` | `taskSlotsAvailable`、`taskSlotsTotal` |
| `numRegisteredTaskManagers` | TaskManager 数量 |
| `numRunningJobs` | 运行中作业数量 |
| `Status.JVM.Memory.Heap.*` | 已使用、已提交、最大值 |
| `Status.JVM.Memory.Metaspace.*` | 已使用、已提交、最大值 |
| `Status.JVM.Memory.Direct.*` | 次数、已用内存、总容量 |
| `Status.JVM.Memory.Mapped.*` | 次数、已用内存、总容量 |
| `Status.JVM.CPU.*` | 时间、负载 |
| `Status.JVM.Threads.Count` | 活跃线程数 |
| `Status.JVM.GarbageCollector.*` | 每种 GC 的次数、时间、每秒时间 |
| `Status.JVM.ClassLoader.*` | 已加载类、已卸载类 |
| `Status.JVM.FileDescriptor.*` | 已打开、最大值 |
| `Status.Flink.Memory.Managed.Total` | 托管内存（状态后端） |
| `Status.Shuffle.Netty.*` | 已用内存、可用内存 |

当有作业在运行时，作业级指标也会出现在下拉菜单中（记录输入/输出、字节数、检查点耗时等）。

---

## 3. 关键指标参考

### 3.1 集群级指标

| 指标 | 何时关注 |
|---|---|
| `taskSlotsAvailable` < `taskSlotsTotal` | 部分槽位被占用 — 作业运行时正常 |
| `taskSlotsAvailable` == 0 | 没有余量提交新作业 — 扩容或停止空闲作业 |
| `numRegisteredTaskManagers` < 预期值 | TaskManager 注册失败 — 查看日志 |
| `numRunningJobs` 无故下降 | 作业可能意外失败 |

### 3.2 JVM 指标

| 指标 | 警告信号 | 操作 |
|---|---|---|
| `Heap.Used` 接近 `Heap.Max` | 内存压力 | 增大 taskmanager.memory.process.size |
| `G1_Old_Generation.TimeMsPerSecond` > 500ms | 老年代 GC 过于频繁 | 检查内存泄漏或减小状态大小 |
| `Direct.MemoryUsed` 接近 `Direct.TotalCapacity` | 网络缓冲区耗尽 | 增大网络内存 |
| `CPU.Load` 持续 > 90% | CPU 密集型作业 | 增大并行度或扩容 |
| `FileDescriptor.Open` 接近 `FileDescriptor.Max` | 套接字泄漏 | 排查连接数异常 |
| `NettyShuffleMemoryAvailable` → 0 | 背压瓶颈 | 调整 `taskmanager.memory.network.max` |

---

## 4. 常见操作

### 4.1 一眼看出集群健康状态

1. 打开**概览标签页**
2. 检查 **Task Managers ≥ 1**（或你期望的数量）
3. 如果要提交作业，检查 **Slots available > 0**
4. 检查 **Jobs running / failed** 是否符合预期

### 4.2 监控运行中的作业

1. 进入**运行中作业**，点击作业名称
2. 查看 DAG — 所有算子是否都是绿色？
3. 点击某个算子 → **子任务列表** — 所有子任务是否都是 RUNNING 状态？
4. 点击**检查点** — 检查点是否完成？是否有失败的？
5. 如果看到背压（黄色/红色），检查背压算子的指标及其下游 sink 的处理速度

### 4.3 排查失败的作业

1. 进入**已完成作业**
2. 找到失败的作业（红色标识），点击进入
3. 点击 DAG 中失败的算子（红色高亮）
4. 点击**异常标签页** — 完整的堆栈跟踪就在那里
5. 结合 TaskManager 日志交叉分析（从**任务管理器标签页** → 日志进入正在运行该子任务的 TM）

### 4.4 提交新作业

从仪表盘提交：
1. **提交作业标签页** → **Add Jar** → 选择 JAR 文件
2. 可选：指定主类和程序参数
3. 点击 **Submit**

从命令行提交：
```bash
docker exec flink-jobmanager flink run /path/to/job.jar
```

### 4.5 扩缩集群

不停机添加更多 TaskManager：

```bash
docker compose up -d --scale taskmanager=3
```

新的 TaskManager 会在几秒内自动出现在**任务管理器标签页**中。

---

## 5. 故障排查

### 5.1 TaskManager 无法连接

**现象：** `numRegisteredTaskManagers` 一直为 0，或**任务管理器标签页**中没有任何 TM。

**排查：**
1. `docker compose ps` — taskmanager 容器是否在运行？
2. `docker compose logs taskmanager` — 是否有连接错误？
3. 确认两个容器在同一个 Docker 网络上（`flink-cluster_flink-net`）
4. TaskManager 环境变量中的 `jobmanager.rpc.address` 必须与 JobManager 的主机名一致

### 5.2 作业卡住或运行缓慢

**排查：**
1. **运行中作业** → 算子详情 → 检查是否有背压
2. **检查点** — 失败的检查点会使流水线停滞（检查点超时默认 10 分钟）
3. **指标标签页** → `Status.Shuffle.Netty.AvailableMemory` — 如果为 0，表示完全背压
4. **指标标签页** → `Status.JVM.CPU.Load` — 查看受影响的 TaskManager

### 5.3 内存溢出

**现象：** 作业因 `OutOfMemoryError` 失败，或 TaskManager 异常退出。

**解决：**
1. 增大 compose 文件中的 `taskmanager.memory.process.size`（当前为 2048m）
2. 同样增大 `jobmanager.memory.process.size`
3. 如果每个槽位内存不足（2048m / 4 个槽位 = 每个槽位 512m），可降低并行度
4. Flink 内存模型：`process = framework + task + managed + network + metaspace + overhead`
   - 每个分量会根据总量自动推导，因此提高 `process.size` 会按比例放大所有部分

### 5.4 仪表盘无法加载

**排查：**
1. `docker compose logs jobmanager` — JobManager 是否健康？
2. `curl -sf http://localhost:8081/overview` — REST API 是否响应？
3. 端口映射：`docker compose ps` — 是否显示 `0.0.0.0:8081 → 8081`？
4. 防火墙 / Docker 网络 — 确保宿主机上 8081 端口未被阻止
