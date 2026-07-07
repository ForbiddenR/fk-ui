# 在 Pod 内控制 Kubernetes 扩缩容 — 笔记

## 简短结论

从技术上说，**可以在 Pod 内控制 Kubernetes 扩缩容**，但通常**不建议让普通业务 Pod 直接做这件事**。

更合理的设计是使用专门的 Kubernetes 控制组件，例如：

- Kubernetes Operator
- 自定义 Controller
- Horizontal Pod Autoscaler（HPA）
- KEDA
- Cluster Autoscaler
- Karpenter
- Flink Kubernetes Operator / Autoscaler

核心思想是：

> Pod 只要拥有合适的 ServiceAccount 和 RBAC 权限，就可以调用 Kubernetes API；但扩缩容逻辑通常应该放在专门的 Controller 里，而不是普通业务程序里。

---

## 两种不同的“扩容”

### 1. 扩展工作负载副本数

这指的是增加或减少应用 Pod 的数量。

例如：

```text
TaskManager 副本数：2 → 5
Deployment 副本数：3 → 10
StatefulSet 副本数：1 → 4
```

这属于 Kubernetes 的工作负载扩缩容。

通常修改这些资源：

- `Deployment.spec.replicas`
- `StatefulSet.spec.replicas`
- `FlinkDeployment.spec.taskManager.replicas`

---

### 2. 扩展 Kubernetes 集群节点

这指的是增加或减少 Kubernetes 集群中的 worker node。

例如：

```text
当前集群 CPU / 内存不足
    ↓
新增 Kubernetes worker node
```

这种节点级扩容通常应该交给：

- Cluster Autoscaler
- Karpenter
- 云厂商的 autoscaling group

普通应用 Pod 通常**不应该直接控制节点级扩容**。

---

## 在 Pod 内控制扩缩容是否可行？

可行。

一个 Pod 如果具备以下条件，就可以控制 Kubernetes 资源：

1. ServiceAccount
2. RBAC 权限
3. 能访问 Kubernetes API
4. 使用 `kubectl`、`client-go`、Python Kubernetes client 等客户端

例如，在 Pod 内执行：

```bash
kubectl scale deployment taskmanager --replicas=5
```

只有当该 Pod 的 ServiceAccount 拥有更新或 patch 这个 Deployment 的权限时，这条命令才会成功。

---

## 为什么普通 Pod 通常不应该这样做？

### 1. 安全风险大

如果给普通业务 Pod 这些权限：

```text
patch deployments
update statefulsets
list pods
watch workloads
```

一旦业务程序被攻击，攻击者也可能获得修改集群工作负载的能力。

应避免授予过大的权限，例如：

```text
cluster-admin
```

应该遵循最小权限原则。

---

### 2. 容易产生控制环震荡

一个简单粗暴的扩缩容循环可能会震荡：

```text
CPU 高 → 扩容
CPU 降低 → 缩容
CPU 再次升高 → 再扩容
```

如果没有冷却时间、上下限和稳定窗口，系统可能不断扩容、缩容，导致不稳定。

---

### 3. 可能和已有 Controller 冲突

Kubernetes 已经有很多控制器，例如：

- Deployment Controller
- StatefulSet Controller
- HPA
- VPA
- KEDA
- Flink Kubernetes Operator

如果应用 Pod 也在修改 replicas，多个控制器可能会争夺同一个 desired state，造成冲突。

---

### 4. Pod 生命周期不稳定

Pod 是可丢弃的。

它可能被：

- 重启
- 驱逐
- 重新调度
- 滚动更新时复制
- 节点维护时终止

如果扩缩容逻辑写在普通业务 Pod 里，那么这个“控制器”本身可能消失，或者被复制成多个实例。

真正的 Controller 应该具备：

- 幂等性
- 高可用
- 基于 reconcile 的设计
- 可重试能力

---

## 更好的设计选择

### 方案 1：Flink Kubernetes Operator

如果 Flink 运行在 Kubernetes 上，优先考虑使用 Flink Kubernetes Operator。

它可以管理：

- JobManager Pod
- TaskManager Pod
- Flink Job 提交
- savepoint
- restart
- upgrade
- FlinkDeployment 自定义资源

概念流程如下：

```text
Flink 指标显示需要更多资源
    ↓
Flink autoscaler / operator 计算期望副本数
    ↓
Operator patch FlinkDeployment
    ↓
Kubernetes 创建更多 TaskManager Pod
    ↓
TaskManager 注册到 JobManager
    ↓
Flink 将任务调度到新的 slot 上
```

---

### 方案 2：HPA

如果只是标准的 CPU / 内存驱动扩缩容，使用 HPA。

例如：

```text
CPU 使用率超过阈值
    ↓
HPA 增加 replicas
```

适合普通无状态服务。

---

### 方案 3：KEDA

如果扩缩容由外部事件或自定义指标驱动，可以使用 KEDA，例如：

- Kafka lag
- 队列长度
- Prometheus 指标
- Redis Streams
- RabbitMQ backlog

例如：

```text
Kafka lag 很高
    ↓
KEDA 增加消费者副本数
```

---

### 方案 4：Cluster Autoscaler / Karpenter

如果要扩的是节点，应该使用这些组件。

典型流程：

```text
新的 Pod 因为节点资源不足而 Pending
    ↓
Cluster Autoscaler 或 Karpenter 创建新节点
    ↓
Pending 的 Pod 被调度到新节点
```

---

## 什么时候可以接受在 Pod 内控制扩缩容？

如果这个 Pod 不是普通业务 Pod，而是专门的控制组件，那么可以接受。

| 场景 | 是否合理 | 说明 |
|---|---:|---|
| 自定义 Kubernetes Operator | 是 | 这是正确模式 |
| 专门的 autoscaler / controller Pod | 是 | 控制逻辑应与业务逻辑分离 |
| 内部平台控制组件 | 是 | 需要严格 RBAC 和 reconcile 设计 |
| 一次性维护 Job | 视情况 | 权限要窄，生命周期要短 |
| 普通业务 Pod 自己扩自己 | 通常不建议 | 安全和稳定性风险大 |
| Flink TaskManager 自己扩 TaskManager | 通常不建议 | 优先用 Operator / autoscaler |

---

## 如果必须从 Pod 内控制扩缩容

至少遵守以下原则。

### 1. 使用独立 ServiceAccount

不要使用 default ServiceAccount。

```yaml
serviceAccountName: flink-scaler
```

---

### 2. 使用最小 RBAC

只授予必要权限。

例如，只允许 patch 某个 namespace 下的指定资源。

避免：

```text
cluster-admin
```

优先只给：

```text
get
patch
update
```

并且只针对必须控制的资源。

---

### 3. 做成 Controller，而不是混在业务逻辑里

推荐：

```text
flink-autoscaler-controller Pod
```

避免：

```text
TaskManager Pod 直接扩容 TaskManager Pod
```

---

### 4. 扩缩容逻辑要幂等

避免这种逻辑：

```text
if load is high:
    replicas = replicas + 1
```

更好的做法是：

```text
desiredReplicas = calculateDesiredReplicas(metrics)
patch replicas to desiredReplicas
```

这样可以避免重复触发导致过度扩容。

---

### 5. 设置冷却时间和上下限

示例规则：

```text
minReplicas: 1
maxReplicas: 20
scaleUpCooldown: 5 minutes
scaleDownCooldown: 15 minutes
```

通常缩容应该比扩容更保守。

---

### 6. 只修改声明式资源

不要直接创建或删除单个 Pod。

应该修改：

- Deployment replicas
- StatefulSet replicas
- FlinkDeployment spec

然后让 Kubernetes controller 自己 reconcile 出真实 Pod。

---

## 推荐的 Flink 自动扩缩容架构

如果 Flink 运行在 Kubernetes 上，推荐架构是：

```text
Flink Job
    ↓ 产生指标
Prometheus
    ↓ autoscaler / operator 查询指标
Flink Autoscaler / Flink Kubernetes Operator
    ↓ patch 期望状态
FlinkDeployment
    ↓ Kubernetes reconcile
TaskManager Pods
    ↓ 注册到 JobManager
Flink Job 使用新的 slots
```

这样职责比较清晰：

| 组件 | 职责 |
|---|---|
| Flink | 数据处理，以及在已有资源内调度任务 |
| Kubernetes | Pod 生命周期和集群编排 |
| Operator / Autoscaler | 扩缩容决策和状态 reconcile |
| Cluster Autoscaler / Karpenter | 节点级容量管理 |

---

## 最终经验法则

> 可以从 Pod 内控制 Kubernetes 扩缩容，但普通业务 Pod 不应该这么做。应使用专门的 Operator、Controller、HPA、KEDA 或 Cluster Autoscaler。

对于 Flink 来说：

> 优先使用 Flink Kubernetes Operator / Autoscaler 管理 TaskManager 扩缩容，让 Kubernetes 或 Karpenter 处理节点级扩容。
