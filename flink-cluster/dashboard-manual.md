# Flink Dashboard — User Manual

**Version:** Apache Flink 2.3.0  
**URL:** [http://localhost:8081](http://localhost:8081)  
**Auto-refresh:** every 3 seconds

---

## Table of Contents

1. [Overview](#1-overview)
2. [Dashboard Tabs](#2-dashboard-tabs)
   - [Overview Tab](#21-overview-tab)
   - [Running Jobs Tab](#22-running-jobs-tab)
   - [Completed Jobs Tab](#23-completed-jobs-tab)
   - [Task Managers Tab](#24-task-managers-tab)
   - [Job Manager Tab](#25-job-manager-tab)
   - [Submit Job Tab](#26-submit-job-tab)
   - [Metrics Tab](#27-metrics-tab)
3. [Key Metrics Reference](#3-key-metrics-reference)
   - [Cluster-Level Metrics](#31-cluster-level-metrics)
   - [JVM Metrics](#32-jvm-metrics)
4. [Common Tasks](#4-common-tasks)
   - [Read the cluster health at a glance](#41-read-the-cluster-health-at-a-glance)
   - [Monitor a running job](#42-monitor-a-running-job)
   - [Investigate a failed job](#43-investigate-a-failed-job)
   - [Submit a new job](#44-submit-a-new-job)
   - [Scale the cluster](#45-scale-the-cluster)
5. [Troubleshooting](#5-troubleshooting)
   - [TaskManager not connecting](#51-taskmanager-not-connecting)
   - [Job stuck or slow](#52-job-stuck-or-slow)
   - [Out of memory](#53-out-of-memory)
   - [Dashboard not loading](#54-dashboard-not-loading)

---

## 1. Overview

The Flink Dashboard is the web-based control centre for your Flink cluster. From it you can:

- See cluster health (how many TaskManagers, available slots, running jobs)
- Monitor running and completed jobs with operator-level detail
- View throughput, backpressure, and checkpoint statistics per operator
- Inspect JVM metrics (heap, GC, threads, CPU) for both JobManager and each TaskManager
- Browse JobManager and TaskManager logs
- Submit new jobs (JAR upload)
- Build custom metric charts

---

## 2. Dashboard Tabs

### 2.1 Overview Tab

This is the landing page. It shows a compact summary of the entire cluster:

| Field | Meaning |
|---|---|
| **Task Managers** | Number of connected TaskManager containers |
| **Slots** | `available / total` — task slots are the parallel processing units |
| **Jobs** | `running / finished / failed / cancelled` |
| **Flink Version** | Release and commit hash |

> For the default cluster that ships with this repo:  
> 1 TaskManager, 4 slots, 0 jobs — a healthy idle cluster.

The four counts in **Jobs** are clickable — they filter the Jobs list below.

---

### 2.2 Running Jobs Tab

When a job is running, this tab shows a directed acyclic graph (DAG) of the job's operators (source → transformation → sink). Each operator node displays:

| Indicator | Meaning |
|---|---|
| **Parallelism** | How many parallel subtasks this operator runs as |
| **Status** | Green = running, yellow = finishing, red = failed |
| **Throughput** | Records/second and bytes/second per operator |
| **Backpressure** | Colour-coded: **blue** (OK), **yellow** (high), **red** (back-pressured) |

Clicking an operator expands a details panel with:

**Subtask List** — a table of every parallel subtask:
| Column | What it tells you |
|---|---|
| Attempt | Execution attempt number (0 = first try) |
| Status | `RUNNING`, `FINISHED`, `FAILED`, `CANCELLING` |
| Host | Which TaskManager the subtask runs on |
| Input / Output | Records received and sent |
| Duration | How long the task has been running |
| Backpressure | Subtask-level pressure reading |
| Acc. Backpressure | Time (ms) spent in backpressure since start |

**Metrics per subtask** (click a subtask row):
- `numRecordsIn` / `numRecordsOut` — records processed
- `numBytesIn` / `numBytesOut` — data volume
- `currentInputWatermark` — event-time watermark (lag indicator)
- `checkpointAlignmentTime` — time spent aligned to barriers

**Checkpoints tab** (next to the operator detail):
| Column | Meaning |
|---|---|
| ID | Sequential checkpoint number |
| Trigger Time | When the checkpoint was initiated |
| Latest Acknowledgment | When the last subtask confirmed |
| End to End Duration | Total checkpoint wall-clock time |
| State Size | Size of persisted state on the backend |
| Buffered During Alignment | Bytes buffered during barrier alignment |
| Status | `COMPLETED`, `IN_PROGRESS`, `FAILED` |

A slow or failing checkpoint is the single most common sign of trouble in a Flink job.

**Exceptions tab** — shows the stack traces for any failed subtasks in this job.

---

### 2.3 Completed Jobs Tab

Shows finished, cancelled, and failed jobs with:

| Column | Use |
|---|---|
| Job Name | Name assigned at submission |
| Duration | Wall-clock runtime |
| Status | `FINISHED`, `FAILED`, `CANCELED` |
| Start / End Time | When it ran |
| Task Status | A bar showing how many tasks succeeded/failed |

Click a job to see its DAG (no longer live, but the full plan and metrics are preserved). For a failed job this is where you click into the **Exceptions** tab to find the root cause.

---

### 2.4 Task Managers Tab

Lists every registered TaskManager. Click one to drill into its details:

**JVM & Memory** — real-time gauges for:
- Heap Used / Committed / Max
- Non-Heap memory
- Direct buffers (used for network communication)
- Mapped / Metaspace

**Garbage Collection:**
- `G1 Young Generation` — count and cumulative time
- `G1 Old Generation` — count and cumulative time
- `All` — combined
- Watch for high GC time → sign of memory pressure

**Network / Shuffle:**
- `Netty Shuffle Memory` — available / used / total (segments)
- Low available segments → backpressure bottleneck

**Hardware** — the host's physical CPU cores and memory (from the JVM's perspective)

**Logs** link — view the TaskManager's stdout/stderr and .log file directly in the browser.

---

### 2.5 Job Manager Tab

Same structure as the Task Manager detail, but for the JobManager JVM:

| Metric group | What to watch |
|---|---|
| **Heap** | Used / Committed / Max — rising heap with no GC plateau means a leak |
| **GC Time** | Elevated `G1_Old_Generation.Time` → head pressure or excessive object churn |
| **CPU Load** | System-wide CPU utilisation |
| **Class Loader** | Loaded vs. unloaded classes — rapid churn can indicate dynamic class loading issues |
| **File Descriptors** | Open vs. Max — hitting the limit prevents new connections |
| **Task Slots** | `taskSlotsAvailable` / `taskSlotsTotal` — at-a-glance capacity |
| **Registered TMs** | `numRegisteredTaskManagers` — should equal your compose scale count |
| **Running Jobs** | `numRunningJobs` — should match the Running Jobs tab |

**Logs** link — JobManager logs are here. Start here for crashes, configuration errors, or deployment failures.

---

### 2.6 Submit Job Tab

A simple form to run a new job:

1. Click **Add Jar** and select your JAR
2. The dashboard parses the JAR and shows available entry points (main classes)
3. Enter any program arguments in the text box
4. Click **Submit**

> If `web-submit` is disabled in your config, this tab won't appear. It's enabled by default in the local compose setup.

Alternatively, submit from the command line:

```bash
# copy the jar into the jobmanager container
docker cp my-job.jar flink-jobmanager:/job.jar
docker exec flink-jobmanager flink run /job.jar --my-arg value
```

Or use the helper script:

```bash
cd flink-cluster && ./submit-job.sh path/to/my-job.jar
```

---

### 2.7 Metrics Tab

A custom chart builder. Select one or more metrics from the dropdown, choose a time range (last 1m / 5m / 15m / 1h), and the dashboard plots live graphs.

Available metric categories:

| Category | Example metrics |
|---|---|
| `taskSlots*` | `taskSlotsAvailable`, `taskSlotsTotal` |
| `numRegisteredTaskManagers` | Count of TMs |
| `numRunningJobs` | Count of running jobs |
| `Status.JVM.Memory.Heap.*` | Used, Committed, Max |
| `Status.JVM.Memory.Metaspace.*` | Used, Committed, Max |
| `Status.JVM.Memory.Direct.*` | Count, MemoryUsed, TotalCapacity |
| `Status.JVM.Memory.Mapped.*` | Count, MemoryUsed, TotalCapacity |
| `Status.JVM.CPU.*` | Time, Load |
| `Status.JVM.Threads.Count` | Active threads |
| `Status.JVM.GarbageCollector.*` | Count, Time, TimeMsPerSecond per GC type |
| `Status.JVM.ClassLoader.*` | ClassesLoaded, ClassesUnloaded |
| `Status.JVM.FileDescriptor.*` | Open, Max |
| `Status.Flink.Memory.Managed.Total` | Managed memory (state backend) |
| `Status.Shuffle.Netty.*` | UsedMemory, AvailableMemory |

When a job is running, job-specific metrics also appear in this dropdown (records in/out, bytes, checkpoint duration, etc.).

---

## 3. Key Metrics Reference

### 3.1 Cluster-Level Metrics

| Metric | When to care |
|---|---|
| `taskSlotsAvailable` < `taskSlotsTotal` | Some slots are occupied — expected when jobs run |
| `taskSlotsAvailable` == 0 | No capacity for new jobs — scale up or stop idle jobs |
| `numRegisteredTaskManagers` < expected | A TaskManager failed to register — check logs |
| `numRunningJobs` + `numRunningJobs` | Shouldn't drop unexpectedly unless a job failed |

### 3.2 JVM Metrics

| Metric | Warning sign | Action |
|---|---|---|
| `Heap.Used` → close to `Heap.Max` | Memory pressure | Increase taskmanager.memory.process.size |
| `G1_Old_Generation.TimeMsPerSecond` > 500ms | Excessive old-gen GC | Check for memory leak or reduce state size |
| `Direct.MemoryUsed` near `Direct.TotalCapacity` | Network buffer exhaustion | Increase network memory |
| `CPU.Load` sustained > 90% | CPU-bound job | Increase parallelism or scale out |
| `FileDescriptor.Open` near `FileDescriptor.Max` | Socket leak | Investigate open connections |
| `NettyShuffleMemoryAvailable` → 0 | Backpressure | Tune `taskmanager.memory.network.max` |

---

## 4. Common Tasks

### 4.1 Read the cluster health at a glance

1. Open the **Overview** tab  
2. Check that **Task Managers ≥ 1** (or your expected count)  
3. Check that **Slots available > 0** if you want to submit a job  
4. Check that **Jobs running / failed** matches your expectations  

### 4.2 Monitor a running job

1. Go to **Running Jobs** and click the job name  
2. Look at the DAG — are all operators green?  
3. Click an operator → **Subtask List** — are all subtasks RUNNING?  
4. Click **Checkpoints** — are checkpoints completing? Any failures?  
5. If you see backpressure (yellow/red), check the back-pressured operator's metrics and its downstream sink speed  

### 4.3 Investigate a failed job

1. Go to **Completed Jobs**  
2. Find the failed job (red badge) and click it  
3. Click the operator that failed (red highlight in the DAG)  
4. Click the **Exceptions** tab — the full stack trace is there  
5. Cross-reference with the TaskManager logs (from **Task Managers** tab → logs) that was running that subtask  

### 4.4 Submit a new job

From the dashboard:
1. **Submit Job** tab → **Add Jar** → select your JAR  
2. Optionally enter main class and program args  
3. Click **Submit**  

From the CLI:
```bash
docker exec flink-jobmanager flink run /path/to/job.jar
```

### 4.5 Scale the cluster

Add more TaskManagers without downtime:

```bash
docker compose up -d --scale taskmanager=3
```

The new TMs appear automatically in the **Task Managers** tab within seconds.

---

## 5. Troubleshooting

### 5.1 TaskManager not connecting

**Symptom:** `numRegisteredTaskManagers` stays at 0, or no TMs in the **Task Managers** tab.

**Check:**
1. `docker compose ps` — is the taskmanager container running?  
2. `docker compose logs taskmanager` — any connection errors?  
3. Verify both containers are on the same Docker network (`flink-cluster_flink-net`)  
4. The `jobmanager.rpc.address` in the TaskManager env must match the JobManager's hostname  

### 5.2 Job stuck or slow

**Check:**
1. **Running Jobs** → operator detail → check for backpressure  
2. **Checkpoints** — failing checkpoints stall the pipeline (checkpoint timeout defaults to 10 min)  
3. **Metrics tab** → `Status.Shuffle.Netty.AvailableMemory` — if 0, backpressure is full  
4. **Metrics tab** → `Status.JVM.CPU.Load` on the affected TaskManager  

### 5.3 Out of memory

**Symptom:** Job fails with `OutOfMemoryError` or TaskManager exits abruptly.

**Fix:**
1. Increase `taskmanager.memory.process.size` in the compose file (currently 2048m)  
2. Increase `jobmanager.memory.process.size` similarly  
3. Reduce parallelism if the total memory per slot is too low (2048m / 4 slots = 512m/slot)  
4. Flink's memory model: `process = framework + task + managed + network + metaspace + overhead`  
   - Each term is auto-derived from the total, so raising `process.size` scales all

### 5.4 Dashboard not loading

**Check:**
1. `docker compose logs jobmanager` — is the JobManager healthy?  
2. `curl -sf http://localhost:8081/overview` — does the REST API respond?  
3. Port mapping: `docker compose ps` — is `0.0.0.0:8081 → 8081` shown?  
4. Firewall / Docker network — ensure port 8081 isn't blocked on your host
