# Long-Running Flink Test Job

This is a small Java Flink job for dashboard testing.

It continuously generates synthetic events, groups them by key, runs a processing-time tumbling window, and prints one summary per key/window to TaskManager stdout.

Pipeline:

```text
Synthetic Event Source
    ↓
keyBy(key)
    ↓
TumblingProcessingTimeWindow
    ↓
Aggregate count/sum
    ↓
print()
```

## Build

From the repo root:

```bash
cd flink-test-job
mvn clean package
```

The JAR will be created at:

```text
flink-test-job/target/flink-test-job-1.0.0.jar
```

## Submit

From the repo root:

```bash
docker cp flink-test-job/target/flink-test-job-1.0.0.jar flink-jobmanager:/long-running-test-job.jar

docker exec flink-jobmanager flink run -d \
  /long-running-test-job.jar \
  --events-per-second 20 \
  --keys 4 \
  --window-seconds 10
```

Optional: set job parallelism at submission time:

```bash
docker exec flink-jobmanager flink run -d -p 2 \
  /long-running-test-job.jar \
  --events-per-second 20 \
  --keys 4 \
  --window-seconds 10
```

## Parameters

| Parameter | Default | Meaning |
|---|---:|---|
| `--events-per-second` | `20` | Number of synthetic events emitted per source subtask per second |
| `--keys` | `4` | Number of logical keys, named `key-0`, `key-1`, ... |
| `--window-seconds` | `10` | Processing-time tumbling window size |
| `--max-events` | `-1` | Stop after N events per source subtask; `-1` means run forever |

## View in Dashboard

Open:

```text
http://localhost:8081
```

Then go to:

```text
Running Jobs → Long Running Status Job
```

Useful things to inspect:

- Job DAG
- Operator parallelism
- Subtasks
- Records in/out
- Backpressure
- TaskManager logs/stdout

## View Printed Output

Because the job uses `print()`, output goes to the TaskManager stdout:

```bash
docker logs -f flink-taskmanager
```

Filter only the window summaries:

```bash
docker logs flink-taskmanager 2>&1 | grep 'window-summary'
```

Example output:

```text
1> window-summary key=key-0 count=51 sum=281 first=2026-07-07T03:40:00.010Z last=2026-07-07T03:40:09.990Z
2> window-summary key=key-1 count=50 sum=275 first=2026-07-07T03:40:00.020Z last=2026-07-07T03:40:09.980Z
```

The prefix such as `1>` or `2>` indicates which print sink subtask produced the line.

## Stop the Job

List running jobs:

```bash
docker exec flink-jobmanager flink list
```

Cancel the job:

```bash
docker exec flink-jobmanager flink cancel <job-id>
```
