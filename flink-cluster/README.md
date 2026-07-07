# Flink Cluster (Docker Compose)

A minimal Flink **2.3.0** cluster with one JobManager and one TaskManager.

## Quick Start

```bash
# Start the cluster
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f
```

Open [http://localhost:8081](http://localhost:8081) for the Flink Web UI.

## Scale Out

Add more TaskManagers without downtime:

```bash
docker compose up -d --scale taskmanager=3
```

## Submit a Job

```bash
./submit-job.sh path/to/my-job.jar --my-args
```

Or manually — copy the JAR into a container first:

```bash
docker cp my-job.jar flink-jobmanager:/job.jar
docker exec flink-jobmanager flink run /job.jar
```

## Long-Running Test Job

A buildable test job is available in `../flink-test-job`. It continuously generates synthetic events, runs `keyBy + window`, and prints summaries to TaskManager stdout.

```bash
cd ../flink-test-job
mvn clean package

docker cp target/flink-test-job-1.0.0.jar flink-jobmanager:/long-running-test-job.jar

docker exec flink-jobmanager flink run -d -p 2 \
  /long-running-test-job.jar \
  --events-per-second 10 \
  --keys 4 \
  --window-seconds 10
```

View it in the dashboard under **Running Jobs**.

Printed window output is in TaskManager stdout:

```bash
docker logs flink-taskmanager 2>&1 | grep 'window-summary'
```

## SQL Client

```bash
./sql-client.sh
```

## Stop

```bash
# Keep containers & data (you can restart)
docker compose stop

# Wipe everything
docker compose down -v
```

## Configuration

Edit `FLINK_PROPERTIES` in `docker-compose.yml` to tune memory, parallelism,
checkpointing, or state backend settings.

## Versions

| Image | Flink | Java |
|-------|-------|------|
| `flink:2.3.0-java17` | 2.3.0 | 17 |
| `flink:2.3.0-java21` | 2.3.0 | 21 |
| `flink:2.3.0-java11` | 2.3.0 | 11 |

Swap the `image:` tag in `docker-compose.yml` to try a different version.
