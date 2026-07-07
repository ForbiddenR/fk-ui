# fk-ui

## Flink

- `flink-cluster/` — Docker Compose Flink cluster and dashboard manuals.
- `flink-test-job/` — buildable long-running Flink Java test job for observing dashboard state.

### Build the test job JAR

```bash
cd flink-test-job
mvn clean package
```

The JAR is generated at:

```text
flink-test-job/target/flink-test-job-1.0.0.jar
```

### Submit the long-running test job

```bash
docker cp flink-test-job/target/flink-test-job-1.0.0.jar flink-jobmanager:/long-running-test-job.jar

docker exec flink-jobmanager flink run -d -p 2 \
  /long-running-test-job.jar \
  --events-per-second 10 \
  --keys 4 \
  --window-seconds 10
```

View output:

```bash
docker logs flink-taskmanager 2>&1 | grep 'window-summary'
```