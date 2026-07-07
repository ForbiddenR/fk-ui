package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.ParameterTool;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * A tiny long-running Flink job for local dashboard testing.
 *
 * <p>The job continuously emits synthetic events, groups them by key, aggregates them in processing-time
 * windows, and prints one summary record per key/window to TaskManager stdout.
 */
public class LongRunningStatusJob {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);

    public static void main(String[] args) throws Exception {
        var params = ParameterTool.fromArgs(args);
        var settings = JobSettings.from(params);

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataStream<TestEvent> events = env
                .addSource(new SyntheticEventSource(settings))
                .name("Synthetic Event Source")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        events
                .keyBy(event -> event.key)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(settings.windowSeconds())))
                .aggregate(new EventAggregate())
                .map(new FormatSummary())
                .name("Format Window Summary")
                .print()
                .name("Print Window Summary");

        env.execute("Long Running Status Job - %d eps, %d keys, %ds windows".formatted(
                settings.eventsPerSecond(),
                settings.keyCount(),
                settings.windowSeconds()));
    }

    public record JobSettings(int eventsPerSecond, int keyCount, int windowSeconds, long maxEvents)
            implements Serializable {
        public static JobSettings from(ParameterTool params) {
            return new JobSettings(
                    Math.max(1, params.getInt("events-per-second", 20)),
                    Math.max(1, params.getInt("keys", 4)),
                    Math.max(1, params.getInt("window-seconds", 10)),
                    params.getLong("max-events", -1L));
        }
    }

    public static final class SyntheticEventSource extends RichParallelSourceFunction<TestEvent> {
        private final JobSettings settings;
        private volatile boolean running = true;

        public SyntheticEventSource(JobSettings settings) {
            this.settings = settings;
        }

        @Override
        public void run(SourceFunction.SourceContext<TestEvent> ctx) throws Exception {
            var taskInfo = getRuntimeContext().getTaskInfo();
            var runtimeInfo = new SourceRuntimeInfo(
                    taskInfo.getIndexOfThisSubtask(),
                    taskInfo.getNumberOfParallelSubtasks());
            long emitted = 0L;
            long sleepMillis = Math.max(1L, 1000L / settings.eventsPerSecond());

            while (running && (settings.maxEvents() < 0 || emitted < settings.maxEvents())) {
                long now = System.currentTimeMillis();
                String key = "key-" + Math.floorMod(emitted + runtimeInfo.subtaskIndex(), settings.keyCount());
                int value = syntheticValue(emitted);

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(new TestEvent(
                            key,
                            value,
                            now,
                            runtimeInfo.subtaskIndex(),
                            runtimeInfo.parallelism(),
                            emitted));
                }

                emitted++;
                Thread.sleep(sleepMillis);
            }
        }

        private static int syntheticValue(long sequence) {
            return switch ((int) Math.floorMod(sequence, 4)) {
                case 0 -> 1;
                case 1 -> 3;
                case 2 -> 7;
                default -> 10;
            };
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public record SourceRuntimeInfo(int subtaskIndex, int parallelism) implements Serializable {
    }

    public static final class EventAggregate implements AggregateFunction<TestEvent, SummaryAccumulator, WindowSummary> {
        @Override
        public SummaryAccumulator createAccumulator() {
            return new SummaryAccumulator();
        }

        @Override
        public SummaryAccumulator add(TestEvent event, SummaryAccumulator accumulator) {
            accumulator.key = event.key;
            accumulator.count++;
            accumulator.sum += event.value;
            accumulator.firstEventTime = accumulator.firstEventTime == 0
                    ? event.eventTimeMillis
                    : Math.min(accumulator.firstEventTime, event.eventTimeMillis);
            accumulator.lastEventTime = Math.max(accumulator.lastEventTime, event.eventTimeMillis);
            return accumulator;
        }

        @Override
        public WindowSummary getResult(SummaryAccumulator accumulator) {
            return new WindowSummary(
                    accumulator.key,
                    accumulator.count,
                    accumulator.sum,
                    accumulator.firstEventTime,
                    accumulator.lastEventTime);
        }

        @Override
        public SummaryAccumulator merge(SummaryAccumulator left, SummaryAccumulator right) {
            SummaryAccumulator merged = new SummaryAccumulator();
            merged.key = left.key != null ? left.key : right.key;
            merged.count = left.count + right.count;
            merged.sum = left.sum + right.sum;
            if (left.firstEventTime == 0) {
                merged.firstEventTime = right.firstEventTime;
            } else if (right.firstEventTime == 0) {
                merged.firstEventTime = left.firstEventTime;
            } else {
                merged.firstEventTime = Math.min(left.firstEventTime, right.firstEventTime);
            }
            merged.lastEventTime = Math.max(left.lastEventTime, right.lastEventTime);
            return merged;
        }
    }

    public static final class FormatSummary implements MapFunction<WindowSummary, String> {
        @Override
        public String map(WindowSummary summary) {
            var template = """
                    window-summary key=%s count=%d sum=%d first=%s last=%s""";
            return template.formatted(
                    summary.key(),
                    summary.count(),
                    summary.sum(),
                    FORMATTER.format(Instant.ofEpochMilli(summary.firstEventTime())),
                    FORMATTER.format(Instant.ofEpochMilli(summary.lastEventTime())));
        }
    }

    public static final class TestEvent implements Serializable {
        public String key;
        public int value;
        public long eventTimeMillis;
        public int sourceSubtask;
        public int sourceParallelism;
        public long sequence;

        public TestEvent() {
        }

        public TestEvent(String key, int value, long eventTimeMillis, int sourceSubtask, int sourceParallelism, long sequence) {
            this.key = key;
            this.value = value;
            this.eventTimeMillis = eventTimeMillis;
            this.sourceSubtask = sourceSubtask;
            this.sourceParallelism = sourceParallelism;
            this.sequence = sequence;
        }
    }

    public static final class SummaryAccumulator implements Serializable {
        public String key;
        public long count;
        public long sum;
        public long firstEventTime;
        public long lastEventTime;
    }

    public record WindowSummary(
            String key,
            long count,
            long sum,
            long firstEventTime,
            long lastEventTime) implements Serializable {
    }
}
