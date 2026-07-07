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
        ParameterTool params = ParameterTool.fromArgs(args);

        int eventsPerSecond = params.getInt("events-per-second", 20);
        int keyCount = params.getInt("keys", 4);
        int windowSeconds = params.getInt("window-seconds", 10);
        long maxEvents = params.getLong("max-events", -1L);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataStream<TestEvent> events = env
                .addSource(new SyntheticEventSource(eventsPerSecond, keyCount, maxEvents))
                .name("Synthetic Event Source")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        events
                .keyBy(event -> event.key)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(windowSeconds)))
                .aggregate(new EventAggregate())
                .map(new FormatSummary())
                .name("Format Window Summary")
                .print()
                .name("Print Window Summary");

        env.execute(String.format(
                Locale.ROOT,
                "Long Running Status Job - %d eps, %d keys, %ds windows",
                eventsPerSecond,
                keyCount,
                windowSeconds));
    }

    public static final class SyntheticEventSource extends RichParallelSourceFunction<TestEvent> {
        private final int eventsPerSecond;
        private final int keyCount;
        private final long maxEvents;
        private volatile boolean running = true;

        public SyntheticEventSource(int eventsPerSecond, int keyCount, long maxEvents) {
            this.eventsPerSecond = Math.max(1, eventsPerSecond);
            this.keyCount = Math.max(1, keyCount);
            this.maxEvents = maxEvents;
        }

        @Override
        public void run(SourceFunction.SourceContext<TestEvent> ctx) throws Exception {
            int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            int parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
            long emitted = 0L;
            long sleepMillis = Math.max(1L, 1000L / eventsPerSecond);

            while (running && (maxEvents < 0 || emitted < maxEvents)) {
                long now = System.currentTimeMillis();
                String key = "key-" + Math.floorMod(emitted + subtaskIndex, keyCount);
                int value = (int) ((emitted % 10) + 1);

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(new TestEvent(key, value, now, subtaskIndex, parallelism, emitted));
                }

                emitted++;
                Thread.sleep(sleepMillis);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
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
            return String.format(
                    Locale.ROOT,
                    "window-summary key=%s count=%d sum=%d first=%s last=%s",
                    summary.key,
                    summary.count,
                    summary.sum,
                    FORMATTER.format(Instant.ofEpochMilli(summary.firstEventTime)),
                    FORMATTER.format(Instant.ofEpochMilli(summary.lastEventTime)));
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

    public static final class WindowSummary implements Serializable {
        public String key;
        public long count;
        public long sum;
        public long firstEventTime;
        public long lastEventTime;

        public WindowSummary() {
        }

        public WindowSummary(String key, long count, long sum, long firstEventTime, long lastEventTime) {
            this.key = key;
            this.count = count;
            this.sum = sum;
            this.firstEventTime = firstEventTime;
            this.lastEventTime = lastEventTime;
        }
    }
}
