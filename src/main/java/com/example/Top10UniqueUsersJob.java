package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class Top10UniqueUsersJob {

    static final ObjectMapper MAPPER = new ObjectMapper();
    // F7: Dead-letter queue side output tag for invalid records
    static final OutputTag<String> DLQ_TAG = new OutputTag<String>("dlq"){};

    public record Event(String eventId, String userId, long timestamp) {}
    public record UserWindowCount(long windowStart, long windowEnd, String userId, long count) {}

    /**
     * F7: Process raw Kafka strings and parse to JSON. Invalid records are sent to a side output
     * rather than crashing the job.
     */
    public static class ParseEventFunction extends ProcessFunction<String, Event> {
        @Override
        public void processElement(String value, Context ctx, Collector<Event> out) {
            try {
                JsonNode n = MAPPER.readTree(value);
                String eventId = n.get("event_id").asText();
                String userId = n.get("user_id").asText();
                String ts = n.get("timestamp").asText();

                long epochMillis = OffsetDateTime.parse(ts).toInstant().toEpochMilli();
                out.collect(new Event(eventId, userId, epochMillis));
            } catch (Exception e) {
                // Route to Dead Letter Queue (DLQ) side output
                ctx.output(DLQ_TAG, value);
            }
        }
    }

    /**
     * Deduplicate events by event_id. State is retained for 24 hours.
     */
    public static class Deduplicate extends KeyedProcessFunction<String, Event, Event> {
        private transient ValueState<Boolean> seen;

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
            ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen", Boolean.class);

            StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofHours(24))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            descriptor.enableTimeToLive(ttl);
            seen = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {
            if (seen.value() == null) {
                seen.update(true);
                out.collect(event);
            }
        }
    }

    /**
     * F5: Incremental aggregation to avoid storing all events in memory during the window.
     */
    public static class CountAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() { return 0L; }

        @Override
        public Long add(Event value, Long accumulator) { return accumulator + 1L; }

        @Override
        public Long getResult(Long accumulator) { return accumulator; }

        @Override
        public Long merge(Long a, Long b) { return a + b; }
    }

    /**
     * F5: Receives the final count from the CountAgg and wraps it in a UserWindowCount.
     */
    public static class WindowResult extends ProcessWindowFunction<Long, UserWindowCount, String, TimeWindow> {
        @Override
        public void process(String userId, Context context, Iterable<Long> elements, Collector<UserWindowCount> out) {
            Long count = elements.iterator().next();
            out.collect(new UserWindowCount(context.window().getStart(), context.window().getEnd(), userId, count));
        }
    }

    /**
     * Collects per-user window counts and emits the Top 10 users.
     */
    public static class Top10Process extends KeyedProcessFunction<Long, UserWindowCount, String> {

        private transient MapState<String, Long> counts;

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
            MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>("userCounts", String.class, Long.class);
            counts = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(UserWindowCount x, Context ctx, Collector<String> out) throws Exception {
            counts.put(x.userId(), x.count());

            // Register a timer at the end of the window.
            ctx.timerService().registerEventTimeTimer(x.windowEnd());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            
            // F6: Use a Min-Heap of size 10 to optimize the Top-10 computation.
            // The worst element is kept at the root (smallest count, or lexicographically larger key).
            Comparator<Map.Entry<String, Long>> minHeapComparator =
                    Comparator.<Map.Entry<String, Long>>comparingLong(Map.Entry::getValue)
                              .thenComparing(Map.Entry::getKey, Comparator.reverseOrder());
            
            PriorityQueue<Map.Entry<String, Long>> heap = new PriorityQueue<>(minHeapComparator);

            for (Map.Entry<String, Long> entry : counts.entries()) {
                heap.offer(entry);
                if (heap.size() > 10) {
                    heap.poll();
                }
            }

            // Drain heap to a list and reverse it so the highest count is rank 1
            List<Map.Entry<String, Long>> top = new ArrayList<>();
            while (!heap.isEmpty()) {
                top.add(heap.poll());
            }
            Collections.reverse(top);

            StringBuilder sb = new StringBuilder();
            sb.append("{\"window_start\":\"").append(Instant.ofEpochMilli(ctx.getCurrentKey()))
              .append("\",\"window_end\":\"").append(Instant.ofEpochMilli(timestamp))
              .append("\",\"top10\":[");

            for (int i = 0; i < top.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append("{\"rank\":").append(i + 1)
                  .append(",\"user_id\":\"").append(escape(top.get(i).getKey()))
                  .append("\",\"unique_count\":").append(top.get(i).getValue())
                  .append('}');
            }
            sb.append("]}");

            // F4: Note: print() provides at-least-once. For exact-once end-to-end,
            // an idempotent sink (like DB upsert keyed by windowStart) is needed.
            out.collect(sb.toString());

            counts.clear();
        }

        private static String escape(String s) {
            return s.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);

        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "events");

        // F1 & F2: Environment variables for window size and latency with defaults matching the problem statement
        long windowSizeMinutes = Long.parseLong(System.getenv().getOrDefault("WINDOW_SIZE_MINUTES", "60"));
        long maxOutOfOrdernessSeconds = Long.parseLong(System.getenv().getOrDefault("MAX_OUT_OF_ORDERNESS_SECONDS", "300"));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId("flink-top10-unique-users")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        // Step 1: Read raw strings and parse to JSON, routing failures to DLQ
        SingleOutputStreamOperator<Event> parsedEvents = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .process(new ParseEventFunction())
                .name("Parse JSON & DLQ");

        // F7: Output the invalid records to standard out (or send to another Kafka topic in prod)
        parsedEvents.getSideOutput(DLQ_TAG)
                .print()
                .name("Dead Letter Queue");

        // F3: Add withIdleness so watermarks don't stall if Kafka partitions go silent
        WatermarkStrategy<Event> wm = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(maxOutOfOrdernessSeconds))
                .withIdleness(Duration.ofMinutes(1))
                .withTimestampAssigner((event, recordTimestamp) -> event.timestamp());

        DataStream<Event> eventsWithWatermarks = parsedEvents.assignTimestampsAndWatermarks(wm);

        // Step 2: Deduplicate globally by event_id
        DataStream<Event> unique = eventsWithWatermarks
                .keyBy((KeySelector<Event, String>) Event::eventId)
                .process(new Deduplicate())
                .name("Deduplicate by event_id");

        // Step 3: F5 incremental aggregation
        SingleOutputStreamOperator<UserWindowCount> perUser = unique
                .keyBy((KeySelector<Event, String>) Event::userId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(windowSizeMinutes)))
                .aggregate(new CountAgg(), new WindowResult())
                .name("Incremental Count per User");

        // Step 4: Top 10 using F6 Min-Heap and F4 idempotent sink semantics
        perUser
                .keyBy((KeySelector<UserWindowCount, Long>) UserWindowCount::windowStart)
                .process(new Top10Process())
                .name("Top 10 Users Heap")
                .print()
                .name("Stdout Sink (At-Least-Once)");

        env.execute("Top 10 Unique Users per Hour");
    }
}