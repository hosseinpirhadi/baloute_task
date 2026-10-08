package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class Top10UniqueUsersJob {

    static final ObjectMapper MAPPER = new ObjectMapper();

    public record Event(
            String eventId,
            String userId,
            long timestamp
    ) {}

    public record UserWindowCount(
            long windowStart,
            long windowEnd,
            String userId,
            long count
    ) {}

    /**
     * Deserializes Kafka JSON messages into Event objects.
     */
    public static class EventDeserializer
            implements KafkaRecordDeserializationSchema<Event> {

        @Override
        public void deserialize(
                ConsumerRecord<byte[], byte[]> record,
                Collector<Event> out) {

            try {
                JsonNode n = MAPPER.readTree(record.value());

                String eventId = n.get("event_id").asText();
                String userId = n.get("user_id").asText();
                String ts = n.get("timestamp").asText();

                long epochMillis =
                        OffsetDateTime.parse(ts)
                                .toInstant()
                                .toEpochMilli();

                out.collect(
                        new Event(
                                eventId,
                                userId,
                                epochMillis
                        )
                );

            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to deserialize Kafka record",
                        e
                );
            }
        }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
    }

    /**
     * Deduplicate events by event_id.
     *
     * State is retained for 24 hours.
     */
    public static class Deduplicate
            extends KeyedProcessFunction<String, Event, Event> {

        private transient ValueState<Boolean> seen;

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) {

            ValueStateDescriptor<Boolean> descriptor =
                    new ValueStateDescriptor<>(
                            "seen",
                            Boolean.class
                    );

            /*
             * Flink 2.x:
             *
             * StateTtlConfig now uses java.time.Duration
             * instead of Flink's old Time class.
             */
            StateTtlConfig ttl =
                    StateTtlConfig
                            .newBuilder(Duration.ofHours(24))
                            .setUpdateType(
                                    StateTtlConfig.UpdateType.OnCreateAndWrite
                            )
                            .setStateVisibility(
                                    StateTtlConfig.StateVisibility.NeverReturnExpired
                            )
                            .build();

            descriptor.enableTimeToLive(ttl);

            seen = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                Event event,
                Context ctx,
                Collector<Event> out)
                throws Exception {

            if (seen.value() == null) {
                seen.update(true);
                out.collect(event);
            }
        }
    }

    /**
     * Counts events for each user inside a one-hour event-time window.
     */
    public static class CountPerUserWindow
            extends ProcessWindowFunction<
                    Event,
                    UserWindowCount,
                    String,
                    TimeWindow> {

        @Override
        public void process(
                String userId,
                Context context,
                Iterable<Event> events,
                Collector<UserWindowCount> out) {

            long count = 0;

            for (Event ignored : events) {
                count++;
            }

            out.collect(
                    new UserWindowCount(
                            context.window().getStart(),
                            context.window().getEnd(),
                            userId,
                            count
                    )
            );
        }
    }

    /**
     * Collects per-user window counts and emits the Top 10 users.
     */
    public static class Top10Process
            extends KeyedProcessFunction<
                    Long,
                    UserWindowCount,
                    String> {

        private transient MapState<String, Long> counts;

        @Override
        public void open(
        org.apache.flink.api.common.functions.OpenContext openContext) {

            MapStateDescriptor<String, Long> descriptor =
                    new MapStateDescriptor<>(
                            "userCounts",
                            String.class,
                            Long.class
                    );

            counts = getRuntimeContext()
                    .getMapState(descriptor);
        }

        @Override
        public void processElement(
                UserWindowCount x,
                Context ctx,
                Collector<String> out)
                throws Exception {

            counts.put(
                    x.userId(),
                    x.count()
            );

            /*
             * Register a timer at the end of the window.
             *
             * Since all UserWindowCount records for the same
             * window are keyed by windowStart, the timer fires
             * after the watermark reaches windowEnd.
             */
            ctx.timerService()
                    .registerEventTimeTimer(x.windowEnd());
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<String> out)
                throws Exception {

            List<Map.Entry<String, Long>> top =
                    new ArrayList<>();

            for (Map.Entry<String, Long> entry : counts.entries()) {
                top.add(entry);
            }

            top.sort(
                    Comparator
                            .<Map.Entry<String, Long>>comparingLong(
                                    Map.Entry::getValue
                            )
                            .reversed()
                            .thenComparing(
                                    Map.Entry::getKey
                            )
            );

            if (top.size() > 10) {
                top = top.subList(0, 10);
            }

            StringBuilder sb = new StringBuilder();

            sb.append("{\"window_start\":\"")
                    .append(
                            Instant.ofEpochMilli(
                                    ctx.getCurrentKey()
                            )
                    )
                    .append("\",\"window_end\":\"")
                    .append(
                            Instant.ofEpochMilli(timestamp)
                    )
                    .append("\",\"top10\":[");

            for (int i = 0; i < top.size(); i++) {

                if (i > 0) {
                    sb.append(',');
                }

                sb.append("{\"rank\":")
                        .append(i + 1)
                        .append(",\"user_id\":\"")
                        .append(
                                escape(
                                        top.get(i).getKey()
                                )
                        )
                        .append("\",\"unique_count\":")
                        .append(
                                top.get(i).getValue()
                        )
                        .append('}');
            }

            sb.append("]}");

            out.collect(sb.toString());

            counts.clear();
        }

        private static String escape(String s) {

            return s
                    .replace("\\", "\\\\")
                    .replace("\"", "\\\"");
        }
    }

    public static void main(String[] args)
            throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment
                        .getExecutionEnvironment();

        /*
         * Checkpoint every 10 seconds.
         */
        env.enableCheckpointing(10_000);

        env.getCheckpointConfig()
                .setCheckpointTimeout(60_000);

        String bootstrap =
                System.getenv()
                        .getOrDefault(
                                "KAFKA_BOOTSTRAP_SERVERS",
                                "kafka:9092"
                        );

        String topic =
                System.getenv()
                        .getOrDefault(
                                "KAFKA_TOPIC",
                                "events"
                        );

        KafkaSource<Event> source =
                KafkaSource
                        .<Event>builder()
                        .setBootstrapServers(bootstrap)
                        .setTopics(topic)
                        .setGroupId(
                                "flink-top10-unique-users"
                        )
                        .setStartingOffsets(
                                OffsetsInitializer.earliest()
                        )
                        .setDeserializer(
                                new EventDeserializer()
                        )
                        .build();

        /*
         * Events can arrive up to 5 minutes late.
         */
        WatermarkStrategy<Event> wm =
                WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(
                                // Duration.ofMinutes(5)
                                Duration.ofSeconds(5)
                        )
                        .withTimestampAssigner(
                                (event, recordTimestamp) ->
                                        event.timestamp()
                        );

        DataStream<Event> events =
                env.fromSource(
                        source,
                        wm,
                        "Kafka Events"
                );

        /*
         * Deduplicate by event_id.
         */
        DataStream<Event> unique =
                events
                        .keyBy(
                                (KeySelector<Event, String>)
                                        Event::eventId
                        )
                        .process(
                                new Deduplicate()
                        );

        /*
         * One-hour tumbling event-time windows.
         *
         * Flink 2.x uses java.time.Duration.
         */
        SingleOutputStreamOperator<UserWindowCount> perUser =
                unique
                        .keyBy(
                                (KeySelector<Event, String>)
                                        Event::userId
                        )
                        .window(
                                TumblingEventTimeWindows
                                        .of(
                                                // Duration.ofHours(1)
                                                Duration.ofMinutes(1)
                                        )
                        )
                        .process(
                                new CountPerUserWindow()
                        );

        /*
         * Group all users belonging to the same window.
         */
        perUser
                .keyBy(
                        (KeySelector<UserWindowCount, Long>)
                                UserWindowCount::windowStart
                )
                .process(
                        new Top10Process()
                )
                .name("Top 10 Users")
                .print();

        env.execute(
                "Top 10 Unique Users per Hour"
        );
    }
}