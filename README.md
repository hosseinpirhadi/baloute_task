# Flink + Kafka: Top 10 Unique Users per Hour

## Goal

Read events from Kafka. Each event contains at least:

- `event_id`
- `user_id`
- `timestamp`

Remove duplicates by `event_id`, then, for every one-hour event-time window, report the 10 users with the highest number of unique events.

## Architecture

Kafka -> Flink Kafka Source -> event-time watermarks -> deduplication by `event_id`
-> 1-hour tumbling event-time window -> count per user -> Top 10 -> stdout

Flink Web UI: http://localhost:8081

## Deliberate assumptions

1. **Source**: Kafka is used rather than a file because the task is a streaming-processing exercise.
2. **Timestamp**: `timestamp` is the event timestamp and is used as Event Time.
3. **Window**: a fixed 1-hour Tumbling Event-Time Window is used. Windows do not overlap.
4. **Out-of-order events**: watermarks allow 5 minutes of out-of-orderness.
5. **Late events**: events arriving later than the watermark are not deliberately reprocessed in this implementation. The README explicitly chooses bounded lateness to keep latency predictable.
6. **Deduplication horizon**: `event_id` state has a 24-hour TTL. Therefore, duplicates arriving within 24 hours are removed. An event repeated after the TTL is not guaranteed to be recognized as a duplicate. This is a deliberate memory/correctness trade-off.
7. **Tie-breaking**: if two users have the same count, `user_id` is used lexicographically as a deterministic tie-breaker.
8. **Output semantics**: the downstream Top-10 operator collects all per-user results for a window and emits one final Top-10 when the event-time watermark reaches the window end.
9. **Restart**: Flink checkpointing is enabled every 10 seconds with EXACTLY_ONCE mode. Kafka offsets and Flink operator state are recovered from checkpoints.
10. **Parallelism**: the job uses parallelism 3. Deduplication is keyed by `event_id`, so the same event ID is routed to the same keyed state partition.

## Important trade-offs

### Latency

The 5-minute watermark delay means a window is normally finalized only after Flink believes it has seen events up to the end of that window plus the allowed out-of-orderness. This increases latency but gives late/out-of-order events a chance to be included.

### Counting accuracy

The count is based on unique `event_id`s. The 24-hour TTL means the guarantee is bounded: duplicates older than 24 hours can be counted again.

Using Event Time instead of Processing Time means events are assigned to windows according to their event timestamps, which is usually more correct for business events.

### Memory

Deduplication requires state keyed by `event_id`. Without a TTL, that state can grow indefinitely. The 24-hour TTL bounds its lifetime.

The Top-10 stage keeps per-user counts for each active window. For very high cardinality, this can become large. A production implementation could use a more scalable aggregation design and/or RocksDB-backed state.

### Restart

Checkpoints persist operator state and Kafka source progress. After a failure, Flink can restore state and continue from the checkpoint rather than starting from scratch.

The checkpoint interval is 10 seconds, so a failure can cause roughly up to the most recent checkpoint interval of processing progress to be replayed, subject to source/connector and checkpoint completion behavior. Stateful deduplication prevents replayed events from being counted twice after state recovery.

## Build and run

Requirements:

- Docker
- Docker Compose
- Internet access for the first image/Maven build

Build the Java job:

```bash
mvn clean package
```

Start Kafka and Flink:

```bash
docker compose up -d --build
```

Open:

- Kafka: `localhost:9092`
- Flink UI: `http://localhost:8081`

## Produce test events

After the containers are running:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh   --bootstrap-server kafka:9092   --topic events
```

Paste JSON events such as:

```json
{"event_id":"1","user_id":"alice","timestamp":"2026-10-08T10:05:00+00:00"}
{"event_id":"2","user_id":"bob","timestamp":"2026-10-08T10:10:00+00:00"}
{"event_id":"3","user_id":"alice","timestamp":"2026-10-08T10:20:00+00:00"}
{"event_id":"1","user_id":"alice","timestamp":"2026-10-08T10:05:00+00:00"}
```

The last event is a duplicate and should not increase Alice's count.

To see job output:

```bash
docker logs -f flink-taskmanager
```

## Stop

```bash
docker compose down
```

To also remove checkpoint/savepoint volumes:

```bash
docker compose down -v
```

## Notes

This implementation intentionally favors clarity for an assignment. The Top-10 stage is keyed by window start and keeps user counts for the window. For a production workload with extremely high user cardinality, the aggregation should be redesigned to avoid concentrating too much state/work in one downstream key.
