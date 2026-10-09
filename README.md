# Flink + Kafka: Top 10 Unique Users per Hour

## Goal

Read events from Kafka. Each event contains at least:

- `event_id`
- `user_id`
- `timestamp`

Remove duplicates by `event_id`, then, for every one-hour event-time window, report the 10 users with the highest number of unique events.

## Architecture

Python Producer (Container) -> Kafka -> Flink Kafka Source -> event-time watermarks -> deduplication by `event_id`
-> 1-hour tumbling event-time window -> count per user -> Top 10 -> stdout

Flink Web UI: http://localhost:8081

## Deliberate Assumptions & Trade-offs

The problem statement intentionally left several requirements ambiguous to allow for architectural decision-making. Below is a breakdown of the assumptions made, the possible alternatives, and the rationale behind our choices, specifically analyzing their impact on **Data Latency**, **Counting Accuracy**, **Memory**, and **Restart Behavior**:

### 1. Data Source (File vs. Kafka)
* **Task Ambiguity**: The task says to "read event records from a file or Kafka".
* **Our Choice**: **Apache Kafka**.
* **Rationale**: This is fundamentally a Stream Processing problem. Kafka allows us to simulate a continuous, real-world stream of events, demonstrating how to handle network delays, out-of-order data, and distributed offset commits.

### 2. Time Semantics (Timestamp)
* **Task Ambiguity**: Each JSON record has a `timestamp`, but it is not specified whether windows should be evaluated based on the server's clock or the event's embedded timestamp.
* **Our Choice**: **Event Time**.
* **Impact on Accuracy & Latency**: Event Time significantly increases **Counting Accuracy**. If the network goes down and events arrive hours late, they will still be grouped into their correct historical 1-hour window. However, this increases **Data Latency**, as Flink must wait for Watermarks to ensure no older data is still on the way before closing the window.

### 3. Windowing Strategy
* **Task Ambiguity**: "For each 1-hour window" could mean fixed non-overlapping windows or sliding windows.
* **Our Choice**: **Tumbling Event-Time Windows** (configurable via the `WINDOW_SIZE_MINUTES` environment variable).

### 4. Out-of-Orderness & Lateness
* **Task Ambiguity**: In the real world, data arrives out of order. How long should the job wait for delayed events?
* **Our Choice**: A **5-minute Watermark Delay** threshold (configurable via environment variables).
* **Impact on Latency**: The output for any 1-hour window is deliberately delayed and emitted **5 minutes after the window closes**. This latency is traded for **Accuracy** so that slightly delayed network events are still counted. Any events arriving later than this 5-minute threshold (Late Events) are entirely dropped.

### 5. Deduplication Scope & Memory Management
* **Task Ambiguity**: "Remove duplicate records based on `event_id`." Should this deduplication happen only within the current 1-hour window, or globally across the lifetime of the application?
* **Our Choice**: We maintain the `event_id`s in Flink's State (`ValueState`) with a **24-hour TTL (Time-To-Live)**.
* **Impact on Memory**: If state were kept forever, the server's memory (RAM/RocksDB) would eventually explode (OOM) as millions of unique IDs pile up. The 24-hour TTL ensures **Memory remains bounded and stable**.
* **Impact on Accuracy**: Guarantees that if a duplicate event arrives 20 hours later, it will still be successfully filtered out and not counted twice.

### 6. Top 10 Processing & Tie-Breaking
* **Task Ambiguity**: How should we efficiently find the Top 10 users among millions? And if two users have the same count, who gets prioritized?
* **Our Choice**: 
  - We use an `AggregateFunction` to keep only a single rolling integer (count) per user during the window, rather than buffering raw events.
  - When the window closes, we process the counts using a **Min-Heap (PriorityQueue) bounded to a size of exactly 10**. 
  - **Tie-Breaking**: If users have identical event counts, we fall back to sorting by `user_id` lexicographically (alphabetically). This guarantees the Top 10 output is always **Deterministic** and reproducible.
* **Impact on Memory & CPU**: Drastically reduces memory consumption during the window and optimizes the CPU sorting overhead down to `O(N log 10)`.

### 7. Restart Behavior & Fault Tolerance
* **Task Ambiguity**: What happens if the server crashes? Will records be double-counted upon restart?
* **Our Choice**: Enabled **Checkpointing every 10 seconds** with the **`EXACTLY_ONCE`** mode using the RocksDB state backend.
* **Impact on Restart Behavior**: If a TaskManager crashes, Flink wakes up and restores exactly from the last 10-second checkpoint. The Kafka consumer offsets and the internal Flink state (deduplicated IDs and user counts) are rolled back together in sync. 
* **Impact on Accuracy**: Because the deduplication state is restored alongside the Kafka offsets, any Kafka messages that are re-read during recovery will simply hit the deduplication filter. This guarantees **Exactly-Once Semantics (no double counting)** across failure boundaries.

### Demo Configuration vs. Production Specification

The official problem statement specifies:
- **Window size**: 1-hour tumbling window
- **Watermark delay (out-of-orderness allowance)**: 5 minutes

For rapid local testing and demonstration, the code currently runs with shortened parameters via `docker-compose.yml` so window evaluations appear in real time:
- **Window size**: 1 minute
- **Watermark delay**: 5 seconds

To switch back to the official production settings (1-hour window, 5-minute watermark delay), you have two options in `docker-compose.yml` under the `submit` container environment:

**Option 1: Update the values explicitly**
```yaml
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_TOPIC: events
      WINDOW_SIZE_MINUTES: 60
      MAX_OUT_OF_ORDERNESS_SECONDS: 300
```

**Option 2: Delete them (Recommended)**
Since the Java code defaults to 60 minutes and 300 seconds automatically, you can simply delete or comment out those two lines:
```yaml
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_TOPIC: events
      # WINDOW_SIZE_MINUTES: 1
      # MAX_OUT_OF_ORDERNESS_SECONDS: 5
```

After modifying `docker-compose.yml`, run `docker compose up -d` to apply the changes.

## Build and run

Requirements:

- Docker
- Docker Compose
- Internet access for the first image/Maven build


Start the pipeline (Kafka, Flink, and the Python producer):

```bash
docker compose up -d --build
```

This starts:
- `kafka`: Kafka broker (`localhost:9092`)
- `kafka-init`: creates the `events` topic (3 partitions)
- `jobmanager` & `taskmanager`: Flink cluster (`http://localhost:8081`)
- `submit`: submits the compiled Flink job to the cluster
- `kafka-producer`: Python producer service that automatically publishes events

Open:

- Kafka: `localhost:9092`
- Flink UI: `http://localhost:8081`

## Event Producer

The project includes an automated Python producer container (`kafka-producer` under `producer/`) that starts automatically with `docker compose up`. It generates and publishes random JSON events to the `events` topic every 2 seconds.

To monitor the Python producer logs:

```bash
docker logs -f kafka-producer
```

To see the Flink job output (Top 10 users printed to stdout):

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
