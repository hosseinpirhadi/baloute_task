import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "events"

USERS = [
    "ali",
    "hossein",
    "hassan",
    "reza",
    "alice",
    "bob",
    "charlie",
    "david",
    "emma",
    "frank",
    "grace",
    "henry",
    "isabella",
    "jack",
]


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def create_event():
    now = datetime.now(timezone.utc)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(USERS),
        "timestamp": now.isoformat().replace("+00:00", "Z"),
    }


print("Kafka producer started", flush=True)
print(f"Bootstrap server: {KAFKA_BOOTSTRAP_SERVERS}", flush=True)
print(f"Topic: {KAFKA_TOPIC}", flush=True)

try:
    while True:
        event = create_event()

        producer.send(
            KAFKA_TOPIC,
            value=event
        )

        producer.flush()

        print(
            json.dumps(event),
            flush=True
        )

        time.sleep(2)

except KeyboardInterrupt:
    print("Stopping producer...", flush=True)

finally:
    producer.close()