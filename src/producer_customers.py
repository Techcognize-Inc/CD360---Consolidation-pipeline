import json, time, uuid, random
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer
from config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_CUSTOMERS
from logger import get_logger

fake = Faker()
log = get_logger("producer_customers")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=20,
    batch_size=64 * 1024,
    acks=1
)

def make_event():
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "op": random.choice(["I", "U"]),
        "source_system": "crm",
        "customer_id": random.randint(1, 500000),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "city": fake.city(),
        "state": fake.state_abbr(),
        "zip": fake.zipcode(),
        "status": random.choice(["ACTIVE", "INACTIVE"])
    }

def main(total=1000000, per_second=5000):
    log.info(f"Producing {total} events to {TOPIC_CUSTOMERS}")
    sent = 0
    while sent < total:
        batch = min(per_second, total - sent)
        for _ in range(batch):
            producer.send(TOPIC_CUSTOMERS, make_event())
        producer.flush()
        sent += batch
        log.info(f"Sent {sent}/{total}")
        time.sleep(1)

if __name__ == "__main__":
    main()