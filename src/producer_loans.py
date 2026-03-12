import json
import time
import uuid
import random
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_LOANS
from logger import get_logger

fake = Faker()
log = get_logger("producer_loans")

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
        "source_system": "loan_system",
        "loan_id": random.randint(1, 500000),
        "customer_id": random.randint(1, 500000),
        "loan_type": random.choice(["HOME", "AUTO", "PERSONAL"]),
        "loan_amount": round(random.uniform(5000, 300000), 2),
        "interest_rate": round(random.uniform(2.5, 9.5), 2),
        "loan_status": random.choice(["ACTIVE", "PAID", "DEFAULT"])
    }

def main(total=1000000, per_second=5000):

    log.info(f"Producing {total} events to {TOPIC_LOANS}")

    sent = 0

    while sent < total:

        batch = min(per_second, total - sent)

        for _ in range(batch):
            producer.send(TOPIC_LOANS, make_event())

        producer.flush()

        sent += batch

        log.info(f"Sent {sent}/{total}")

        time.sleep(1)

    producer.close()
    log.info("Done producing.")

if __name__ == "__main__":
    main()