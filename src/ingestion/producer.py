"""Simulates two disagreeing upstream flight-data feeds publishing onto the
same raw Kafka topic: an internal ops system and a public tracker-style feed.

Real airport ops pipelines ingest from multiple providers that don't always
agree on formatting or arrive complete - that disagreement is simulated here
by deliberately emitting a fraction of malformed records, not as a rubric
exercise but because it's the realistic failure mode this pipeline exists
to handle.
"""

import json
import random
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

RAW_TOPIC = "flight-events-raw"
BOOTSTRAP_SERVERS = "localhost:29092"

AIRLINES = ["SV", "XY", "F3"]
AIRPORTS = ["RUH", "JED", "DMM", "MED", "AHB"]
STATUSES = ["SCHEDULED", "BOARDING", "GATE_CHANGE", "DEPARTED", "DELAYED", "ARRIVED", "CANCELLED"]
DELAY_REASONS = ["WEATHER", "CREW", "TECHNICAL", "ATC", "LATE_INBOUND"]


def _good_event(flight_seq: int) -> dict:
    airline = random.choice(AIRLINES)
    status = random.choice(STATUSES)
    event = {
        "flight_id": f"{airline}{1000 + flight_seq}-{datetime.now(timezone.utc):%Y%m%d}",
        "airline_code": airline,
        "flight_number": str(1000 + flight_seq),
        "scheduled_date": datetime.now(timezone.utc).date().isoformat(),
        "origin": random.choice(AIRPORTS),
        "destination": random.choice(AIRPORTS),
        "status": status,
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source_feed": random.choice(["ops_system", "public_tracker"]),
    }
    if status in ("BOARDING", "GATE_CHANGE"):
        event["gate"] = f"{random.randint(1, 40)}"
    if status == "DELAYED":
        event["delay_minutes"] = random.randint(5, 240)
        event["delay_reason_code"] = random.choice(DELAY_REASONS)
    return event


def _malformed_event(flight_seq: int) -> dict:
    """Simulates the realistic ways the two upstream feeds disagree or drop fields."""
    event = _good_event(flight_seq)
    fault = random.choice(
        ["missing_flight_id", "bad_status", "negative_delay", "missing_gate", "bad_timestamp"]
    )
    if fault == "missing_flight_id":
        event.pop("flight_id", None)
    elif fault == "bad_status":
        event["status"] = "ON_APPROACH"  # not in the enum contract
    elif fault == "negative_delay":
        event["status"] = "DELAYED"
        event["delay_minutes"] = -15
        event["delay_reason_code"] = random.choice(DELAY_REASONS)
    elif fault == "missing_gate":
        event["status"] = "BOARDING"
        event.pop("gate", None)
    elif fault == "bad_timestamp":
        event["event_ts"] = "not-a-timestamp"
    return event


def run(num_events: int = 200, malformed_rate: float = 0.15, delay_seconds: float = 0.05) -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    sent_good, sent_bad = 0, 0
    for i in range(num_events):
        if random.random() < malformed_rate:
            payload = _malformed_event(i)
            sent_bad += 1
        else:
            payload = _good_event(i)
            sent_good += 1

        producer.send(RAW_TOPIC, value=payload)
        time.sleep(delay_seconds)

    producer.flush()
    producer.close()
    print(f"Produced {num_events} events to '{RAW_TOPIC}': {sent_good} well-formed, {sent_bad} deliberately malformed.")


if __name__ == "__main__":
    run()
