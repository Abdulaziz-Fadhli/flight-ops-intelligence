"""Ingestion-boundary validator.

Consumes raw flight events, enforces the FlightEvent Pydantic contract, and
routes each record to exactly one place:

  - flight-events-validated  - passed the contract, safe for downstream use
  - flight-events-dlq        - failed validation, original payload + the
                                specific rejection reason are preserved so
                                the failure is debuggable, not just discarded

Polls explicitly (rather than relying on the message-iterator's idle
timeout) and stops after a few consecutive empty polls, since consumer-group
join/rebalance overhead on a cold start can itself exceed a short idle
timeout and cause records to be missed.
"""

import json
import os
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from pydantic import ValidationError

from schema import FlightEvent

RAW_TOPIC = "flight-events-raw"
VALIDATED_TOPIC = "flight-events-validated"
DLQ_TOPIC = "flight-events-dlq"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


def _handle_message(raw_payload: dict, producer: KafkaProducer) -> bool:
    """Returns True if the record was accepted, False if rejected."""
    try:
        event = FlightEvent.model_validate(raw_payload)
        producer.send(VALIDATED_TOPIC, value=event.model_dump(mode="json"))
        return True
    except ValidationError as exc:
        reasons = [
            {"field": ".".join(str(p) for p in e["loc"]), "msg": e["msg"], "type": e["type"]}
            for e in exc.errors()
        ]
        dlq_record = {
            "original_payload": raw_payload,
            "rejection_reason": reasons,
            "rejected_at": datetime.now(timezone.utc).isoformat(),
        }
        producer.send(DLQ_TOPIC, value=dlq_record)
        print(f"REJECTED flight_id={raw_payload.get('flight_id', '<missing>')}: "
              f"{[r['msg'] for r in reasons]}")
        return False


def run(poll_timeout_ms: int = 5000, max_empty_polls: int = 3) -> None:
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="flight-events-validator",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    consumer.subscribe([RAW_TOPIC])
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    accepted, rejected = 0, 0
    empty_polls = 0
    while empty_polls < max_empty_polls:
        batches = consumer.poll(timeout_ms=poll_timeout_ms)
        if not batches:
            empty_polls += 1
            continue
        empty_polls = 0
        for tp_messages in batches.values():
            for message in tp_messages:
                if _handle_message(message.value, producer):
                    accepted += 1
                else:
                    rejected += 1

    producer.flush()
    producer.close()
    consumer.close()

    total = accepted + rejected
    print(f"\nValidation run complete: {total} consumed, {accepted} accepted -> "
          f"'{VALIDATED_TOPIC}', {rejected} rejected -> '{DLQ_TOPIC}'.")


if __name__ == "__main__":
    run()
