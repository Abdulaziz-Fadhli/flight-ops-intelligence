"""Ingestion-boundary validator.

Consumes raw flight events, enforces the FlightEvent Pydantic contract, and
routes each record to exactly one place:

  - flight-events-validated  - passed the contract, safe for downstream use
  - flight-events-dlq        - failed validation, original payload + the
                                specific rejection reason are preserved so
                                the failure is debuggable, not just discarded

Runs until `idle_timeout_ms` passes with no new messages, so it terminates
cleanly for demo/evidence runs instead of blocking forever.
"""

import json
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from pydantic import ValidationError

from schema import FlightEvent

RAW_TOPIC = "flight-events-raw"
VALIDATED_TOPIC = "flight-events-validated"
DLQ_TOPIC = "flight-events-dlq"
BOOTSTRAP_SERVERS = "localhost:29092"


def run(idle_timeout_ms: int = 8000) -> None:
    consumer = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="flight-events-validator",
        consumer_timeout_ms=idle_timeout_ms,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    accepted, rejected = 0, 0

    for message in consumer:
        raw_payload = message.value
        try:
            event = FlightEvent.model_validate(raw_payload)
            producer.send(VALIDATED_TOPIC, value=event.model_dump(mode="json"))
            accepted += 1
        except ValidationError as exc:
            # exc.errors() can carry a non-serializable exception object in
            # each error's `ctx`, so we keep only the plain, serializable
            # fields for the dead-letter record.
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
            rejected += 1
            print(f"REJECTED flight_id={raw_payload.get('flight_id', '<missing>')}: "
                  f"{[r['msg'] for r in reasons]}")

    producer.flush()
    producer.close()
    consumer.close()

    total = accepted + rejected
    print(f"\nValidation run complete: {total} consumed, {accepted} accepted -> "
          f"'{VALIDATED_TOPIC}', {rejected} rejected -> '{DLQ_TOPIC}'.")


if __name__ == "__main__":
    run()
