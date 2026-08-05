"""Bronze layer: land validated flight events exactly as received.

Reads everything currently sitting in `flight-events-validated`, appends it
to the Bronze Delta table untouched (beyond parsing timestamps/dates and
stamping an ingestion time). No cleaning, no deduplication, no aggregation -
Bronze exists to be the replay source if a downstream transform is wrong.
"""

import json
import os
from datetime import date, datetime, timezone

from kafka import KafkaConsumer

from lakehouse_schema import BRONZE_SCHEMA
from spark_session import BRONZE_PATH, get_spark

VALIDATED_TOPIC = "flight-events-validated"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _consume_validated(poll_timeout_ms: int = 5000, max_empty_polls: int = 3) -> list[dict]:
    """Polls explicitly rather than relying on the message-iterator's idle
    timeout, since consumer-group join/rebalance overhead on a cold start
    can itself exceed a short idle timeout and cause records to be missed.
    """
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="bronze-loader",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    consumer.subscribe([VALIDATED_TOPIC])

    records = []
    ingested_at = datetime.now(timezone.utc)
    empty_polls = 0
    while empty_polls < max_empty_polls:
        batches = consumer.poll(timeout_ms=poll_timeout_ms)
        if not batches:
            empty_polls += 1
            continue
        empty_polls = 0
        for tp_messages in batches.values():
            for message in tp_messages:
                record = dict(message.value)
                record["scheduled_date"] = date.fromisoformat(record["scheduled_date"])
                record["event_ts"] = _parse_ts(record["event_ts"])
                record["ingested_at"] = ingested_at
                records.append(record)

    consumer.close()
    return records


def run() -> None:
    records = _consume_validated()
    if not records:
        print("No new validated records to load into Bronze.")
        return

    spark = get_spark("bronze-loader")
    df = spark.createDataFrame(records, schema=BRONZE_SCHEMA)

    (
        df.write.format("delta")
        .mode("append")
        .save(BRONZE_PATH)
    )

    print(f"Bronze: appended {df.count()} records to {BRONZE_PATH}")
    spark.stop()


if __name__ == "__main__":
    run()
