"""Windowed stream processing demo: tumbling window, sliding window, and
watermark-based late-event handling, applied to a real live Kafka stream,
sinking anomalies to a real Delta table as they're detected.

Standalone and additive - runs independently of the Airflow-orchestrated
batch pipeline (produce_and_validate -> Bronze -> Silver -> Gold), which
stays exactly as it is. A real streaming engine (Flink, Spark Structured
Streaming) runs as a long-lived continuous process, not a task in a DAG, so
this demo intentionally sits outside Airflow rather than forcing continuous
stream processing into a batch-task orchestrator.

Consumes directly from flight-events-raw (not the validated topic) with a
clean-only producer (malformed_rate=0) - this demo is about windowing
semantics, not re-proving the ingestion contract, which is already covered
end-to-end in src/ingestion/.
"""

import json
import os
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from kafka import KafkaConsumer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lakehouse"))

import producer as ingestion_producer  # src/ingestion/producer.py
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType
from spark_session import LAKEHOUSE_ROOT, get_spark  # src/lakehouse/spark_session.py

RAW_TOPIC = "flight-events-raw"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

TUMBLING_WINDOW_S = 1
SLIDING_WINDOW_S = 5
WATERMARK_LATENESS_S = 3
SEVERE_DELAY_THRESHOLD_MIN = 120
DEMO_DURATION_S = 12

ANOMALY_SINK_PATH = os.path.join(LAKEHOUSE_ROOT, "streaming", "severe_delay_anomalies")

ANOMALY_SCHEMA = StructType(
    [
        StructField("flight_id", StringType(), nullable=False),
        StructField("airline_code", StringType(), nullable=False),
        StructField("delay_minutes", IntegerType(), nullable=True),
        StructField("event_ts", TimestampType(), nullable=False),
        StructField("detected_at", TimestampType(), nullable=False),
    ]
)


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _process_event(
    event: dict,
    watermark: datetime,
    tumbling_buckets: dict,
    sliding_window: dict,
    anomalies: list,
) -> tuple[datetime, bool]:
    """Core windowing logic, factored out so it can be exercised directly
    with a synthetic late event (see the watermark proof at the end of
    run()) rather than only ever running against live, in-order Kafka
    traffic where a real drop may or may not happen to occur.
    """
    event_ts = _parse_ts(event["event_ts"])

    if event_ts < watermark - timedelta(seconds=WATERMARK_LATENESS_S):
        return watermark, True

    watermark = max(watermark, event_ts)
    bucket_key = event_ts.replace(microsecond=0).strftime("%H:%M:%S")
    tumbling_buckets[bucket_key] += 1

    if event["status"] == "DELAYED" and event.get("delay_minutes") is not None:
        airline = event["airline_code"]
        sliding_window[airline].append((event_ts, event["delay_minutes"]))
        cutoff = event_ts - timedelta(seconds=SLIDING_WINDOW_S)
        sliding_window[airline] = [(t, v) for t, v in sliding_window[airline] if t > cutoff]
        rolling_avg = sum(v for _, v in sliding_window[airline]) / len(sliding_window[airline])

        if event["delay_minutes"] > SEVERE_DELAY_THRESHOLD_MIN:
            anomalies.append(
                (
                    event["flight_id"],
                    airline,
                    event["delay_minutes"],
                    event_ts,
                    datetime.now(timezone.utc),
                )
            )
            print(f"  [ANOMALY] {event['flight_id']} delayed {event['delay_minutes']}min "
                  f"(rolling avg for {airline}: {rolling_avg:.1f}min)")

    return watermark, False


def run(duration_s: int = DEMO_DURATION_S) -> None:
    spark = get_spark("windowed-stream-demo")

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        group_id=f"windowed-demo-{int(time.time())}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    consumer.subscribe([RAW_TOPIC])
    # Force the initial group join/partition assignment to complete before
    # the producer starts - otherwise "latest" offset resolution can race
    # with the first few produced events and silently miss them.
    consumer.poll(timeout_ms=3000)

    print(f"=== Starting a brief live producer in the background ({duration_s}s window) ===")
    num_events = duration_s * 5
    producer_thread = threading.Thread(
        target=ingestion_producer.run,
        kwargs={
            "num_events": num_events,
            "malformed_rate": 0.0,
            "delay_seconds": duration_s / num_events,
        },
        daemon=True,
    )
    producer_thread.start()

    watermark = datetime.now(timezone.utc)
    tumbling_buckets: dict[str, int] = defaultdict(int)
    sliding_window: dict[str, list[tuple[datetime, int]]] = defaultdict(list)
    late_dropped = 0
    anomalies: list[tuple] = []

    print("=== Processing live events: tumbling + sliding windows, watermark ===")
    deadline = time.time() + duration_s
    while time.time() < deadline:
        batches = consumer.poll(timeout_ms=500)
        for tp_messages in batches.values():
            for message in tp_messages:
                event = message.value
                watermark, dropped = _process_event(
                    event, watermark, tumbling_buckets, sliding_window, anomalies
                )
                if dropped:
                    late_dropped += 1
                    print(f"  [WATERMARK] dropped late event {event['flight_id']}")

    consumer.close()
    producer_thread.join(timeout=5)

    print("\n=== Tumbling window: event counts per 1-second bucket ===")
    for bucket, count in sorted(tumbling_buckets.items()):
        print(f"  {bucket}: {count} events")

    print(f"\n=== Watermark: {late_dropped} late event(s) dropped during live processing ===")

    print("\n=== Watermark proof: feeding one synthetic event well behind the "
          f"current watermark (>{WATERMARK_LATENESS_S}s late) ===")
    late_event = {
        "flight_id": "SV9999-LATE-PROOF",
        "airline_code": "SV",
        "status": "SCHEDULED",
        "event_ts": (watermark - timedelta(seconds=WATERMARK_LATENESS_S + 5)).isoformat(),
    }
    _, was_dropped = _process_event(late_event, watermark, tumbling_buckets, sliding_window, anomalies)
    print(f"  Synthetic event {WATERMARK_LATENESS_S + 5}s behind the watermark -> "
          f"dropped: {was_dropped}")
    assert was_dropped, "watermark logic did not drop a deliberately late event"

    print(f"\n=== Delta ACID sink: writing {len(anomalies)} severe-delay anomalies ===")
    if anomalies:
        anomalies_df = spark.createDataFrame(anomalies, schema=ANOMALY_SCHEMA)
        anomalies_df.write.format("delta").mode("append").save(ANOMALY_SINK_PATH)
        spark.read.format("delta").load(ANOMALY_SINK_PATH).show(truncate=False)
    else:
        print(f"  No delays over {SEVERE_DELAY_THRESHOLD_MIN} min occurred in this window - nothing to sink.")

    spark.stop()


if __name__ == "__main__":
    run()
