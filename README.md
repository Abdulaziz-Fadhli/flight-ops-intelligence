# Flight Ops Intelligence

Real-time flight operations data pipeline with a retrieval-augmented Q&A layer, built as the capstone project for the **Modern Data Engineering for AI Systems** program at SDAIA Academy.

## What this project does

Flight status events (gate changes, delays, boarding, departures, cancellations) stream in from operational feeds, get validated and quarantined if malformed, land in a Delta Lake bronze/silver/gold lakehouse, and get aggregated into daily operational metrics (on-time performance, delay-by-cause). A separate RAG pipeline answers operational questions (e.g. rebooking and delay-compensation policy) grounded in airline policy documents, optionally combined with a flight's live status pulled from the lakehouse.

## Pipeline overview

1. **Ingestion** — Kafka producer/consumer with a Pydantic schema contract at the ingestion boundary. Malformed records are routed to a dead-letter topic with the rejection reason recorded.
2. **Delta Lakehouse** — Bronze (raw), Silver (`MERGE`-upserted current flight state keyed on `flight_id`), Gold (daily on-time/delay aggregates).
3. **RAG Pipeline** — Document chunking, embeddings, a vector store, hybrid search (dense + BM25 via Reciprocal Rank Fusion), and cross-encoder reranking. Answers are grounded in retrieved context with citations.
4. **Orchestration** — An Airflow DAG wires every stage together; a failed quality gate halts the pipeline before downstream stages run.
5. **Quality Gate + Lineage** — Great Expectations checks gate the pipeline, and OpenLineage START/COMPLETE/FAIL events are emitted per stage.

## Status

- [x] **Ingestion** — Kafka (KRaft mode, no Zookeeper) + Pydantic contract + dead-letter routing. Verified end-to-end.
- [x] **Delta Lakehouse** — Bronze/Silver/Gold with a real `MERGE` upsert and schema enforcement. Verified end-to-end.
- [ ] RAG Pipeline
- [ ] Orchestration
- [ ] Quality Gate + Lineage

## Running the ingestion stage (Day 1)

Prerequisites: Docker Desktop, Python 3.11+.

```bash
# 1. Start Kafka (KRaft mode, single broker)
cd docker
docker compose up -d

# 2. Install dependencies
pip install -r requirements.txt

# 3. Produce simulated flight events (some deliberately malformed)
cd ../src/ingestion
python producer.py

# 4. Validate against the FlightEvent contract, routing to
#    flight-events-validated or flight-events-dlq
python validator_consumer.py
```

**Expected output** (see [docs/day1_validator_run.txt](docs/day1_validator_run.txt) for a captured run):

```
Validation run complete: 200 consumed, 170 accepted -> 'flight-events-validated', 30 rejected -> 'flight-events-dlq'.
```

Each dead-lettered record carries the original payload plus the specific rejection reason (missing field, invalid enum, negative delay, missing gate on boarding, bad timestamp, missing delay reason) — proving the failure path, not just the happy path.

## Running the lakehouse stage (Day 2)

Runs inside a Linux container (`docker/Dockerfile.spark`) rather than natively on Windows, to avoid the native Hadoop/`winutils.exe` dependency Spark needs on Windows.

```bash
# 1. Build the Spark runner image (once)
cd docker
docker compose build spark-runner

# 2. Load newly-validated Kafka records into Bronze (raw, as received)
docker compose run --rm spark-runner python3 kafka_to_bronze.py

# 3. MERGE Bronze's latest-per-flight state into Silver, keyed on flight_id
docker compose run --rm spark-runner python3 silver_merge.py

# 4. Recompute the Gold daily on-time/delay aggregate from Silver
docker compose run --rm spark-runner python3 gold_aggregate.py

# 5. Prove Delta actually refuses a malformed write (wrong type + missing
#    required column) rather than silently coercing it
docker compose run --rm spark-runner python3 demo_schema_enforcement.py
```

**Expected output** (see [docs/day2_lakehouse_run.txt](docs/day2_lakehouse_run.txt) for a full captured run, including the incremental MERGE going from 170 → 295 flights across two batches):

```
Silver: MERGE complete, 295 flights tracked at /data/lakehouse/silver/flight_state
...
Gold: wrote daily ops summary for 3 airline/date groups to /data/lakehouse/gold/daily_ops_summary
+------------+--------------+-------------+-------------+---------------+------------------+-----------+
|airline_code|scheduled_date|total_flights|delayed_count|cancelled_count|avg_delay_minutes |on_time_pct|
+------------+--------------+-------------+-------------+---------------+------------------+-----------+
...
REJECTED as expected. Delta raised: AnalysisException
Reason: [DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields 'delay_minutes' and 'delay_minutes'
```

Silver's `MERGE` only updates a matched `flight_id` when the incoming event is actually newer (`event_ts` comparison) — a blind overwrite would let a late-arriving stale event clobber a newer one, which is exactly the kind of bug multi-feed ingestion produces in practice. Gold is a genuine aggregate (counts/rates/averages grouped by airline + date), computed fresh from Silver each run, not a copy of it.

## Training program attribution

Completed as the capstone project for **Modern Data Engineering for AI Systems**, SDAIA Academy (delivered via Learning Space).

See also: [SDAIA Academy on GitHub](https://github.com/SDAIAAcademy).
