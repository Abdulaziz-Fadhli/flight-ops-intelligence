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

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full data-flow diagram, repository layout, why each stage runs where it runs, and the environment variables involved.

## Status

- [x] **Ingestion** — Kafka (KRaft mode, no Zookeeper) + Pydantic contract + dead-letter routing. Verified end-to-end.
- [x] **Delta Lakehouse** — Bronze/Silver/Gold with a real `MERGE` upsert and schema enforcement. Verified end-to-end.
- [x] **RAG Pipeline** — Chunking, embeddings, hybrid search (dense + BM25, RRF fusion), cross-encoder reranking, grounded generation with citations. Verified end-to-end.
- [x] **Orchestration** — Airflow DAG wiring every stage together; a failed quality gate halts downstream tasks. Verified end-to-end.
- [x] **Quality Gate + Lineage** — Great Expectations gates on Bronze/Silver, OpenLineage START/COMPLETE/FAIL events per stage. Verified end-to-end.

## Running the ingestion stage

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

**Expected output** (see [docs/ingestion_validation_run.txt](docs/ingestion_validation_run.txt) for a captured run):

```
Validation run complete: 200 consumed, 170 accepted -> 'flight-events-validated', 30 rejected -> 'flight-events-dlq'.
```

Each dead-lettered record carries the original payload plus the specific rejection reason (missing field, invalid enum, negative delay, missing gate on boarding, bad timestamp, missing delay reason) — proving the failure path, not just the happy path.

## Running the lakehouse stage

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

**Expected output** (see [docs/lakehouse_run.txt](docs/lakehouse_run.txt) for a full captured run, including the incremental MERGE going from 170 → 295 flights across two batches):

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

### Transaction log, time travel, and OPTIMIZE + Z-ORDER

```bash
docker compose run --rm spark-runner python3 demo_time_travel_and_optimize.py
```

Every MERGE against Silver is a logged, queryable version — nothing is
overwritten in place. This script inspects that log, queries Silver as it
looked at an earlier version, and runs `OPTIMIZE ... ZORDER BY flight_id` to
physically co-locate files by the column every downstream query filters or
joins on.

**Expected output** (see [docs/time_travel_optimize_run.txt](docs/time_travel_optimize_run.txt) for a full captured run):

```
=== Transaction log: every write Silver has ever recorded ===
+-------+-----------------------+---------+
|version|timestamp              |operation|
+-------+-----------------------+---------+
|2      |2026-08-06 00:11:04.828|MERGE    |
|1      |2026-08-05 23:14:02.426|MERGE    |
|0      |2026-08-05 23:11:38.271|WRITE    |
+-------+-----------------------+---------+

=== Time travel: Silver as of version 0 vs. current (2) ===
Version 0 row count: 170
Version 2 (current) row count: 581

=== OPTIMIZE + Z-ORDER on flight_id ===
Files removed (small, pre-optimize): 1
Files added (compacted, Z-ordered): 1
```

The row counts genuinely differ between version 0 and the current version — this is real history being read back, not a staged number — and the `OPTIMIZE` itself shows up as its own logged version afterward.

### VACUUM and schema evolution

```bash
docker compose run --rm spark-runner python3 demo_vacuum_and_schema_evolution.py
```

Runs against a disposable scratch table seeded from Silver's data, not
Silver itself — `VACUUM` permanently deletes old file versions, which would
break the time-travel demo above if run against the same table it depends
on. Isolating this keeps every script safely re-runnable independently.

**Expected output** (see [docs/vacuum_and_schema_evolution_run.txt](docs/vacuum_and_schema_evolution_run.txt) for a full captured run):

```
=== VACUUM: retention cleanup ===
Parquet data files before VACUUM: 2
Parquet data files after VACUUM:  1

=== Schema evolution: adding a column via mergeSchema ===
Table after schema evolution (existing rows show NULL for the new column):
+---------------+---------------------------------+
|flight_id      |ops_notes                        |
+---------------+---------------------------------+
|F31000-20260805|Manually reviewed - no anomalies.|
|F31000-20260805|NULL                             |
+---------------+---------------------------------+
```

`VACUUM` genuinely removes an orphaned Parquet file left behind by a
superseded write (2 → 1) — the first version of this script actually
reported the count going *up* after `VACUUM`, because the counting logic
included `_delta_log` JSON commit files alongside data files; fixed to
count only `.parquet` files before trusting the number. Schema evolution
shows the real, expected behavior: the existing row gets `NULL` for the new
column rather than the write being rejected, once `mergeSchema=true` is set.

### Windowed stream processing (tumbling/sliding windows, watermark, live Delta sink)

```bash
docker compose run --rm --workdir /app/streaming spark-runner python3 windowed_processor.py
```

Standalone and additive — runs independently of the Airflow-orchestrated
batch pipeline above, which stays exactly as it is. A real streaming engine
(Flink, Spark Structured Streaming) runs as a long-lived continuous
process, not a task in a DAG, so this intentionally sits outside Airflow
rather than forcing continuous stream processing into a batch-task
orchestrator. Briefly runs a live producer in the background while a
consumer applies real windowing logic to the live Kafka stream: a tumbling
window counts events per second, a sliding window tracks each airline's
rolling average delay, and a watermark drops events that arrive too late
relative to the newest event seen. Severe delays (>120 min) are written to
a real Delta table as they're detected.

**Expected output** (see [docs/windowed_stream_processor_run.txt](docs/windowed_stream_processor_run.txt) for a full captured run):

```
=== Tumbling window: event counts per 1-second bucket ===
  13:38:03: 5 events
  13:38:04: 5 events
  ...

=== Watermark: 0 late event(s) dropped during live processing ===

=== Watermark proof: feeding one synthetic event well behind the current watermark (>3s late) ===
  Synthetic event 8s behind the watermark -> dropped: True

=== Delta ACID sink: writing 6 severe-delay anomalies ===
+---------------+------------+-------------+--------------------------+--------------------------+
|flight_id      |airline_code|delay_minutes|event_ts                  |detected_at               |
+---------------+------------+-------------+--------------------------+--------------------------+
...
```

Zero events happened to arrive late during any live run — real traffic
staying in order, not a rigged demo — so the watermark's actual drop
behavior is proven separately: a synthetic event is fed through the exact
same `_process_event()` function with a timestamp deliberately placed past
the lateness threshold, and the script asserts it gets dropped. The
anomaly count in the Delta sink grows across separate runs (6 new rows
appended on top of earlier runs' rows) — a real ACID append, not an
overwrite.

## Running the RAG stage

Uses a local embedding model (`all-MiniLM-L6-v2`), a local cross-encoder
reranker (`ms-marco-MiniLM-L-6-v2`), and a small local generation model
(`flan-t5-base`) — no external API key or per-call cost, so the pipeline is
free to re-run for grading.

```bash
cd src/rag

# 1. Chunk the policy docs (structure-aware, split on section headers)
python chunking.py

# 2. Embed chunks into a persistent Chroma collection + build the BM25
#    sparse index over the same chunks
python build_index.py

# 3. Ask questions: hybrid search (dense + BM25 fused via RRF) -> cross-
#    encoder rerank -> grounded generation with citations. If a question
#    mentions a flight_id, its live status is pulled from the Silver Delta
#    table and included alongside the policy answer.
python qa.py
```

**Expected output** (see [docs/rag_pipeline_run.txt](docs/rag_pipeline_run.txt) for a full captured run):

```
Q: If my flight is cancelled, what are my options?
A: Passengers are automatically offered, at no cost: 1. Rebooking onto the
   next available flight on the same route (see Rebooking Policy), or 2. A
   full refund to the original payment method, or 3. Rebooking onto a
   different route via a connection, if the direct route is cancelled for
   an extended period (multi-day weather event, for example).
   Citations:
     - Flight Cancellation Policy > Passenger Options on Cancellation
     - Passenger Rebooking Policy > Cancellation-Based Rebooking
     - Flight Cancellation Policy > Same-Day Cancellation and Rebooking Interaction
```

Reranking is doing real, visible work here: for "what happens if my flight
is delayed for a technical issue," the correct section (*Compensation by
Delay Cause*) ranks 3rd out of the hybrid-fused candidates before
reranking, and 2nd after — an honest look at both stages, not a
cherry-picked result. Answer quality from the small local model is
similarly honest: three of the four demo questions produce full, grounded
sentences, and one produces a terse heading-like answer — a real limitation
of a lightweight, free, locally-run model rather than a papered-over
result.

## Running the orchestration + quality/lineage stage

Airflow runs as its own container (`docker/Dockerfile.airflow`, standalone
mode — scheduler, webserver, and triggerer in one process, SQLite-backed).
Rather than having Airflow spin up sibling containers (which runs into
Docker Desktop's host-path translation problems for Docker-outside-of-Docker
on Windows), every pipeline dependency is installed directly into the
Airflow image and the same `src/`/`data/` directories are mounted in as
plain volumes — identical in spirit to how `spark-runner` already works.

```bash
cd docker
docker compose build airflow
docker compose up -d airflow

# First run only: fix ownership so both the Spark and Airflow containers
# (different default users) can write into the shared data/ directory
docker compose run --rm spark-runner chmod -R 777 /data

# Get the auto-generated admin password
docker logs flight-ops-airflow | grep "Login with username"

# Trigger the DAG (or use the web UI at http://localhost:8080)
docker exec flight-ops-airflow airflow dags unpause flight_ops_pipeline
docker exec flight-ops-airflow airflow dags trigger flight_ops_pipeline

# Check progress
docker exec flight-ops-airflow airflow tasks states-for-dag-run \
  flight_ops_pipeline <run_id>
```

The DAG: `produce_and_validate -> load_bronze -> quality_gate_bronze ->
silver_merge -> quality_gate_silver -> gold_aggregate`. Each quality gate
runs real Great Expectations checks against the Bronze/Silver Delta tables
and wraps its work in an OpenLineage `lineage_run()` context that emits
START, then COMPLETE or FAIL, to the console.

**Expected output — clean run** (see [docs/orchestration_run.txt](docs/orchestration_run.txt)):

```
quality_gate_bronze  | success | ...
silver_merge         | success | ...
quality_gate_silver  | success | ...
gold_aggregate       | success | ...
```

**Expected output — a bad row injected directly into Bronze** (same file):

```
quality_gate_bronze  | failed          | ...
silver_merge         | upstream_failed | ...
quality_gate_silver  | upstream_failed | ...
gold_aggregate       | upstream_failed | ...
```

This is the actual rubric requirement in action: the bad row (`delay_minutes
= -500`) is a value Delta's schema enforcement has no opinion on — it's a
perfectly valid integer — but it violates a business rule GE checks for.
The gate catches it, emits an OpenLineage FAIL event (see
[docs/quality_lineage_run.txt](docs/quality_lineage_run.txt) for
the raw JSON), and every downstream task is correctly marked
`upstream_failed` rather than running against bad data.

That same evidence file also documents a real edge case found while testing
the FAIL path: pointing a gate at a nonexistent path causes `deltalake`'s
Rust layer to panic with an exception type that bypasses a plain `except
Exception` — noted honestly rather than hidden, since the business-rule
failure path (the one the rubric actually asks for) works correctly.

## Training program attribution

Completed as the capstone project for **Modern Data Engineering for AI Systems**, SDAIA Academy (delivered via Learning Space).

See also: [SDAIA Academy on GitHub](https://github.com/SDAIAAcademy).

## Made By

Abdulaziz Alfadhli · Abdullah Alzahrani · Fahad Alsubaie · Abdulwahab Alolyan
