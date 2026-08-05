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
- [ ] Delta Lakehouse
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

## Training program attribution

Completed as the capstone project for **Modern Data Engineering for AI Systems**, SDAIA Academy (delivered via Learning Space).

See also: [SDAIA Academy on GitHub](https://github.com/SDAIAAcademy).
