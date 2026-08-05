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

Project scaffolding in progress — this README will be updated with setup/run instructions as each stage lands.

## Training program attribution

Completed as the capstone project for **Modern Data Engineering for AI Systems**, SDAIA Academy (delivered via Learning Space).

See also: [SDAIA Academy on GitHub](https://github.com/SDAIAAcademy).
