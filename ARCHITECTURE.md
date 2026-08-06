# Architecture

## Pipeline overview

```
producer.py --> flight-events-raw (Kafka)
                        |
                        v
          validator_consumer.py (Pydantic contract)
                 /              \
                v                v
   flight-events-validated   flight-events-dlq
                |
                v
        kafka_to_bronze.py --> Bronze (Delta, raw/append-only)
                |
                v
        quality_gate_bronze.py (Great Expectations)  --[fail]--> halt
                |
                v
        silver_merge.py --> Silver (Delta, MERGE upsert keyed on flight_id)
                |
                v
        quality_gate_silver.py (Great Expectations)  --[fail]--> halt
                |
                v
        gold_aggregate.py --> Gold (Delta, daily on-time/delay-by-cause aggregate)


       docs/policy_docs/*.md --> chunking.py --> build_index.py --> Chroma + BM25 index
                                                                          |
                                                                          v
                                            qa.py: hybrid search -> rerank -> generate
                                                       (optionally joins live status
                                                        from Silver, keyed on flight_id
                                                        mentioned in the question)
```

All of the above (except the RAG stage, which is invoked directly) is wired
together as one Airflow DAG (`dags/flight_ops_pipeline.py`):

```
produce_and_validate -> load_bronze -> quality_gate_bronze -> silver_merge
                                              |
                                    (halts here on failure)
                                              v
                            quality_gate_silver -> gold_aggregate
```

Every quality gate and lakehouse stage emits OpenLineage `START` /
`COMPLETE` / `FAIL` events via `src/lineage/emit.py`.

## Repository layout

```
dags/                       Airflow DAG definition
docker/
  docker-compose.yml        Kafka (KRaft), spark-runner, airflow services
  Dockerfile.spark           Spark + Delta Lake image (used by lakehouse stage)
  Dockerfile.airflow          Airflow image with every pipeline dependency baked in
docs/
  policy_docs/*.md           Source documents for the RAG pipeline
  day*_*.txt                 Captured evidence/output from each stage's real run
src/
  ingestion/                 Kafka producer + Pydantic-validating consumer
  lakehouse/                 Bronze/Silver/Gold Delta Lake jobs (PySpark)
  rag/                       Chunking, embeddings, hybrid search, reranking, generation
  quality/                   Great Expectations gates for Bronze and Silver
  lineage/                   OpenLineage event emission helper
requirements.txt             Python dependencies (native/local runs)
```

## Why things run where they run

- **Kafka** runs in Docker (KRaft mode, no Zookeeper) — the same for any OS.
- **The lakehouse stage (PySpark + Delta Lake) runs in a Linux container**
  (`spark-runner`), not natively on Windows. Native Windows Spark needs
  `winutils.exe`, a Windows-specific Hadoop shim normally fetched from a
  third-party GitHub mirror — rather than pulling an unverified binary, the
  Spark jobs run in a proper Linux container instead, which also removes any
  host-specific quirks from the setup a grader has to reproduce.
- **Airflow runs standalone in its own container**, with every pipeline
  dependency (PySpark, Delta Lake, Great Expectations, OpenLineage, kafka-python,
  Pydantic) installed directly into the image, and `src/`/`data/` mounted in
  as plain volumes. The alternative — Airflow spinning up sibling containers
  via a mounted Docker socket — runs into Docker Desktop's host-path
  translation behavior for Docker-outside-of-Docker on Windows; baking
  dependencies into one image sidesteps that entirely.
- **The RAG pipeline runs with entirely local models** (`all-MiniLM-L6-v2`
  for embeddings, `ms-marco-MiniLM-L-6-v2` for reranking, `flan-t5-base` for
  generation) — no external API key, no per-call cost, so it's free to
  re-run for grading.

## Configuration / environment variables

| Variable | Used by | Default | Purpose |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | ingestion, lakehouse scripts | `localhost:29092` (native), `kafka:9092` (in-container) | Kafka broker address |
| `LAKEHOUSE_ROOT` | lakehouse, quality scripts | `data/lakehouse` (relative), `/data/lakehouse` (in-container) | Root path for Bronze/Silver/Gold Delta tables |
| `KAGGLE_USERNAME`, `KAGGLE_KEY` | `src/quality/demo_real_flight_dataset.py` only | none | Kaggle API credentials, needed only for the optional real-dataset demo. Never committed; set as environment variables from your own `kaggle.json`. |

No secrets or API keys are required for the core pipeline (ingestion,
lakehouse, RAG, orchestration, synthetic-data quality gates) — everything
runs locally. The one exception is the optional real-dataset quality demo,
which needs your own free Kaggle account.
