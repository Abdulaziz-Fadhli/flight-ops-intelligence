"""Flight Ops Intelligence - end-to-end batch pipeline DAG.

Wires ingestion -> Bronze -> quality gate -> Silver MERGE -> quality gate ->
Gold into one DAG with the default `all_success` trigger rule on every
edge, so a failed quality gate genuinely halts the pipeline before any
downstream stage runs - not just logs a warning and continues.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

SRC = "/opt/airflow/src"

default_args = {
    "owner": "flight-ops-intelligence",
    "retries": 0,
}

with DAG(
    dag_id="flight_ops_pipeline",
    description="Ingest flight events, land them in the lakehouse, and gate quality at each layer.",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["capstone", "lakehouse"],
) as dag:

    produce_and_validate = BashOperator(
        task_id="produce_and_validate",
        bash_command=(
            f"cd {SRC}/ingestion && python3 producer.py && python3 validator_consumer.py"
        ),
    )

    load_bronze = BashOperator(
        task_id="load_bronze",
        bash_command=f"cd {SRC}/lakehouse && python3 kafka_to_bronze.py",
    )

    quality_gate_bronze = BashOperator(
        task_id="quality_gate_bronze",
        bash_command=f"cd {SRC}/quality && python3 gate_bronze.py",
    )

    silver_merge = BashOperator(
        task_id="silver_merge",
        bash_command=f"cd {SRC}/lakehouse && python3 silver_merge.py",
    )

    quality_gate_silver = BashOperator(
        task_id="quality_gate_silver",
        bash_command=f"cd {SRC}/quality && python3 gate_silver.py",
    )

    gold_aggregate = BashOperator(
        task_id="gold_aggregate",
        bash_command=f"cd {SRC}/lakehouse && python3 gold_aggregate.py",
    )

    (
        produce_and_validate
        >> load_bronze
        >> quality_gate_bronze
        >> silver_merge
        >> quality_gate_silver
        >> gold_aggregate
    )
