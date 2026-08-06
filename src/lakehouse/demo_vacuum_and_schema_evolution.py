"""Demonstrates two more Delta Lake capabilities not covered elsewhere:
VACUUM (retention cleanup) and schema evolution via mergeSchema.

Runs against a disposable scratch table seeded from Silver's current data,
not the real Silver table - VACUUM permanently deletes old file versions,
which would break the separate time-travel demo (demo_time_travel_and_optimize.py)
if run against the same table it depends on. Isolating this to a scratch
copy keeps every existing script safely re-runnable.
"""

import os
import shutil

from delta.tables import DeltaTable
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from spark_session import LAKEHOUSE_ROOT, SILVER_PATH, get_spark

SCRATCH_PATH = os.path.join(LAKEHOUSE_ROOT, "_demo", "vacuum_and_evolution")


def run() -> None:
    spark = get_spark("vacuum-and-schema-evolution-demo")

    if os.path.exists(SCRATCH_PATH):
        shutil.rmtree(SCRATCH_PATH)

    print("=== Seeding a disposable scratch table from Silver's current schema/data ===")
    seed_df = spark.read.format("delta").load(SILVER_PATH).limit(5)
    seed_df.write.format("delta").mode("overwrite").save(SCRATCH_PATH)
    # A second small write gives VACUUM something real to clean up (an old
    # file version superseded by this write).
    seed_df.limit(1).write.format("delta").mode("overwrite").save(SCRATCH_PATH)
    scratch_table = DeltaTable.forPath(spark, SCRATCH_PATH)
    print(f"Scratch table ready at {SCRATCH_PATH} with "
          f"{scratch_table.history().count()} versions so far.")

    print("\n=== VACUUM: retention cleanup ===")
    print("Delta keeps every historical file so time travel works. VACUUM permanently")
    print("deletes files no longer referenced by the current version, once they're")
    print("older than the retention window - trading away time travel for storage cost.")

    def _count_parquet_files() -> int:
        # Only the actual data files - _delta_log/ JSON commits and
        # checkpoints aren't what VACUUM cleans up, and counting them
        # alongside data files would muddy what this is demonstrating.
        count = 0
        for dirpath, _dirnames, files in os.walk(SCRATCH_PATH):
            if "_delta_log" in dirpath:
                continue
            count += sum(1 for f in files if f.endswith(".parquet"))
        return count

    pre_vacuum_files = _count_parquet_files()
    # 0-hour retention is only safe here because this is a throwaway scratch
    # table with no readers depending on its history - never do this in production.
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    scratch_table.vacuum(retentionHours=0)
    post_vacuum_files = _count_parquet_files()
    print(f"Parquet data files before VACUUM: {pre_vacuum_files}")
    print(f"Parquet data files after VACUUM:  {post_vacuum_files}")
    print("-> Orphaned Parquet files from the superseded write are gone; the table's")
    print("   current version is unaffected, but time travel to it is no longer possible.")

    print("\n=== Schema evolution: adding a column via mergeSchema ===")
    print("Without mergeSchema, appending a row with an extra column is rejected")
    print("(proven for Bronze in demo_schema_enforcement.py). mergeSchema=true instead")
    print("safely adds the new nullable column - existing rows get NULL, not an error.")

    current_schema = spark.read.format("delta").load(SCRATCH_PATH).schema
    evolved_schema = StructType(
        current_schema.fields + [StructField("ops_notes", StringType(), nullable=True)]
    )
    sample_row = spark.read.format("delta").load(SCRATCH_PATH).limit(1).collect()[0]
    new_row = spark.createDataFrame(
        [tuple(sample_row) + ("Manually reviewed - no anomalies.",)],
        schema=evolved_schema,
    )
    (
        new_row.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(SCRATCH_PATH)
    )

    result_df = spark.read.format("delta").load(SCRATCH_PATH).select("flight_id", "ops_notes")
    print("Table after schema evolution (existing rows show NULL for the new column):")
    result_df.show(truncate=False)

    shutil.rmtree(SCRATCH_PATH)
    print(f"Scratch table cleaned up ({SCRATCH_PATH} removed) - nothing left behind.")

    spark.stop()


if __name__ == "__main__":
    run()
