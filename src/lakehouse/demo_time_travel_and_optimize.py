"""Demonstrates three Delta Lake capabilities that fall out of the
transaction log for free, on top of Silver's existing MERGE history:

  - Transaction log inspection: the full audit trail of every write.
  - Time travel: querying Silver exactly as it looked at an earlier version,
    before later MERGEs changed it.
  - OPTIMIZE + Z-ORDER: physically co-locating Silver's files by flight_id,
    the column every downstream query filters or joins on.
"""

from delta.tables import DeltaTable

from spark_session import SILVER_PATH, get_spark


def run() -> None:
    spark = get_spark("time-travel-and-optimize-demo")
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)

    print("=== Transaction log: every write Silver has ever recorded ===")
    history = silver_table.history().select("version", "timestamp", "operation")
    history.show(truncate=False)

    versions = [row["version"] for row in history.collect()]
    earliest_version, latest_version = min(versions), max(versions)

    print(f"=== Time travel: Silver as of version {earliest_version} vs. current ({latest_version}) ===")
    earliest_df = spark.read.format("delta").option("versionAsOf", earliest_version).load(SILVER_PATH)
    current_df = spark.read.format("delta").load(SILVER_PATH)
    print(f"Version {earliest_version} row count: {earliest_df.count()}")
    print(f"Version {latest_version} (current) row count: {current_df.count()}")
    print(f"-> Same table, two points in history, both queryable - nothing was overwritten or lost.")

    print("=== OPTIMIZE + Z-ORDER on flight_id (the column every query filters/joins on) ===")
    result = silver_table.optimize().executeZOrderBy("flight_id")
    metrics = result.select("metrics.numFilesAdded", "metrics.numFilesRemoved").collect()[0]
    print(f"Files removed (small, pre-optimize): {metrics['numFilesRemoved']}")
    print(f"Files added (compacted, Z-ordered): {metrics['numFilesAdded']}")

    print("=== Transaction log after OPTIMIZE - the compaction itself is just another logged version ===")
    silver_table.history().select("version", "timestamp", "operation").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    run()
