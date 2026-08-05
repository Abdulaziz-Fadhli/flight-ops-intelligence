"""Silver layer: current, conformed state per flight.

Reads Bronze (the full raw event history) and MERGEs it into Silver, keyed
on the business key `flight_id`. A matched row is only updated if the
incoming event is actually newer (`event_ts` comparison) - a blind
overwrite would let a late-arriving stale event clobber a newer one, which
is exactly the kind of bug real multi-feed ingestion produces.
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_session import BRONZE_PATH, SILVER_PATH, get_spark


def run() -> None:
    spark = get_spark("silver-merge")

    bronze_df = spark.read.format("delta").load(BRONZE_PATH)

    # Bronze can contain multiple events per flight_id; only the latest
    # event per flight actually represents "current state" for Silver.
    latest_per_flight = Window.partitionBy("flight_id").orderBy(F.col("event_ts").desc())
    latest_events = (
        bronze_df.withColumn("_rn", F.row_number().over(latest_per_flight))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        (
            latest_events.write.format("delta")
            .mode("overwrite")
            .save(SILVER_PATH)
        )
        print(f"Silver: initialized table with {latest_events.count()} flights at {SILVER_PATH}")
        spark.stop()
        return

    silver_table = DeltaTable.forPath(spark, SILVER_PATH)

    (
        silver_table.alias("target")
        .merge(
            latest_events.alias("source"),
            "target.flight_id = source.flight_id",
        )
        .whenMatchedUpdateAll(condition="source.event_ts > target.event_ts")
        .whenNotMatchedInsertAll()
        .execute()
    )

    result_count = spark.read.format("delta").load(SILVER_PATH).count()
    print(f"Silver: MERGE complete, {result_count} flights tracked at {SILVER_PATH}")
    spark.stop()


if __name__ == "__main__":
    run()
