"""Gold layer: daily on-time performance and delay-by-cause aggregate.

Computed fresh from Silver on every run - this is a genuine aggregate
(counts, rates, averages grouped by airline + date), not a copy of Silver's
row-per-flight state.
"""

from pyspark.sql import functions as F

from spark_session import GOLD_PATH, SILVER_PATH, get_spark


def run() -> None:
    spark = get_spark("gold-aggregate")

    silver_df = spark.read.format("delta").load(SILVER_PATH)

    gold_df = (
        silver_df.groupBy("airline_code", "scheduled_date")
        .agg(
            F.count("*").alias("total_flights"),
            F.sum(F.when(F.col("status") == "DELAYED", 1).otherwise(0)).alias("delayed_count"),
            F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
            F.avg(F.when(F.col("status") == "DELAYED", F.col("delay_minutes"))).alias(
                "avg_delay_minutes"
            ),
        )
        .withColumn(
            "on_time_pct",
            F.round(
                100.0
                * (F.col("total_flights") - F.col("delayed_count") - F.col("cancelled_count"))
                / F.col("total_flights"),
                1,
            ),
        )
        .orderBy("scheduled_date", "airline_code")
    )

    (
        gold_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_PATH)
    )

    print(f"Gold: wrote daily ops summary for {gold_df.count()} airline/date groups to {GOLD_PATH}")
    gold_df.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    run()
