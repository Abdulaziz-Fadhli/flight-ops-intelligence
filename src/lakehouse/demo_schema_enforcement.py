"""Proves Delta actually refuses a bad write - not just that the happy path works.

Attempts to append a record where delay_minutes is a string instead of an
int, and a required field (flight_id) is dropped. Both should be rejected
by Delta's schema enforcement rather than silently coerced or nulled.
"""

from datetime import datetime, timezone

from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_session import BRONZE_PATH, get_spark

BAD_SCHEMA = StructType(
    [
        StructField("airline_code", StringType(), nullable=False),
        StructField("flight_number", StringType(), nullable=False),
        StructField("scheduled_date", DateType(), nullable=False),
        StructField("origin", StringType(), nullable=False),
        StructField("destination", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("delay_minutes", StringType(), nullable=True),  # wrong type: should be Integer
        StructField("event_ts", TimestampType(), nullable=False),
        StructField("source_feed", StringType(), nullable=False),
        StructField("ingested_at", TimestampType(), nullable=False),
        # flight_id (required, non-nullable in Bronze) is deliberately omitted
    ]
)


def run() -> None:
    spark = get_spark("schema-enforcement-demo")

    now = datetime.now(timezone.utc)
    bad_row = [
        (
            "SV",
            "9999",
            datetime.now(timezone.utc).date(),
            "RUH",
            "JED",
            "DELAYED",
            "thirty",  # not an int - violates the Bronze table's IntegerType
            now,
            "ops_system",
            now,
        )
    ]
    bad_df = spark.createDataFrame(bad_row, schema=BAD_SCHEMA)

    print("Attempting to append a record with wrong type (delay_minutes as string) "
          "and a missing required column (flight_id) into Bronze...\n")

    try:
        bad_df.write.format("delta").mode("append").save(BRONZE_PATH)
        print("UNEXPECTED: write succeeded - schema enforcement did not trigger.")
    except Exception as exc:  # Delta raises AnalysisException for schema mismatches
        print(f"REJECTED as expected. Delta raised: {type(exc).__name__}")
        print(f"Reason: {str(exc).splitlines()[0]}")

    spark.stop()


if __name__ == "__main__":
    run()
