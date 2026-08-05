"""Spark schema for the flight-event lakehouse tables.

Mirrors the FlightEvent Pydantic contract from the ingestion stage, since a
record that already passed the ingestion boundary shouldn't need a
different shape here - the type still gets enforced by Delta on write.
"""

from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_SCHEMA = StructType(
    [
        StructField("flight_id", StringType(), nullable=False),
        StructField("airline_code", StringType(), nullable=False),
        StructField("flight_number", StringType(), nullable=False),
        StructField("scheduled_date", DateType(), nullable=False),
        StructField("origin", StringType(), nullable=False),
        StructField("destination", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("gate", StringType(), nullable=True),
        StructField("delay_minutes", IntegerType(), nullable=True),
        StructField("delay_reason_code", StringType(), nullable=True),
        StructField("event_ts", TimestampType(), nullable=False),
        StructField("source_feed", StringType(), nullable=False),
        StructField("ingested_at", TimestampType(), nullable=False),
    ]
)
