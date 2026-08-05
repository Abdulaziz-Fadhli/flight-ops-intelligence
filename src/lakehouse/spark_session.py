"""Shared Spark + Delta Lake session builder.

Pins JAVA_HOME to a Spark-compatible JDK (17) for the Spark subprocess only,
since the machine's default system Java may be a version Spark's Py4J
gateway and Hadoop shims don't support.
"""

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

_JAVA_HOME_FOR_SPARK = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.8-hotspot"

LAKEHOUSE_ROOT = os.environ.get(
    "LAKEHOUSE_ROOT",
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "lakehouse"),
)
BRONZE_PATH = os.path.join(LAKEHOUSE_ROOT, "bronze", "flight_events")
SILVER_PATH = os.path.join(LAKEHOUSE_ROOT, "silver", "flight_state")
GOLD_PATH = os.path.join(LAKEHOUSE_ROOT, "gold", "daily_ops_summary")


def get_spark(app_name: str = "flight-ops-lakehouse") -> SparkSession:
    if os.path.isdir(_JAVA_HOME_FOR_SPARK):
        os.environ["JAVA_HOME"] = _JAVA_HOME_FOR_SPARK

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", os.path.join(LAKEHOUSE_ROOT, "_warehouse"))
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
