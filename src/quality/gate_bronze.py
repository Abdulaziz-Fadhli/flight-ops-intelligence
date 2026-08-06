"""Bronze quality gate: structural expectations that must hold before Silver
is allowed to run at all. A failed expectation raises, which fails the
Airflow task and halts every downstream task via the default trigger rule -
this is the actual "failed quality gate halts the pipeline" behavior, not
just a logged warning.
"""

import os
import sys

import great_expectations as ge
from deltalake import DeltaTable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lineage"))
from emit import lineage_run  # noqa: E402

LAKEHOUSE_ROOT = os.environ.get(
    "LAKEHOUSE_ROOT", os.path.join(os.path.dirname(__file__), "..", "..", "data", "lakehouse")
)
BRONZE_PATH = os.path.join(LAKEHOUSE_ROOT, "bronze", "flight_events")

ALLOWED_STATUSES = [
    "SCHEDULED", "BOARDING", "GATE_CHANGE", "DEPARTED", "DELAYED", "ARRIVED", "CANCELLED",
]


def run() -> None:
    with lineage_run("quality_gate_bronze"):
        df = DeltaTable(BRONZE_PATH).to_pandas()

        # Consistency: a flight can't depart and arrive at the same airport.
        # Expressed as a derived boolean column since GX 0.18's pandas API
        # doesn't have a direct "columns must differ" expectation.
        df["origin_ne_destination"] = df["origin"] != df["destination"]

        gx_df = ge.from_pandas(df)

        checks = [
            gx_df.expect_column_values_to_not_be_null("flight_id"),
            gx_df.expect_column_values_to_be_in_set("status", ALLOWED_STATUSES),
            gx_df.expect_column_values_to_be_between("delay_minutes", min_value=0, mostly=1.0),
            gx_df.expect_column_values_to_be_in_set("origin_ne_destination", [True]),
        ]

        failures = [c for c in checks if not c["success"]]
        if failures:
            for f in failures:
                exp_type = f["expectation_config"]["expectation_type"]
                unexpected = f["result"].get("unexpected_count", "?")
                print(f"FAILED expectation: {exp_type} -> {unexpected} unexpected value(s)")
            raise ValueError(f"Bronze quality gate failed: {len(failures)} expectation(s) not met")

        print(f"Bronze quality gate PASSED: {len(df)} rows checked, "
              f"{len(checks)} expectations satisfied.")


if __name__ == "__main__":
    run()
