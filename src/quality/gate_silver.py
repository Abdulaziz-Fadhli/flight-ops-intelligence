"""Silver quality gate: after the MERGE, Silver should hold exactly one
current row per flight_id and every airport code should look like a real
3-letter IATA code - both are referential/business-rule checks a bronze-
level structural check can't catch (they only make sense post-merge).
"""

import os
import sys

import great_expectations as ge
import pandas as pd
from deltalake import DeltaTable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lineage"))
from emit import lineage_run  # noqa: E402

LAKEHOUSE_ROOT = os.environ.get(
    "LAKEHOUSE_ROOT", os.path.join(os.path.dirname(__file__), "..", "..", "data", "lakehouse")
)
SILVER_PATH = os.path.join(LAKEHOUSE_ROOT, "silver", "flight_state")

MAX_EVENT_SCHEDULED_DRIFT_DAYS = 1


def run() -> None:
    with lineage_run("quality_gate_silver"):
        df = DeltaTable(SILVER_PATH).to_pandas()

        # Timeliness: an event about a flight shouldn't be timestamped days
        # away from that flight's scheduled date - catches clock skew,
        # backdated events, or a corrupted scheduled_date.
        event_date = pd.to_datetime(df["event_ts"], utc=True).dt.tz_localize(None).dt.normalize()
        scheduled_date = pd.to_datetime(df["scheduled_date"])
        df["event_scheduled_drift_days"] = (event_date - scheduled_date).abs().dt.days

        gx_df = ge.from_pandas(df)

        checks = [
            gx_df.expect_column_values_to_be_unique("flight_id"),
            gx_df.expect_column_values_to_match_regex("origin", r"^[A-Z]{3,4}$"),
            gx_df.expect_column_values_to_match_regex("destination", r"^[A-Z]{3,4}$"),
            gx_df.expect_column_values_to_be_between(
                "event_scheduled_drift_days", min_value=0, max_value=MAX_EVENT_SCHEDULED_DRIFT_DAYS
            ),
        ]

        failures = [c for c in checks if not c["success"]]
        if failures:
            for f in failures:
                exp_type = f["expectation_config"]["expectation_type"]
                unexpected = f["result"].get("unexpected_count", "?")
                print(f"FAILED expectation: {exp_type} -> {unexpected} unexpected value(s)")
            raise ValueError(f"Silver quality gate failed: {len(failures)} expectation(s) not met")

        print(f"Silver quality gate PASSED: {len(df)} flights checked, "
              f"{len(checks)} expectations satisfied.")


if __name__ == "__main__":
    run()
