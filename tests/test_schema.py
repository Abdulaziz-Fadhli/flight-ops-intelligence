"""Tests for the Pydantic data contract enforced at the Kafka ingestion
boundary (src/ingestion/schema.py). These back up the README/rubric claim
that malformed records are rejected with a specific reason, rather than
silently dropped or coerced.
"""

import os
import sys
from datetime import date, datetime

import pytest
from pydantic import ValidationError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "ingestion"))
from schema import FlightEvent, FlightStatus  # noqa: E402


def _base_event(**overrides):
    payload = dict(
        flight_id="FL123",
        airline_code="sv",
        flight_number="123",
        scheduled_date=date(2026, 8, 6),
        origin="ruh",
        destination="jed",
        status=FlightStatus.SCHEDULED,
        gate=None,
        delay_minutes=None,
        delay_reason_code=None,
        event_ts=datetime(2026, 8, 6, 10, 0, 0),
        source_feed="test-feed",
    )
    payload.update(overrides)
    return payload


def test_valid_scheduled_event_passes():
    event = FlightEvent(**_base_event())
    assert event.flight_id == "FL123"


def test_airline_and_airport_codes_are_uppercased():
    event = FlightEvent(**_base_event())
    assert event.airline_code == "SV"
    assert event.origin == "RUH"
    assert event.destination == "JED"


def test_boarding_without_gate_is_rejected():
    with pytest.raises(ValidationError, match="gate is required"):
        FlightEvent(**_base_event(status=FlightStatus.BOARDING, gate=None))


def test_gate_change_without_gate_is_rejected():
    with pytest.raises(ValidationError, match="gate is required"):
        FlightEvent(**_base_event(status=FlightStatus.GATE_CHANGE, gate=None))


def test_boarding_with_gate_passes():
    event = FlightEvent(**_base_event(status=FlightStatus.BOARDING, gate="A12"))
    assert event.gate == "A12"


def test_delayed_without_reason_is_rejected():
    with pytest.raises(ValidationError, match="delay_reason_code is required"):
        FlightEvent(
            **_base_event(status=FlightStatus.DELAYED, delay_minutes=30, delay_reason_code=None)
        )


def test_delayed_with_reason_passes():
    event = FlightEvent(
        **_base_event(status=FlightStatus.DELAYED, delay_minutes=30, delay_reason_code="ATC")
    )
    assert event.delay_reason_code == "ATC"


def test_negative_delay_minutes_is_rejected():
    with pytest.raises(ValidationError):
        FlightEvent(
            **_base_event(status=FlightStatus.DELAYED, delay_minutes=-5, delay_reason_code="ATC")
        )


def test_invalid_status_enum_is_rejected():
    with pytest.raises(ValidationError):
        FlightEvent(**_base_event(status="NOT_A_REAL_STATUS"))


def test_missing_required_field_is_rejected():
    payload = _base_event()
    del payload["flight_id"]
    with pytest.raises(ValidationError):
        FlightEvent(**payload)
