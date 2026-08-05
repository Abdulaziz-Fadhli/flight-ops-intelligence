"""Pydantic data contract enforced at the Kafka ingestion boundary.

Every raw flight-event record from the upstream feeds must satisfy this
contract before it is allowed into the validated stream. Anything that
fails validation is routed to the dead-letter topic with the reason
recorded, never silently dropped or coerced.
"""

from datetime import date, datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class FlightStatus(str, Enum):
    SCHEDULED = "SCHEDULED"
    BOARDING = "BOARDING"
    GATE_CHANGE = "GATE_CHANGE"
    DEPARTED = "DEPARTED"
    DELAYED = "DELAYED"
    ARRIVED = "ARRIVED"
    CANCELLED = "CANCELLED"


class FlightEvent(BaseModel):
    flight_id: str = Field(..., min_length=1)
    airline_code: str = Field(..., min_length=2, max_length=3)
    flight_number: str = Field(..., min_length=1)
    scheduled_date: date
    origin: str = Field(..., min_length=3, max_length=4)
    destination: str = Field(..., min_length=3, max_length=4)
    status: FlightStatus
    gate: Optional[str] = None
    delay_minutes: Optional[int] = Field(default=None, ge=0)
    delay_reason_code: Optional[str] = None
    event_ts: datetime
    source_feed: str

    @field_validator("airline_code", "origin", "destination")
    @classmethod
    def uppercase_codes(cls, v: str) -> str:
        return v.upper()

    @model_validator(mode="after")
    def gate_required_when_boarding_or_gate_change(self) -> "FlightEvent":
        if self.status in (FlightStatus.BOARDING, FlightStatus.GATE_CHANGE) and not self.gate:
            raise ValueError(
                f"gate is required when status is {self.status.value}"
            )
        return self

    @model_validator(mode="after")
    def delay_reason_required_when_delayed(self) -> "FlightEvent":
        if self.status == FlightStatus.DELAYED and not self.delay_reason_code:
            raise ValueError("delay_reason_code is required when status is DELAYED")
        return self
