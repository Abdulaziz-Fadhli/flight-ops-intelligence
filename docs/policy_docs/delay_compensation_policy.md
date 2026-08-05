# Delay Compensation Policy

## Purpose

Defines what compensation, if any, a passenger is entitled to when their
flight is delayed, based on the delay length and its cause.

## Compensation by Delay Cause

Compensation eligibility depends heavily on the recorded delay reason code:

- **WEATHER**: no cash compensation is owed, since weather delays are
  outside airline control. Passengers delayed over 8 hours still receive a
  meal voucher and, if overnight, a hotel accommodation voucher.
- **ATC** (air traffic control restriction): treated the same as WEATHER -
  no cash compensation, meal/hotel vouchers apply at the same thresholds.
- **CREW** (crew scheduling or crew rest limits): considered within airline
  control. Passengers delayed over 3 hours are entitled to a cash
  compensation payment in addition to meal/hotel vouchers at the usual
  thresholds.
- **TECHNICAL** (aircraft mechanical issue): considered within airline
  control, same compensation treatment as CREW.
- **LATE_INBOUND** (delay caused by the aircraft's previous incoming
  flight): treated as within airline control unless the previous flight's
  own delay reason was WEATHER or ATC, in which case it is treated as
  outside airline control.

## Cash Compensation Amounts

For delay reasons within airline control:

| Delay length      | Compensation           |
|-------------------|-------------------------|
| 3-6 hours         | 300 SAR                |
| 6-8 hours         | 600 SAR                |
| Over 8 hours      | 900 SAR                |

## Meal and Hotel Vouchers

Independent of cause, any delay over 4 hours qualifies the passenger for a
meal voucher. Any delay classified as overnight (original or rebooked
departure falls between 22:00 and 06:00 local time) qualifies for a hotel
accommodation voucher, regardless of delay cause.

## Claiming Compensation

Passengers must submit a compensation claim within 30 days of the delayed
flight's actual departure. Claims reference the flight's recorded delay
reason code, which is treated as authoritative for determining eligibility.
