"""
Validates that a sample OpenSky state, once flattened by the producer,
contains exactly the keys expected by FlightEvent.
"""

import sys
import os

# Allow importing from the same directory without package installation.
sys.path.insert(0, os.path.dirname(__file__))

from opensky_producer import flatten_state, STATE_FIELDS  # noqa: E402

EXPECTED_KEYS = {
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
}

# A realistic OpenSky state array (truncated at index 11 — the fields we care about).
SAMPLE_STATE = [
    "3c6444",       # icao24
    "DLH123 ",      # callsign
    "Germany",      # origin_country
    1715000000,     # time_position
    1715000005,     # last_contact
    8.5432,         # longitude
    50.0379,        # latitude
    10668.0,        # baro_altitude
    False,          # on_ground
    240.0,          # velocity
    270.5,          # true_track
    -2.6,           # vertical_rate
    None,           # sensors  (index 12, not mapped)
    10820.0,        # geo_altitude (index 13, not mapped)
    "1234",         # squawk (index 14, not mapped)
    False,          # spi (index 15, not mapped)
    0,              # position_source (index 16, not mapped)
]


def test_flatten_produces_expected_keys():
    event = flatten_state(SAMPLE_STATE)
    actual_keys = set(event.keys())
    assert actual_keys == EXPECTED_KEYS, (
        f"Key mismatch.\n  Missing : {EXPECTED_KEYS - actual_keys}\n"
        f"  Extra   : {actual_keys - EXPECTED_KEYS}"
    )
    print("PASS: flatten_state produced the correct keys:", sorted(actual_keys))


def test_state_fields_constant_matches_expected_keys():
    assert set(STATE_FIELDS) == EXPECTED_KEYS, (
        f"STATE_FIELDS does not match EXPECTED_KEYS.\n"
        f"  Missing : {EXPECTED_KEYS - set(STATE_FIELDS)}\n"
        f"  Extra   : {set(STATE_FIELDS) - EXPECTED_KEYS}"
    )
    print("PASS: STATE_FIELDS matches EXPECTED_KEYS.")


def test_values_are_preserved():
    event = flatten_state(SAMPLE_STATE)
    assert event["icao24"] == "3c6444"
    assert event["callsign"] == "DLH123 "
    assert event["origin_country"] == "Germany"
    assert event["time_position"] == 1715000000
    assert event["last_contact"] == 1715000005
    assert event["longitude"] == 8.5432
    assert event["latitude"] == 50.0379
    assert event["baro_altitude"] == 10668.0
    assert event["on_ground"] is False
    assert event["velocity"] == 240.0
    assert event["true_track"] == 270.5
    assert event["vertical_rate"] == -2.6
    print("PASS: all field values are preserved correctly.")


if __name__ == "__main__":
    test_state_fields_constant_matches_expected_keys()
    test_flatten_produces_expected_keys()
    test_values_are_preserved()
    print("\nAll schema validation tests passed.")
