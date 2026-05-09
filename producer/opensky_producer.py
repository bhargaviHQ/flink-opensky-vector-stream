import json
import time
import logging

import requests
from confluent_kafka import Producer

OPENSKY_URL = "https://opensky-network.org/api/states/all"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "raw_flight_data"
POLL_INTERVAL_SECONDS = 10

# Index mapping for the OpenSky states/all response array.
# Reference: https://opensky-network.org/apidoc/rest.html#response
STATE_FIELDS = [
    "icao24",          # 0  – ICAO 24-bit address (hex string)
    "callsign",        # 1  – call sign (str or null)
    "origin_country",  # 2  – country of origin
    "time_position",   # 3  – UNIX timestamp of last position update (int or null)
    "last_contact",    # 4  – UNIX timestamp of last update from transponder
    "longitude",       # 5  – WGS-84 longitude in decimal degrees (float or null)
    "latitude",        # 6  – WGS-84 latitude in decimal degrees (float or null)
    "baro_altitude",   # 7  – barometric altitude in metres (float or null)
    "on_ground",       # 8  – true if aircraft is on ground
    "velocity",        # 9  – ground speed in m/s (float or null)
    "true_track",      # 10 – track angle in degrees clockwise from north (float or null)
    "vertical_rate",   # 11 – vertical rate in m/s; positive = climbing (float or null)
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def delivery_report(err, msg):
    if err is not None:
        logger.error("Delivery failed for record %s: %s", msg.key(), err)
    else:
        logger.debug(
            "Record delivered to %s [%s] at offset %s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def flatten_state(state: list) -> dict:
    """Convert a raw OpenSky state array into a keyed dict matching FlightEvent fields."""
    return {field: state[idx] for idx, field in enumerate(STATE_FIELDS)}


def fetch_states():
    try:
        response = requests.get(OPENSKY_URL, timeout=15)
        response.raise_for_status()
        data = response.json()
        return data.get("states") or []
    except requests.RequestException as exc:
        logger.error("Failed to fetch OpenSky data: %s", exc)
        return []


def main():
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "acks": "all",
            "retries": 5,
            "max.in.flight.requests.per.connection": 1,
        }
    )
    logger.info("Producer started. Polling %s every %ds", OPENSKY_URL, POLL_INTERVAL_SECONDS)

    while True:
        try:
            states = fetch_states()
            logger.info("Fetched %d flight states", len(states))

            for state in states:
                event = flatten_state(state)
                record = json.dumps(event)
                producer.produce(
                    KAFKA_TOPIC,
                    key=event.get("icao24", "").encode("utf-8"),
                    value=record.encode("utf-8"),
                    callback=delivery_report,
                )

            producer.flush()
        except Exception as exc:  # noqa: BLE001
            logger.error("Unexpected error during produce cycle: %s", exc)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

