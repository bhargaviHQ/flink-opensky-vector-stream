import json
import time
import logging

import requests
from confluent_kafka import Producer

OPENSKY_URL = "https://opensky-network.org/api/states/all"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "raw_flight_data"
POLL_INTERVAL_SECONDS = 10

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
                record = json.dumps(state)
                producer.produce(
                    KAFKA_TOPIC,
                    value=record.encode("utf-8"),
                    callback=delivery_report,
                )

            producer.flush()
        except Exception as exc:  # noqa: BLE001
            logger.error("Unexpected error during produce cycle: %s", exc)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
