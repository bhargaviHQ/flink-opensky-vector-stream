# flink-opensky-vector-stream

An event-driven pipeline using Apache Flink and Kafka to process unbounded ADS-B telemetry for real-time aviation spatial analytics and anomaly detection.

## Project Structure

```text
src/main/java/com/bhargavihq/flinkopenskyvectorstream/
├── FlightEvent.java
└── OpenSkyVectorStreamJob.java
```

## Build

```bash
mvn clean package
```

## Infrastructure

The `docker/` directory contains a `docker-compose.yml` that starts the full local stack:

| Service              | Description                          | Exposed ports |
|----------------------|--------------------------------------|---------------|
| `zookeeper`          | Bitnami Zookeeper 3.9                | 2181          |
| `kafka`              | Bitnami Kafka 3.7                    | 9092          |
| `clickhouse`         | ClickHouse 24.4                      | 8123, 9000    |
| `flink-jobmanager`   | Flink 1.19 JobManager                | 8081 (Web UI) |
| `flink-taskmanager`  | Flink 1.19 TaskManager (2 slots)     | —             |

### Start the stack

```bash
cd docker
docker compose up -d
```

Wait until all services are healthy (check with `docker compose ps`).

### Run the OpenSky producer

The `producer/` directory contains a Python script that polls the
[OpenSky Network REST API](https://opensky-network.org/api/states/all) every 10 seconds and
publishes each flight state as a JSON message to the Kafka topic `raw_flight_data`.

```bash
cd producer
pip install -r requirements.txt
python opensky_producer.py
```

> **Note:** The producer expects Kafka to be reachable at `kafka:9092` (the service name used
> inside the Docker network). When running the script on your host machine, either add
> `kafka` to your `/etc/hosts` pointing to `127.0.0.1`, or override `KAFKA_BOOTSTRAP_SERVERS`
> before starting the script:
>
> ```bash
> # host-machine override
> KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python opensky_producer.py
> ```

### Stop the stack

```bash
cd docker
docker compose down
```
