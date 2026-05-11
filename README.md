# flink-opensky-vector-stream

A real-time flight tracking pipeline that ingests live ADS-B telemetry from the [OpenSky Network](https://opensky-network.org), processes it with Apache Flink, stores it in ClickHouse, and visualises it in Grafana — all running locally via Docker.

---

## What This Does

Every 10 seconds, the pipeline:

1. **Fetches** live aircraft states from the OpenSky REST API
2. **Produces** each flight as a JSON event into a Kafka topic
3. **Processes** the stream in Flink across three parallel operations:
   - Writes every raw event to ClickHouse via JDBC
   - Emits a climb/descent alert when vertical rate exceeds 6 m/s (~1,200 ft/min)
   - Aggregates flight counts per country in 60-second tumbling windows
4. **Stores** the data in ClickHouse using a `ReplacingMergeTree` that deduplicates by `(icao24, time_position)`
5. **Visualises** everything in a Grafana dashboard that auto-refreshes every 10 seconds

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│   OpenSky Network REST API                                       │
│          │                                                       │
│          │  HTTP (every 10s, up to 200 aircraft states)          │
│          ▼                                                       │
│   ┌─────────────────┐                                            │
│   │ Python Producer │  confluent-kafka, flattens array → JSON    │
│   └────────┬────────┘                                            │
│            │  Kafka topic: raw_flight_data                       │
│            ▼                                                     │
│   ┌─────────────────┐                                            │
│   │  Apache Kafka   │  Confluent Platform 7.6 + ZooKeeper        │
│   └────────┬────────┘                                            │
│            │                                                     │
│            ▼                                                     │
│   ┌──────────────────────────────────────────┐                   │
│   │            Apache Flink 1.19             │                   │
│   │                                          │                   │
│   │  ┌────────────────────────────────────┐  │                   │
│   │  │  KafkaSource<FlightEvent>          │  │                   │
│   │  │  (event-time watermarks, ±5s)      │  │                   │
│   │  └──────────┬─────────────────────────┘  │                   │
│   │             │                            │                   │
│   │    ┌────────┼────────────┐               │                   │
│   │    ▼        ▼            ▼               │                   │
│   │  Raw      Alert        60-sec            │                   │
│   │  sink     stream       country           │                   │
│   │  (JDBC)   (keyed       window            │                   │
│   │           state)       (tumbling)        │                   │
│   └────┬──────────────────────────────────── ┘                   │
│        │                                                         │
│        ▼                                                         │
│   ┌─────────────────┐                                            │
│   │   ClickHouse    │  ReplacingMergeTree, dedup on              │
│   │   flights table │  (icao24, time_position)                   │
│   └────────┬────────┘                                            │
│            │                                                     │
│            ▼                                                     │
│   ┌─────────────────┐                                            │
│   │    Grafana      │  Auto-provisioned dashboard,               │
│   │   Dashboard     │  refreshes every 10s                       │
│   └─────────────────┘                                            │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Version | Role |
|---|---|---|---|
| **Data source** | OpenSky Network REST API | — | Live ADS-B flight telemetry |
| **Message queue** | Apache Kafka (Confluent Platform) | 7.6 | Durable event buffer |
| **Coordination** | Apache ZooKeeper | — | Kafka broker metadata |
| **Stream processor** | Apache Flink | 1.19 | Stateful stream processing, windowing, alerting |
| **Storage** | ClickHouse | 24.4 | Columnar OLAP database |
| **Visualisation** | Grafana | 10.4 | Live auto-refreshing dashboard |
| **Producer** | Python + confluent-kafka | 3.11 | Polls OpenSky, publishes to Kafka |
| **Container runtime** | Docker + Colima | — | Runs all services locally |
| **Build** | Apache Maven + shade plugin | 3.9 | Self-contained Flink fat JAR |

---

## Project Structure

```
.
├── docker/
│   ├── docker-compose.yml              # All 7 services
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/            # Auto-configures ClickHouse datasource
│       │   └── dashboards/             # Auto-loads dashboard on startup
│       └── dashboards/
│           └── flights.json            # Live flights dashboard definition
├── producer/
│   ├── Dockerfile                      # Python 3.11 + librdkafka
│   ├── opensky_producer.py             # Polls OpenSky, publishes to Kafka
│   └── requirements.txt
├── sql/
│   └── create_flights_table.sql        # ClickHouse schema (auto-applied on start)
├── src/
│   └── main/java/
│       ├── com/project/model/
│       │   └── FlightEvent.java        # Full OpenSky POJO with Jackson annotations
│       └── com/bhargavihq/flinkopenskyvectorstream/
│           ├── OpenSkyVectorStreamJob.java           # Main Flink job (3 operators)
│           └── FlightEventDeserializationSchema.java # Kafka JSON → FlightEvent
└── pom.xml
```

---

## Flink Job — Three Operators

### 1. Raw JDBC sink → ClickHouse
Every event is batched (1,000 records or 1 second) and written to ClickHouse. The `ReplacingMergeTree` engine deduplicates rows for the same `(icao24, time_position)`, keeping the latest `last_contact`.

### 2. Climb / descent alerts (keyed stateful process function)
For each aircraft (`icao24`), Flink keeps the previous `FlightEvent` in managed key/value state. On each update it computes:

```
vertical_rate = Δaltitude / Δtime_seconds
```

When `|vertical_rate| ≥ 6 m/s` an alert fires:
```
CLIMBING   icao24=a1b2c3 callsign=UAL123 rate=12.4 m/s alt=4200 m
DESCENDING icao24=d4e5f6 callsign=BAW456 rate=-9.1 m/s alt=1800 m
```

### 3. 60-second tumbling window — flights per country
Using event-time watermarks (bounded out-of-orderness: 5 seconds), Flink counts flights per country in each 60-second window and logs the result.

---

## Data Contract

OpenSky returns each aircraft state as a positional array. The producer maps it to named JSON fields:

```json
{
  "icao24": "a1b2c3",
  "callsign": "UAL123",
  "origin_country": "United States",
  "time_position": 1715382000,
  "last_contact": 1715382001,
  "longitude": -87.6298,
  "latitude": 41.8781,
  "baro_altitude": 10668.0,
  "on_ground": false,
  "velocity": 245.3,
  "true_track": 274.5,
  "vertical_rate": 0.0
}
```

Nullable fields (`longitude`, `latitude`, `baro_altitude`, `velocity`, `true_track`, `vertical_rate`) are typed as `Nullable(Float64)` in ClickHouse and boxed `Double` in the Java POJO.

---

## Running Locally

### Prerequisites

| Tool | Install |
|---|---|
| Java 11+ | `brew install openjdk` or Oracle JDK |
| Maven | Direct binary from [apache.org](https://maven.apache.org/download.cgi) |
| Colima | `brew install colima` |
| Docker CLI | `brew install docker docker-compose` |

### Start the pipeline

```bash
# 1. Start the Docker VM
colima start

# 2. Build the Flink fat JAR
mvn clean package -DskipTests

# 3. Start all 7 services (images download on first run — ~2 GB)
cd docker
docker-compose up -d

# 4. Create the Kafka topic
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic raw_flight_data \
  --partitions 1 --replication-factor 1

# 5. Submit the Flink job
JAR=$(curl -s -X POST http://localhost:8081/jars/upload -H "Expect:" \
  -F "jarfile=@../target/flink-opensky-vector-stream-0.1.0-SNAPSHOT.jar" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['filename'].split('/')[-1])")

curl -s -X POST "http://localhost:8081/jars/${JAR}/run" \
  -H "Content-Type: application/json" \
  -d '{"entryClass":"com.bhargavihq.flinkopenskyvectorstream.OpenSkyVectorStreamJob","parallelism":1}'
```

The producer starts automatically as part of `docker-compose up`.

### UIs

| Service | URL |
|---|---|
| Grafana dashboard | `http://localhost:3000` |
| Flink Web UI | `http://localhost:8081` |

### Query ClickHouse directly

```bash
# Row count
docker exec -it clickhouse clickhouse-client \
  --query "SELECT count() FROM flights"

# Top countries
docker exec -it clickhouse clickhouse-client \
  --query "SELECT origin_country, count() AS flights
           FROM flights
           GROUP BY origin_country
           ORDER BY flights DESC
           LIMIT 10"
```

### Stop everything

```bash
cd docker
docker-compose down   # removes containers (data is lost)
colima stop           # shuts down the Docker VM
```

---

## Grafana Dashboard Panels

| Panel | Query |
|---|---|
| Total flights tracked | `SELECT count() FROM flights` |
| In air right now | `SELECT count() FROM flights WHERE on_ground = false` |
| Countries represented | `SELECT count(DISTINCT origin_country) FROM flights` |
| Average cruise altitude | Mean `baro_altitude` for airborne flights |
| Top 15 countries | Group by `origin_country`, order by count |
| Altitude distribution | Histogram of `baro_altitude` for airborne flights |
| Live flight feed | Latest 50 events ordered by `last_contact` |

---

## Key Design Decisions

**Why ClickHouse?**
Columnar storage and `ReplacingMergeTree` are a natural fit for append-heavy telemetry with deduplication. Aggregation queries run in milliseconds even over millions of rows.

**Why Kafka between producer and Flink?**
Kafka decouples the polling rate from the processing rate, provides offset-based replay (useful during development), and gives Flink a durable checkpoint anchor for exactly-once semantics.

**Why compute vertical rate in Flink rather than using OpenSky's field directly?**
OpenSky's `vertical_rate` can be null or stale. Computing it from consecutive altitude observations inside Flink's keyed state gives a fresh per-observation rate and demonstrates stateful stream processing — the core of what Flink is built for.

**Why cap at 200 states per poll?**
OpenSky returns 10,000+ global aircraft per poll. For local development, 200 states every 10 seconds is enough to demonstrate the full pipeline without saturating a laptop.
