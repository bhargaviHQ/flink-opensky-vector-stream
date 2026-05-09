-- ClickHouse schema for ADS-B flight telemetry.
--
-- ReplacingMergeTree deduplicates rows sharing the same ORDER BY key,
-- keeping the row with the greatest version value (last_contact).
-- This ensures that repeated updates for the same (aircraft, time-slot)
-- converge to the most recently observed state.

CREATE TABLE IF NOT EXISTS flights
(
    icao24         String,
    callsign       Nullable(String),
    origin_country String,
    time_position  Int64 DEFAULT 0,
    last_contact   Int64,
    longitude      Nullable(Float64),
    latitude       Nullable(Float64),
    baro_altitude  Nullable(Float64),
    on_ground      Bool,
    velocity       Nullable(Float64),
    true_track     Nullable(Float64),
    vertical_rate  Nullable(Float64)
)
ENGINE = ReplacingMergeTree(last_contact)
ORDER BY (icao24, time_position)
SETTINGS index_granularity = 8192;
