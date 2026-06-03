package com.bhargavihq.flinkopenskyvectorstream;

import com.project.model.FlightEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.io.Serializable;

public class OpenSkyVectorStreamJob {

    private static final String KAFKA_BROKERS = "kafka:9092";
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/default";
    private static final String CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver";

    // Vertical rate thresholds in m/s: ~6 m/s ≈ 1200 ft/min
    private static final double ALERT_VERTICAL_RATE_MS = 6.0;
    static final int ALERT_STATE_TTL_MINUTES = 30;
    static final int HOTSPOT_WINDOW_SECONDS = 120;
    static final int HOTSPOT_SLIDE_SECONDS = 30;
    static final int HOTSPOT_TOP_N = 5;
    static final double HOTSPOT_GRID_DEGREES = 1.0;

    private static final String INSERT_FLIGHTS_SQL =
            "INSERT INTO flights (icao24, callsign, origin_country, time_position, last_contact, " +
            "longitude, latitude, baro_altitude, on_ground, velocity, true_track, vertical_rate) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final JdbcConnectionOptions CLICKHOUSE_CONN =
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(CLICKHOUSE_URL)
                    .withDriverName(CLICKHOUSE_DRIVER)
                    .build();

    private static final JdbcExecutionOptions JDBC_OPTS =
            JdbcExecutionOptions.builder()
                    .withBatchSize(1000)
                    .withBatchIntervalMs(1000)
                    .withMaxRetries(3)
                    .build();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<FlightEvent> kafkaSource = KafkaSource.<FlightEvent>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics("raw_flight_data")
                .setGroupId("flink-opensky-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new FlightEventDeserializationSchema())
                .build();

        WatermarkStrategy<FlightEvent> watermarkStrategy = WatermarkStrategy
                .<FlightEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event.getTimestamp());

        DataStream<FlightEvent> flights = env.fromSource(
                kafkaSource, watermarkStrategy, "OpenSky Kafka Source");

        // 1. Raw events → ClickHouse flights table.
        flights.addSink(JdbcSink.sink(
                INSERT_FLIGHTS_SQL,
                (ps, e) -> {
                    ps.setString(1, e.getIcao24());
                    setNullableString(ps, 2, e.getCallsign());
                    ps.setString(3, e.getOriginCountry() != null ? e.getOriginCountry() : "");
                    ps.setLong(4, e.getTimePosition() != null ? e.getTimePosition() : 0L);
                    ps.setLong(5, e.getLastContact() != null ? e.getLastContact() : 0L);
                    setNullableDouble(ps, 6, e.getLongitude());
                    setNullableDouble(ps, 7, e.getLatitude());
                    setNullableDouble(ps, 8, e.getBaroAltitude());
                    ps.setBoolean(9, Boolean.TRUE.equals(e.getOnGround()));
                    setNullableDouble(ps, 10, e.getVelocity());
                    setNullableDouble(ps, 11, e.getTrueTrack());
                    setNullableDouble(ps, 12, e.getVerticalRate());
                },
                JDBC_OPTS, CLICKHOUSE_CONN));

        // 2. Climb/descent alerts — emit when a single aircraft's computed vertical
        //    rate exceeds the threshold between consecutive observations.
        flights.keyBy(FlightEvent::getIcao24)
                .process(new VerticalRateAlertFunction())
                .print("ALERT");

        // 3. Tumbling 60-second window: flights seen per country.
        flights.keyBy(e -> e.getOriginCountry() != null ? e.getOriginCountry() : "Unknown")
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .aggregate(new CountAggregate())
                .map(t -> String.format("country=%s count=%d", t.f0, t.f1))
                .print("COUNTRY-WINDOW");

        // 4. Sliding spatial hotspots: top-N busiest geo-cells over event-time windows.
        flights.filter(OpenSkyVectorStreamJob::isAirborneWithCoordinates)
                .keyBy(e -> toSpatialCell(
                        e.getLatitude(),
                        e.getLongitude(),
                        HOTSPOT_GRID_DEGREES))
                .window(SlidingEventTimeWindows.of(
                        Time.seconds(HOTSPOT_WINDOW_SECONDS),
                        Time.seconds(HOTSPOT_SLIDE_SECONDS)))
                .aggregate(new CellCountAggregate(), new CellCountWindowFunction())
                .keyBy(CellWindowCount::getWindowEnd)
                .process(new TopNHotspotsFunction(HOTSPOT_TOP_N))
                .print("HOTSPOT-WINDOW");

        env.execute("flink-opensky-vector-stream");
    }

    // ── Climb / descent alert per aircraft ────────────────────────────────────

    public static class VerticalRateAlertFunction
            extends KeyedProcessFunction<String, FlightEvent, String> {

        private transient ValueState<FlightEvent> previousState;

        @Override
        public void open(Configuration parameters) {
            previousState = getRuntimeContext().getState(previousEventStateDescriptor());
        }

        @Override
        public void processElement(FlightEvent current, Context ctx, Collector<String> out)
                throws Exception {
            FlightEvent previous = previousState.value();
            if (previous != null) {
                Double rate = calculateVerticalRateMetersPerSecond(previous, current);
                if (rate != null && Math.abs(rate) >= ALERT_VERTICAL_RATE_MS) {
                    String direction = rate > 0 ? "CLIMBING" : "DESCENDING";
                    out.collect(String.format("%s icao24=%s callsign=%s rate=%.1f m/s alt=%.0f m",
                            direction,
                            current.getIcao24(),
                            current.getCallsign() != null ? current.getCallsign() : "?",
                            rate,
                            current.getBaroAltitude() != null ? current.getBaroAltitude() : 0.0));
                }
            }
            previousState.update(current);
        }
    }

    static ValueStateDescriptor<FlightEvent> previousEventStateDescriptor() {
        ValueStateDescriptor<FlightEvent> descriptor =
                new ValueStateDescriptor<>("prev-event", FlightEvent.class);
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(org.apache.flink.api.common.time.Time.minutes(ALERT_STATE_TTL_MINUTES))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();
        descriptor.enableTimeToLive(ttlConfig);
        return descriptor;
    }

    // ── 60-second tumbling window: count flights per country ──────────────────

    private static class CountAggregate
            implements AggregateFunction<FlightEvent, Tuple2<String, Long>, Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> createAccumulator() {
            return Tuple2.of("", 0L);
        }

        @Override
        public Tuple2<String, Long> add(FlightEvent e, Tuple2<String, Long> acc) {
            return Tuple2.of(e.getOriginCountry() != null ? e.getOriginCountry() : "Unknown",
                    acc.f1 + 1);
        }

        @Override
        public Tuple2<String, Long> getResult(Tuple2<String, Long> acc) {
            return acc;
        }

        @Override
        public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
            return Tuple2.of(a.f0, a.f1 + b.f1);
        }
    }

    // ── Sliding hotspots: top-N spatial cells by aircraft density ─────────────

    static class CellWindowCount implements Serializable {
        private long windowEnd;
        private String cell;
        private long count;

        CellWindowCount() {
        }

        CellWindowCount(long windowEnd, String cell, long count) {
            this.windowEnd = windowEnd;
            this.cell = cell;
            this.count = count;
        }

        long getWindowEnd() {
            return windowEnd;
        }

        String getCell() {
            return cell;
        }

        long getCount() {
            return count;
        }
    }

    private static class CellCountAggregate implements AggregateFunction<FlightEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(FlightEvent value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class CellCountWindowFunction
            extends ProcessWindowFunction<Long, CellWindowCount, String, TimeWindow> {
        @Override
        public void process(
                String cell,
                Context context,
                Iterable<Long> counts,
                Collector<CellWindowCount> out) {
            out.collect(new CellWindowCount(context.window().getEnd(), cell, counts.iterator().next()));
        }
    }

    private static class TopNHotspotsFunction
            extends KeyedProcessFunction<Long, CellWindowCount, String> {
        private final int topN;
        private transient ListState<CellWindowCount> countsState;

        TopNHotspotsFunction(int topN) {
            this.topN = topN;
        }

        @Override
        public void open(Configuration parameters) {
            countsState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("cell-window-counts", CellWindowCount.class));
        }

        @Override
        public void processElement(
                CellWindowCount value,
                Context ctx,
                Collector<String> out) throws Exception {
            countsState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<String> out) throws Exception {
            List<CellWindowCount> counts = new ArrayList<>();
            for (CellWindowCount item : countsState.get()) {
                counts.add(item);
            }
            out.collect(formatTopHotspots(timestamp - 1, counts, topN));
            countsState.clear();
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    static Double calculateVerticalRateMetersPerSecond(FlightEvent previous, FlightEvent current) {
        if (previous.getBaroAltitude() == null || current.getBaroAltitude() == null) return null;
        long deltaMs = current.getTimestamp() - previous.getTimestamp();
        if (deltaMs <= 0) return null;
        return (current.getBaroAltitude() - previous.getBaroAltitude()) / (deltaMs / 1000.0);
    }

    static boolean isAirborneWithCoordinates(FlightEvent event) {
        return !Boolean.TRUE.equals(event.getOnGround())
                && event.getLatitude() != null
                && event.getLongitude() != null;
    }

    static String toSpatialCell(double latitude, double longitude, double gridDegrees) {
        double latStart = Math.floor(latitude / gridDegrees) * gridDegrees;
        double lonStart = Math.floor(longitude / gridDegrees) * gridDegrees;
        return String.format(
                Locale.ROOT,
                "lat=%.1f..%.1f,lon=%.1f..%.1f",
                latStart,
                latStart + gridDegrees,
                lonStart,
                lonStart + gridDegrees);
    }

    static String formatTopHotspots(long windowEnd, List<CellWindowCount> counts, int topN) {
        counts.sort(Comparator
                .comparingLong(CellWindowCount::getCount).reversed()
                .thenComparing(CellWindowCount::getCell));
        int limit = Math.min(topN, counts.size());
        StringBuilder sb = new StringBuilder();
        sb.append("window_end=").append(windowEnd).append(" top_cells=[");
        for (int i = 0; i < limit; i++) {
            CellWindowCount item = counts.get(i);
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(i + 1)
                    .append(":")
                    .append(item.getCell())
                    .append("=")
                    .append(item.getCount());
        }
        sb.append("]");
        return sb.toString();
    }

    private static void setNullableString(java.sql.PreparedStatement ps, int i, String v)
            throws java.sql.SQLException {
        if (v != null) ps.setString(i, v); else ps.setNull(i, Types.VARCHAR);
    }

    private static void setNullableDouble(java.sql.PreparedStatement ps, int i, Double v)
            throws java.sql.SQLException {
        if (v != null) ps.setDouble(i, v); else ps.setNull(i, Types.DOUBLE);
    }
}
