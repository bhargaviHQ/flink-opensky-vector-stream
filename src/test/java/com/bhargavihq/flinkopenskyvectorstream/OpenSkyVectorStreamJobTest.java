package com.bhargavihq.flinkopenskyvectorstream;

import com.project.model.FlightEvent;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenSkyVectorStreamJobTest {

    private static FlightEvent event(double baroAltitude, long lastContactSeconds) {
        FlightEvent e = new FlightEvent();
        e.setIcao24("abc123");
        e.setCallsign("CALL1");
        e.setBaroAltitude(baroAltitude);
        e.setLastContact(lastContactSeconds);
        return e;
    }

    @Test
    void calculatesVerticalRateForValidDeltaTime() {
        // lastContact 1s and 4s → timestamps 1000ms and 4000ms → deltaTime 3s
        // deltaAltitude = 1150 - 1000 = 150m → rate = 50 m/s
        Double rate = OpenSkyVectorStreamJob.calculateVerticalRateMetersPerSecond(
                event(1000.0, 1L), event(1150.0, 4L));
        assertEquals(50.0, rate);
    }

    @Test
    void returnsNullWhenDeltaTimeIsZero() {
        Double rate = OpenSkyVectorStreamJob.calculateVerticalRateMetersPerSecond(
                event(1000.0, 1L), event(1150.0, 1L));
        assertNull(rate);
    }

    @Test
    void returnsNullWhenDeltaTimeIsNegative() {
        Double rate = OpenSkyVectorStreamJob.calculateVerticalRateMetersPerSecond(
                event(1000.0, 5L), event(1150.0, 4L));
        assertNull(rate);
    }

    @Test
    void returnsNullWhenBaroAltitudeIsNull() {
        FlightEvent prev = event(1000.0, 1L);
        FlightEvent curr = new FlightEvent();
        curr.setLastContact(4L);
        // baroAltitude left null
        assertNull(OpenSkyVectorStreamJob.calculateVerticalRateMetersPerSecond(prev, curr));
    }

    @Test
    void alertStateDescriptorHasTtlEnabled() {
        ValueStateDescriptor<FlightEvent> descriptor =
                OpenSkyVectorStreamJob.previousEventStateDescriptor();
        StateTtlConfig ttlConfig = descriptor.getTtlConfig();
        assertTrue(ttlConfig.isEnabled());
        assertEquals(OpenSkyVectorStreamJob.ALERT_STATE_TTL_MINUTES * 60_000L,
                ttlConfig.getTimeToLive().toMillis());
    }

    @Test
    void marksEventAsAirborneWithCoordinatesOnlyWhenValid() {
        FlightEvent valid = event(1000.0, 10L);
        valid.setLatitude(41.8);
        valid.setLongitude(-87.6);
        valid.setOnGround(false);

        FlightEvent missingCoordinates = event(1000.0, 10L);
        missingCoordinates.setOnGround(false);

        FlightEvent onGround = event(1000.0, 10L);
        onGround.setLatitude(41.8);
        onGround.setLongitude(-87.6);
        onGround.setOnGround(true);

        assertTrue(OpenSkyVectorStreamJob.isAirborneWithCoordinates(valid));
        assertTrue(!OpenSkyVectorStreamJob.isAirborneWithCoordinates(missingCoordinates));
        assertTrue(!OpenSkyVectorStreamJob.isAirborneWithCoordinates(onGround));
    }

    @Test
    void mapsCoordinatesIntoStableSpatialCellBuckets() {
        assertEquals(
                "lat=41.0..42.0,lon=-88.0..-87.0",
                OpenSkyVectorStreamJob.toSpatialCell(41.8781, -87.6298, 1.0));

        assertEquals(
                "lat=-34.0..-33.0,lon=151.0..152.0",
                OpenSkyVectorStreamJob.toSpatialCell(-33.8688, 151.2093, 1.0));
    }

    @Test
    void formatsTopHotspotsInDescendingCountOrder() {
        String summary = OpenSkyVectorStreamJob.formatTopHotspots(
                120_000L,
                Arrays.asList(
                        new OpenSkyVectorStreamJob.CellWindowCount(120_000L, "cell-c", 3),
                        new OpenSkyVectorStreamJob.CellWindowCount(120_000L, "cell-a", 7),
                        new OpenSkyVectorStreamJob.CellWindowCount(120_000L, "cell-b", 7)),
                2);

        assertEquals("window_end=120000 top_cells=[1:cell-a=7, 2:cell-b=7]", summary);
    }
}
