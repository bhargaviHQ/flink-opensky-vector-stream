package com.bhargavihq.flinkopenskyvectorstream;

import com.project.model.FlightEvent;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;

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
}
