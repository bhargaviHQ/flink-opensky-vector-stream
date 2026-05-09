package com.bhargavihq.flinkopenskyvectorstream;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class OpenSkyVectorStreamJobTest {

    @Test
    void calculatesVerticalRateForValidDeltaTime() {
        FlightEvent previous = new FlightEvent("abc123", "CALL1", 0.0, 0.0, 1000.0, 200.0, 1_000L);
        FlightEvent current = new FlightEvent("abc123", "CALL1", 0.1, 0.1, 1150.0, 205.0, 4_000L);

        Double verticalRate = OpenSkyVectorStreamJob.calculateVerticalRateMetersPerSecond(previous, current);

        assertEquals(50.0, verticalRate);
    }

    @Test
    void returnsNullWhenDeltaTimeIsZero() {
        FlightEvent previous = new FlightEvent("abc123", "CALL1", 0.0, 0.0, 1000.0, 200.0, 1_000L);
        FlightEvent current = new FlightEvent("abc123", "CALL1", 0.1, 0.1, 1150.0, 205.0, 1_000L);

        Double verticalRate = OpenSkyVectorStreamJob.calculateVerticalRateMetersPerSecond(previous, current);

        assertNull(verticalRate);
    }

    @Test
    void returnsNullWhenDeltaTimeIsNegative() {
        FlightEvent previous = new FlightEvent("abc123", "CALL1", 0.0, 0.0, 1000.0, 200.0, 5_000L);
        FlightEvent current = new FlightEvent("abc123", "CALL1", 0.1, 0.1, 1150.0, 205.0, 4_000L);

        Double verticalRate = OpenSkyVectorStreamJob.calculateVerticalRateMetersPerSecond(previous, current);

        assertNull(verticalRate);
    }
}
