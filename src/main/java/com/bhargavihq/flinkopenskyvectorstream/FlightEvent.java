package com.bhargavihq.flinkopenskyvectorstream;

import java.io.Serializable;

/**
 * OpenSky ADS-B telemetry event where altitude is in meters, velocity is in meters/second,
 * and timestamp is epoch milliseconds.
 */
public class FlightEvent implements Serializable {
    private String icao24;
    private String callsign;
    private double longitude;
    private double latitude;
    private double baroAltitude;
    private double velocity;
    private long timestamp;

    public FlightEvent() {
    }

    public FlightEvent(
            String icao24,
            String callsign,
            double longitude,
            double latitude,
            double baroAltitude,
            double velocity,
            long timestamp
    ) {
        this.icao24 = icao24;
        this.callsign = callsign;
        this.longitude = longitude;
        this.latitude = latitude;
        this.baroAltitude = baroAltitude;
        this.velocity = velocity;
        this.timestamp = timestamp;
    }

    public String getIcao24() {
        return icao24;
    }

    public void setIcao24(String icao24) {
        this.icao24 = icao24;
    }

    public String getCallsign() {
        return callsign;
    }

    public void setCallsign(String callsign) {
        this.callsign = callsign;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getBaroAltitude() {
        return baroAltitude;
    }

    public void setBaroAltitude(double baroAltitude) {
        this.baroAltitude = baroAltitude;
    }

    public double getVelocity() {
        return velocity;
    }

    public void setVelocity(double velocity) {
        this.velocity = velocity;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
