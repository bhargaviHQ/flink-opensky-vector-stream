package com.project.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;

/**
 * Data contract for a single OpenSky ADS-B flight state event.
 * Field names mirror the JSON keys produced by the Python producer so that
 * Jackson can deserialize directly without any custom configuration.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlightEvent implements Serializable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonProperty("icao24")
    private String icao24;

    @JsonProperty("callsign")
    private String callsign;

    @JsonProperty("origin_country")
    private String originCountry;

    @JsonProperty("time_position")
    private Long timePosition;

    @JsonProperty("last_contact")
    private Long lastContact;

    @JsonProperty("longitude")
    private Double longitude;

    @JsonProperty("latitude")
    private Double latitude;

    @JsonProperty("baro_altitude")
    private Double baroAltitude;

    @JsonProperty("on_ground")
    private Boolean onGround;

    @JsonProperty("velocity")
    private Double velocity;

    @JsonProperty("true_track")
    private Double trueTrack;

    @JsonProperty("vertical_rate")
    private Double verticalRate;

    public FlightEvent() {
    }

    /**
     * Deserializes a JSON string into a {@code FlightEvent}.
     *
     * @param json the JSON string to parse
     * @return the deserialized {@code FlightEvent}
     * @throws IOException if the JSON cannot be parsed
     */
    public static FlightEvent fromJson(String json) throws IOException {
        return MAPPER.readValue(json, FlightEvent.class);
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

    public String getOriginCountry() {
        return originCountry;
    }

    public void setOriginCountry(String originCountry) {
        this.originCountry = originCountry;
    }

    public Long getTimePosition() {
        return timePosition;
    }

    public void setTimePosition(Long timePosition) {
        this.timePosition = timePosition;
    }

    public Long getLastContact() {
        return lastContact;
    }

    public void setLastContact(Long lastContact) {
        this.lastContact = lastContact;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getBaroAltitude() {
        return baroAltitude;
    }

    public void setBaroAltitude(Double baroAltitude) {
        this.baroAltitude = baroAltitude;
    }

    public Boolean getOnGround() {
        return onGround;
    }

    public void setOnGround(Boolean onGround) {
        this.onGround = onGround;
    }

    public Double getVelocity() {
        return velocity;
    }

    public void setVelocity(Double velocity) {
        this.velocity = velocity;
    }

    public Double getTrueTrack() {
        return trueTrack;
    }

    public void setTrueTrack(Double trueTrack) {
        this.trueTrack = trueTrack;
    }

    public Double getVerticalRate() {
        return verticalRate;
    }

    public void setVerticalRate(Double verticalRate) {
        this.verticalRate = verticalRate;
    }

    /** Returns event time in epoch milliseconds for Flink watermarks. */
    public long getTimestamp() {
        return lastContact != null ? lastContact * 1000L : 0L;
    }
}
