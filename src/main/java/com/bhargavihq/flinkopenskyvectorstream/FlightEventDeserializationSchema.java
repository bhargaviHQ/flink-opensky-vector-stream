package com.bhargavihq.flinkopenskyvectorstream;

import com.project.model.FlightEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FlightEventDeserializationSchema implements DeserializationSchema<FlightEvent> {

    @Override
    public FlightEvent deserialize(byte[] message) throws IOException {
        return FlightEvent.fromJson(new String(message, StandardCharsets.UTF_8));
    }

    @Override
    public boolean isEndOfStream(FlightEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<FlightEvent> getProducedType() {
        return TypeInformation.of(FlightEvent.class);
    }
}
