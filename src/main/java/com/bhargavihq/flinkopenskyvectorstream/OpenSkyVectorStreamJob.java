package com.bhargavihq.flinkopenskyvectorstream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;

public class OpenSkyVectorStreamJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<FlightEvent> watermarkStrategy = WatermarkStrategy
                .<FlightEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, previousTimestamp) -> event.getTimestamp());

        // Placeholder for Kafka source integration.
        // KafkaSource<FlightEvent> kafkaSource = createKafkaSourcePlaceholder();
        // DataStream<FlightEvent> flights = env.fromSource(kafkaSource, watermarkStrategy, "OpenSky Kafka Source");

        DataStream<FlightEvent> flights = env
                .fromCollection(Collections.<FlightEvent>emptyList())
                .assignTimestampsAndWatermarks(watermarkStrategy);

        flights
                .keyBy(FlightEvent::getIcao24)
                .process(new VerticalRateProcessFunction())
                .print();

        env.execute("flink-opensky-vector-stream");
    }

    private static KafkaSource<FlightEvent> createKafkaSourcePlaceholder() {
        throw new UnsupportedOperationException("Configure KafkaSource<FlightEvent> for OpenSky topic");
    }

    public static class VerticalRateProcessFunction extends KeyedProcessFunction<String, FlightEvent, String> {
        private transient ValueState<FlightEvent> previousFlightEventState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<FlightEvent> descriptor =
                    new ValueStateDescriptor<>("previous-flight-event", FlightEvent.class);
            previousFlightEventState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(FlightEvent current, Context ctx, Collector<String> out) throws Exception {
            FlightEvent previous = previousFlightEventState.value();
            if (previous != null) {
                long deltaTimeMillis = current.getTimestamp() - previous.getTimestamp();
                if (deltaTimeMillis > 0) {
                    double deltaAltitude = current.getBaroAltitude() - previous.getBaroAltitude();
                    double verticalRateMetersPerSecond = deltaAltitude / (deltaTimeMillis / 1000.0d);
                    out.collect(String.format(
                            "icao24=%s, callsign=%s, verticalRate=%.3f m/s",
                            current.getIcao24(),
                            current.getCallsign(),
                            verticalRateMetersPerSecond
                    ));
                }
            }
            previousFlightEventState.update(current);
        }
    }
}
