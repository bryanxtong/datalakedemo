package org.example;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * Read Json byte as java Object using confluent KafkaJsonDeserializer
 */
public class KafkaDataStreamJsonRead {
    public static void main(String[] args) throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, StockTicks.class.getName());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<StockTicks> source = KafkaSource.<StockTicks>builder()
                .setBootstrapServers("localhost:19092")
                .setTopics("stock_ticks")
                .setGroupId("stock_sticks_client")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(KafkaStockticksJsonDeserializer.class, config))
                .build();

        DataStreamSource<StockTicks> kafka_source = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        kafka_source.map((MapFunction<StockTicks, StockTicks>) value -> {
            System.out.println(value);
            return value;
        }).print();
        env.execute();
    }
}
