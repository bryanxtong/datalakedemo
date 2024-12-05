package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.model.avro.StockTicks;
import java.util.ArrayList;
import java.util.List;

/**
 * Write Avro records into kafka with Flink-Avro library
 */
public class KafkaSpecificAvroProducerWithFlinkAvro {

    public static void main(String[] args) throws Exception {
        KafkaSink<StockTicks> sink = KafkaSink.<StockTicks>builder()
                .setBootstrapServers("localhost:19092,localhost:29092,localhost:39092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("StockTicksSpecificAvro")
                        .setValueSerializationSchema(AvroSerializationSchema.forSpecific(StockTicks.class))
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<String> lines = Utils.readJsonLines("batch_1.json");
        ObjectMapper objectMapper = new ObjectMapper();
        List<StockTicks> stockTicksList = new ArrayList<>();
        for (String line : lines) {
            org.example.model.json.StockTicks stockTicksJson = objectMapper.readValue(line, org.example.model.json.StockTicks.class);
            StockTicks stockTicksAvro = new StockTicks();
            stockTicksAvro.put("volume", stockTicksJson.getVolume());
            stockTicksAvro.put("symbol", stockTicksJson.getSymbol());
            stockTicksAvro.put("ts", stockTicksJson.getTs());
            stockTicksAvro.put("month", stockTicksJson.getMonth());
            stockTicksAvro.put("high", stockTicksJson.getHigh());
            stockTicksAvro.put("low", stockTicksJson.getLow());
            stockTicksAvro.put("key", stockTicksJson.getKey());
            stockTicksAvro.put("year", stockTicksJson.getYear());
            stockTicksAvro.put("date", stockTicksJson.getDate());
            stockTicksAvro.put("close", stockTicksJson.getClose());
            stockTicksAvro.put("open", stockTicksJson.getOpen());
            stockTicksAvro.put("day", stockTicksJson.getDay());
            stockTicksList.add(stockTicksAvro);
        }
        DataStreamSource<StockTicks> dataStreamSource = env.fromData(stockTicksList);
        dataStreamSource.sinkTo(sink);
        env.execute();
    }
}
