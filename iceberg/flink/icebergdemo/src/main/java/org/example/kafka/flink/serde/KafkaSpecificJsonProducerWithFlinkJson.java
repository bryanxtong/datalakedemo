package org.example.kafka.flink.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.Utils;
import org.example.model.json.StockTicks;

import java.util.ArrayList;
import java.util.List;

/**
 * Use Flink-Json to write json specific records(eg StockTicks) into kafka
 */
public class KafkaSpecificJsonProducerWithFlinkJson {

    public static void main(String[] args) throws Exception {
        KafkaSink<StockTicks> sink = KafkaSink.<StockTicks>builder()
                .setBootstrapServers("localhost:19092,localhost:29092,localhost:39092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("StockTicksJsonSpecific")
                        .setValueSerializationSchema(new JsonSerializationSchema<StockTicks>())
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
            stockTicksList.add(stockTicksJson);
        }
        DataStreamSource<StockTicks> dataStreamSource = env.fromData(stockTicksList);
        dataStreamSource.print("json records...");
        dataStreamSource.sinkTo(sink);
        env.execute();
    }
}
