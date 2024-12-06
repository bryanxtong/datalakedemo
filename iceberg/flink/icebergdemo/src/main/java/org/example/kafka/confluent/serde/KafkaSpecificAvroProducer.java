package org.example.kafka.confluent.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.Utils;
import org.example.model.avro.StockTicks;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public class KafkaSpecificAvroProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException, IOException, URISyntaxException {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:18081");
        kafkaProps.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        Producer<String, StockTicks> producer = new KafkaProducer<>(kafkaProps);
        //read json lines
        List<String> lines = Utils.readJsonLines("batch_1.json");
        ObjectMapper objectMapper = new ObjectMapper();
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
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>("StockTicksSpecificAvro", String.valueOf(stockTicksAvro.getKey()), stockTicksAvro), (recordMetadata, e) -> {
                if (null == e) {
                    System.out.println(recordMetadata.topic() + " " + recordMetadata.partition());
                } else {
                    e.printStackTrace();
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
