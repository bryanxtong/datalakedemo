package org.example.kafka.confluent.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.Utils;
import org.example.model.json.StockTicks;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 * Send Avro Generic data
 */
public class KafkaGenericAvroProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException, IOException, URISyntaxException {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:18081");
        kafkaProps.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        Producer<String, GenericRecord> producer = new KafkaProducer<>(kafkaProps);
        Schema schema = Utils.getAvroSchema("StockTicks.avsc");
        List<String> lines = Utils.readJsonLines("batch_1.json");
        ObjectMapper objectMapper = new ObjectMapper();
        for (String line : lines) {
            //read data as json and convert into avro for testing
            StockTicks stockTicks = objectMapper.readValue(line, StockTicks.class);
            GenericRecord genericRecord = new GenericData.Record(schema);
            genericRecord.put("volume", stockTicks.getVolume());
            genericRecord.put("symbol", stockTicks.getSymbol());
            genericRecord.put("ts", stockTicks.getTs());
            genericRecord.put("month", stockTicks.getMonth());
            genericRecord.put("high", stockTicks.getHigh());
            genericRecord.put("low", stockTicks.getLow());
            genericRecord.put("key", stockTicks.getKey());
            genericRecord.put("year", stockTicks.getYear());
            genericRecord.put("date", stockTicks.getDate());
            genericRecord.put("close", stockTicks.getClose());
            genericRecord.put("open", stockTicks.getOpen());
            genericRecord.put("day", stockTicks.getDay());
            ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("StockTicksGenericAvro", genericRecord.hasField("key") ? String.valueOf(genericRecord.get("key")) : null, genericRecord);
            Future<RecordMetadata> future = producer.send(record, (recordMetadata, e) -> {
                if (null == e) {
                    System.out.println("topic: " + recordMetadata.topic() + " partition: " + recordMetadata.partition());
                } else {
                    e.printStackTrace();
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
