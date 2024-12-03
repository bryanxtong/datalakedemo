package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.model.json.StockTicks;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
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
        String schemaStr = """
                                        {
                  "type": "record",
                  "name": "StockTicks",
                  "namespace": "org.example.model.avro",
                  "fields": [
                    {
                      "name": "volume",
                      "type": "long"
                    },
                    {
                      "name": "symbol",
                      "type": "string"
                    },
                    {
                      "name": "ts",
                      "type": "string"
                    },
                    {
                      "name": "month",
                      "type": "string"
                    },
                    {
                      "name": "high",
                      "type": "double"
                    },
                    {
                      "name": "low",
                      "type": "double"
                    },
                    {
                      "name": "key",
                      "type": "string"
                    },
                    {
                      "name": "year",
                      "type": "int"
                    },
                    {
                      "name": "date",
                      "type": "string"
                    },
                    {
                      "name": "close",
                      "type": "double"
                    },
                    {
                      "name": "open",
                      "type": "double"
                    },
                    {
                      "name": "day",
                      "type": "string"
                    }
                  ]
                }
                                """;
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaStr);

        List<String> lines = readJsonLines();
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
            ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("StockTicksAvro", genericRecord.hasField("key") ? String.valueOf(genericRecord.get("key")) : null, genericRecord);
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

    public static List<String> readJsonLines() throws IOException, URISyntaxException {
        URL resource = KafkaGenericAvroProducer.class.getClassLoader().getResource("data/batch_1.json");
        return Files.readAllLines(Paths.get(resource.toURI()));
    }
}
