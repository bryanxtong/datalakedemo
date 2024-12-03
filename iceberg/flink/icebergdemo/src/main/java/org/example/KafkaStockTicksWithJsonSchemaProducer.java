package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

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
 * Produce record with json schema validation which Using @Schema annotation on the Java object
 */
public class KafkaStockTicksWithJsonSchemaProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException, IOException, URISyntaxException {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put("schema.registry.url", "http://localhost:18081");
        kafkaProps.put("json.fail.invalid.schema", true);

        Producer<String, StockTicksWithSchema> producer = new KafkaProducer<>(kafkaProps);
        List<String> lines = readJsonLines();

        ObjectMapper objectMapper = new ObjectMapper();
        for (String jsonLine : lines) {
            StockTicksWithSchema stockTicks = objectMapper.readValue(jsonLine, StockTicksWithSchema.class);
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>("stock_ticks_jsonschema", stockTicks), (recordMetadata, e) -> {
                if (null == e) {
                    System.out.println(recordMetadata.topic() + " " + recordMetadata.partition());
                } else {
                    e.printStackTrace();
                }
            });
        }
        producer.flush();
        producer.close();
        Thread.sleep(10000);
    }
    public static List<String> readJsonLines() throws IOException, URISyntaxException {
        URL resource = KafkaStockTicksWithJsonSchemaProducer.class.getClassLoader().getResource("data/batch_1.json");
        return Files.readAllLines(Paths.get(resource.toURI()));
    }
}
