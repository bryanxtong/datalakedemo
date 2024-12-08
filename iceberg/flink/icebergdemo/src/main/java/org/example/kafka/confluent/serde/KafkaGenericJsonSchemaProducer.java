package org.example.kafka.confluent.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.model.json.StockTicksWithSchema;
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
 * Use confluent json to write json generic records into kafka
 */
public class KafkaGenericJsonSchemaProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException, IOException, URISyntaxException {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put("schema.registry.url", "http://localhost:18081");
        kafkaProps.put("json.fail.invalid.schema", true);
        Producer<String, JsonNode> producer = new KafkaProducer<>(kafkaProps);
        List<String> lines = readJsonLines();

        ObjectMapper objectMapper = new ObjectMapper();
        for (String jsonLine : lines) {
            StockTicksWithSchema stockTicks = objectMapper.readValue(jsonLine, StockTicksWithSchema.class);
            JsonNode jsonNode = objectMapper.valueToTree(stockTicks);
            JsonNode jsonNodeSchema = objectMapper.readTree(StockTicksWithSchema.SCHEMA_AS_STRING);
            JsonSchema jsonSchema = new JsonSchema(jsonNodeSchema);
            ObjectNode envelope = JsonSchemaUtils.envelope(jsonSchema, jsonNode);
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>("StockTicksJsonSchemaGeneric", envelope), (recordMetadata, e) -> {
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
        URL resource = KafkaGenericJsonSchemaProducer.class.getClassLoader().getResource("data/batch_1.json");
        return Files.readAllLines(Paths.get(resource.toURI()));
    }
}
