package org.example.kafka.confluent.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
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
 * Use confluent json serializer to write json generic records into kafka(Pure Json)
 */
public class KafkaGenericJsonProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException, IOException, URISyntaxException {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        Producer<String, JsonNode> producer = new KafkaProducer<>(kafkaProps);
        List<String> lines = readJsonLines();

        ObjectMapper objectMapper = new ObjectMapper();
        for (String jsonLine : lines) {
            StockTicks stockTicks = objectMapper.readValue(jsonLine, StockTicks.class);
            JsonNode jsonNode = objectMapper.valueToTree(stockTicks);
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>("StockTicksJsonGeneric", jsonNode), (recordMetadata, e) -> {
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
    public static List<String> readJsonLines() throws IOException, URISyntaxException {
        URL resource = KafkaGenericJsonProducer.class.getClassLoader().getResource("data/batch_1.json");
        return Files.readAllLines(Paths.get(resource.toURI()));
    }
}
