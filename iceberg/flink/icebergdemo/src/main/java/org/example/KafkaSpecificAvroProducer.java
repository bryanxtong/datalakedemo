package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
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
        kafkaProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"http://localhost:18081");
        kafkaProps.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        Producer<String, StockTicks> producer = new KafkaProducer<>(kafkaProps);
        //read json lines
        List<String> lines = Utils.readJsonLines("batch_1.json");
        for (String line : lines) {
            ObjectMapper objectMapper = new ObjectMapper();
            StockTicks stockTicks = objectMapper.readValue(line, StockTicks.class);
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>("StockTicksSpecificAvro", String.valueOf(stockTicks.getKey()), stockTicks), (recordMetadata, e) -> {
                if(null == e){
                    System.out.println(recordMetadata.topic() +" "+ recordMetadata.partition());
                }else{
                    e.printStackTrace();
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
