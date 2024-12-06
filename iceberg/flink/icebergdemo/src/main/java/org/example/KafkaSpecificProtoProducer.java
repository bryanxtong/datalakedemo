package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.model.protobuf.StockTicksProto;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

@Deprecated
public class KafkaSpecificProtoProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException, IOException, URISyntaxException {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:18081");
        kafkaProps.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        Producer<String, StockTicksProto.StockTicks> producer = new KafkaProducer<>(kafkaProps);
        //read json lines
        List<String> lines = Utils.readJsonLines("batch_1.json");
        for (String line : lines) {
            ObjectMapper objectMapper = new ObjectMapper();
            org.example.model.json.StockTicks stockTicksJson = objectMapper.readValue(line, org.example.model.json.StockTicks.class);
            StockTicksProto.StockTicks message = StockTicksProto.StockTicks.newBuilder()
                    .setVolume(stockTicksJson.getVolume())
                    .setSymbol(stockTicksJson.getSymbol())
                    .setTs(stockTicksJson.getTs())
                    .setMonth(stockTicksJson.getMonth())
                    .setHigh(stockTicksJson.getHigh())
                    .setLow(stockTicksJson.getLow())
                    .setKey(stockTicksJson.getKey())
                    .setYear(stockTicksJson.getYear())
                    .setDate(stockTicksJson.getDate())
                    .setClose(stockTicksJson.getClose())
                    .setOpen(stockTicksJson.getOpen())
                    .setDay(stockTicksJson.getDay())
                    .build();
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>("StockTicksSpecificProtoBuf", message), (recordMetadata, e) -> {
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
