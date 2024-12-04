package org.example;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import org.example.model.protobuf.StockTicksProto;

public class KafkaStockTicksProtobufDeserializer extends KafkaProtobufDeserializer<StockTicksProto.StockTicks> {
}
