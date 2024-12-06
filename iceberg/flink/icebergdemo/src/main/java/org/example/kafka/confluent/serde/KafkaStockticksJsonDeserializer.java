package org.example.kafka.confluent.serde;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import org.example.model.json.StockTicks;

public class KafkaStockticksJsonDeserializer extends KafkaJsonDeserializer<StockTicks>{
}
