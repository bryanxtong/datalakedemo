package org.example.kafka.confluent.serde;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import org.example.model.json.StockTicksWithSchema;

public class KafkaStockticksJsonSchemaDeserializer extends KafkaJsonSchemaDeserializer<StockTicksWithSchema>{

}
