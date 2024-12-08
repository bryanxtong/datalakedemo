package org.example.kafka.confluent.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
public class KafkaGenericJsonSchemaDeserializer extends KafkaJsonSchemaDeserializer<JsonNode> {

    @Override
    public JsonNode deserialize(String topic, byte[] data) {
        return objectMapper().convertValue(super.deserialize(topic, data), new TypeReference<>() {
        });
    }
}
