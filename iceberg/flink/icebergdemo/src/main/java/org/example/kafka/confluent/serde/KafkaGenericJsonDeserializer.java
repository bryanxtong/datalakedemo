package org.example.kafka.confluent.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;

public class KafkaGenericJsonDeserializer extends KafkaJsonDeserializer<JsonNode> {

    /**
     * Ensure the stream record can be serializable and pushed to the next operator
     *
     * when using JsonNode,the Long value is less than the Integer.MAX_VALUE, Jackson
     * Long will change to integer, and It is different with your design if your iceberg
     * use bigint type.
     */
    public JsonNode deserialize(String topic, byte[] data) {
        return this.objectMapper().convertValue(super.deserialize(topic, data), new TypeReference<>() {});
    }
}
