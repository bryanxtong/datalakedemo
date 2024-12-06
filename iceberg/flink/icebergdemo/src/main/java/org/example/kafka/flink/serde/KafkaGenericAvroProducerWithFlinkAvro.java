package org.example.kafka.flink.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.Utils;
import java.util.ArrayList;
import java.util.List;

/**
 * Use Flink-Avro to serialize avro generic records into kafka
 */
public class KafkaGenericAvroProducerWithFlinkAvro {

    public static void main(String[] args) throws Exception {
        Schema avroSchema = Utils.getAvroSchema("StockTicks.avsc");
        KafkaSink<GenericRecord> sink = KafkaSink.<GenericRecord>builder()
                .setBootstrapServers("localhost:19092,localhost:29092,localhost:39092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("StockTicksGenericAvro")
                        .setValueSerializationSchema(AvroSerializationSchema.forGeneric(avroSchema))
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //enable the avro generic record serialization
         /*env.getConfig().getSerializerConfig().registerTypeWithKryoSerializer(
                GenericData.Array.class,
                Serializers.SpecificInstanceCollectionSerializerForArrayList.class);
        env.getConfig().getSerializerConfig().addDefaultKryoSerializer(Schema.class, AvroKryoSerializerUtils.AvroSchemaSerializer.class);*/
        AvroKryoSerializerUtils.getAvroUtils().addAvroSerializersIfRequired(env.getConfig().getSerializerConfig(), GenericData.Record.class);

        setCheckpoint(env);
        List<String> lines = Utils.readJsonLines("batch_1.json");
        ObjectMapper objectMapper = new ObjectMapper();
        List<GenericRecord> genericRecords = new ArrayList<>();
        for (String line : lines) {
            //load json line and converted into generic record
            org.example.model.json.StockTicks stockTicksJson = objectMapper.readValue(line, org.example.model.json.StockTicks.class);
            GenericRecord genericRecord = new GenericData.Record(avroSchema);
            genericRecord.put("volume", stockTicksJson.getVolume());
            genericRecord.put("symbol", stockTicksJson.getSymbol());
            genericRecord.put("ts", stockTicksJson.getTs());
            genericRecord.put("month", stockTicksJson.getMonth());
            genericRecord.put("high", stockTicksJson.getHigh());
            genericRecord.put("low", stockTicksJson.getLow());
            genericRecord.put("key", stockTicksJson.getKey());
            genericRecord.put("year", stockTicksJson.getYear());
            genericRecord.put("date", stockTicksJson.getDate());
            genericRecord.put("close", stockTicksJson.getClose());
            genericRecord.put("open", stockTicksJson.getOpen());
            genericRecord.put("day", stockTicksJson.getDay());
            genericRecords.add(genericRecord);
        }
        DataStreamSource<GenericRecord> dataStreamSource = env.fromData(genericRecords);
        //dataStreamSource.print("avro generic source data");
        dataStreamSource.sinkTo(sink);
        env.execute();
    }

    public static void setCheckpoint(StreamExecutionEnvironment env) throws ClassNotFoundException {
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///c://flink/checkpoint");
        env.configure(config);
    }
}
