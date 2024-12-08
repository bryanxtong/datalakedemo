package org.example.kafka.confluent.serde;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.FlinkDataStreamAvroWritesWithSchema;
import org.example.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Deserialize Avro generic records as java Object using confluent KafkaAvroDeserializer and write to iceberg tables of hadoop/hive catalog
 * and use the KafkaGenericAvroProducer to send records into kafka for testing and then run the class below(Schema registry involved)
 * <p>
 * <p>
 */
public class KafkaDataStreamGenericAvroRecordRead2IcebergTables2 {
    public static String CONFLUENT_SCHEMA_REGISTRY_URL = "http://localhost:18081";

    public void setCheckpoint(StreamExecutionEnvironment env) throws ClassNotFoundException {
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///c://flink/checkpoint");
        env.configure(config);
    }

    public KafkaSource<Object> buildKafkaSource(String kafkaBootStrapServers, String[] topics, String groupId) {
        Map<String, String> kafkaProps = new HashMap<>();
        kafkaProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:18081");

        //KafkaAvroDeserializer implements Deserializer<Object> not Deserializer<T>
        KafkaSource<Object> kafkaSource = KafkaSource.builder()
                .setBootstrapServers(kafkaBootStrapServers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(KafkaAvroDeserializer.class, kafkaProps))
                .build();
        return kafkaSource;
    }

    public DataStream<GenericRecord> createDataStreamSource(StreamExecutionEnvironment env, String kafkaBootStrapServers, String[] topics, String groupId) throws ClassNotFoundException {
        setCheckpoint(env);
        env.getConfig().getSerializerConfig().addDefaultKryoSerializer(Schema.class, AvroKryoSerializerUtils.AvroSchemaSerializer.class);
        KafkaSource<Object> source = this.buildKafkaSource(kafkaBootStrapServers, topics, groupId);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map((MapFunction<Object, GenericRecord>) value -> {
                    GenericData.Record rc = (GenericData.Record) value;
                    return rc;
                });
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "bryan");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        KafkaDataStreamGenericAvroRecordRead2IcebergTables2 write2Tables = new KafkaDataStreamGenericAvroRecordRead2IcebergTables2();
        DataStream<GenericRecord> kafkaSource = write2Tables.createDataStreamSource(env, "localhost:19092", new String[]{"StockTicksGenericAvro"}, "stock_sticks_client");
        kafkaSource.print("----------------");
        FlinkDataStreamAvroWritesWithSchema flinkWritesJob = new FlinkDataStreamAvroWritesWithSchema(Utils.CatalogType.HADOOP, "hadoop_catalog", "default", "stock_ticks");
        flinkWritesJob.writeAvroRecords(kafkaSource, Utils.getAvroSchema("StockTicks.avsc"));
        FlinkDataStreamAvroWritesWithSchema flinkWritesJob2 = new FlinkDataStreamAvroWritesWithSchema(Utils.CatalogType.HIVE, "hive_catalog", "hive_db", "stock_ticks");
        flinkWritesJob2.writeAvroRecords(kafkaSource, Utils.getAvroSchema("StockTicks.avsc"));
        env.execute("Writes Avro data with schema in schema registry into iceberg tables");
    }
}