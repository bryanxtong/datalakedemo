package org.example.kafka.confluent.serde;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.FlinkDataStreamAvroWritesWithSchema;
import org.example.Utils;
import org.example.model.avro.StockTicks;

import java.util.HashMap;
import java.util.Map;

/**
 * Deserialize Avro specific records as java Object using confluent KafkaAvroDeserializer and write to iceberg tables of hadoop/hive catalog
 * and use the KafkaSpecificAvroProducer to send records into kafka for testing and then run the class below(Schema registry involved)
 * <p>
 * <p>
 */
public class KafkaDataStreamSpecificAvroRecordRead2IcebergTables {
    public static String CONFLUENT_SCHEMA_REGISTRY_URL = "http://localhost:18081";

    public void setCheckpoint(StreamExecutionEnvironment env) throws ClassNotFoundException {
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

    /**
     * Specfic avro records(Stockticks)
     *
     * @param kafkaBootStrapServers
     * @param topics
     * @param groupId
     * @return
     */
    public KafkaSource<Object> buildKafkaSource(String kafkaBootStrapServers, String[] topics, String groupId) {
        Map<String, String> kafkaProps = new HashMap<>();
        kafkaProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:18081");
        kafkaProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, String.valueOf(true));

        //KafkaAvroDeserializer implements Deserializer<Object> not Deserializer<T>
        KafkaSource<Object> kafkaSource = KafkaSource.<Object>builder()
                .setBootstrapServers(kafkaBootStrapServers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(KafkaAvroDeserializer.class, kafkaProps))
                .build();
        return kafkaSource;
    }

    /**
     * transform to specific avro record StockTicks
     *
     * @param env
     * @param kafkaBootStrapServers
     * @param topics
     * @param groupId
     * @return
     * @throws ClassNotFoundException
     */
    public SingleOutputStreamOperator<GenericRecord> createDataStreamSource(StreamExecutionEnvironment env, String kafkaBootStrapServers, String[] topics, String groupId) throws ClassNotFoundException {
        setCheckpoint(env);
        env.getConfig().getSerializerConfig().registerTypeWithKryoSerializer(
                GenericData.Array.class,
                Serializers.SpecificInstanceCollectionSerializerForArrayList.class);
        KafkaSource<Object> source = this.buildKafkaSource(kafkaBootStrapServers, topics, groupId);
        return env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map((MapFunction<Object, GenericRecord>) value -> {
                    StockTicks stockTicks = (StockTicks) value;
                    return stockTicks;
                });
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "bryan");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        KafkaDataStreamSpecificAvroRecordRead2IcebergTables write2Tables = new KafkaDataStreamSpecificAvroRecordRead2IcebergTables();
        DataStream<GenericRecord> kafkaSource = write2Tables.createDataStreamSource(env, "localhost:19092", new String[]{"StockTicksSpecificAvro"}, "stock_sticks_client");

        FlinkDataStreamAvroWritesWithSchema flinkWritesJob = new FlinkDataStreamAvroWritesWithSchema(Utils.CatalogType.HADOOP, "hadoop_catalog", "default", "stock_ticks");
        flinkWritesJob.writeAvroRecords(kafkaSource, FlinkDataStreamAvroWritesWithSchema.getAvroSchema("StockTicks.avsc"));
        FlinkDataStreamAvroWritesWithSchema flinkWritesJob2 = new FlinkDataStreamAvroWritesWithSchema(Utils.CatalogType.HIVE, "hive_catalog", "hive_db", "stock_ticks");
        flinkWritesJob2.writeAvroRecords(kafkaSource, FlinkDataStreamAvroWritesWithSchema.getAvroSchema("StockTicks.avsc"));
        env.execute("Writes avro data with schema in schema registry into iceberg tables");
    }
}