package org.example;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.model.avro.StockTicks;

/**
 * using Flink Avro to deserialize Avro specific records and write to iceberg tables of hadoop/hive catalog
 * and use the KafkaSpecificAvroProducerWithFlinkAvro.java to send records into kafka for testing and then run the class below
 * <p>
 * <p>
 */
public class KafkaDataStreamSpecificAvroRecordRead2IcebergTablesWithFlinkAvro {
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
    public KafkaSource<StockTicks> buildKafkaSource(String kafkaBootStrapServers, String[] topics, String groupId) {
        KafkaSource<StockTicks> kafkaSource = KafkaSource.<StockTicks>builder()
                .setBootstrapServers(kafkaBootStrapServers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(AvroDeserializationSchema.forSpecific(StockTicks.class))
                .build();
        return kafkaSource;
    }

    /**
     * transform to generic record for writing only using FlinkDataStreamAvroWritesWithSchema and no conversion for field one by one
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
        KafkaSource<StockTicks> source = this.buildKafkaSource(kafkaBootStrapServers, topics, groupId);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map((MapFunction<StockTicks, GenericRecord>) value-> value);
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "bryan");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        KafkaDataStreamSpecificAvroRecordRead2IcebergTablesWithFlinkAvro write2Tables = new KafkaDataStreamSpecificAvroRecordRead2IcebergTablesWithFlinkAvro();
        DataStream<GenericRecord> kafkaSource = write2Tables.createDataStreamSource(env, "localhost:19092", new String[]{"StockTicksSpecificAvro"}, "stock_sticks_client");
        FlinkDataStreamAvroWritesWithSchema flinkWritesJob = new FlinkDataStreamAvroWritesWithSchema(Utils.CatalogType.HADOOP, "hadoop_catalog", "default", "stock_ticks");
        flinkWritesJob.writeAvroRecords(kafkaSource, FlinkDataStreamAvroWritesWithSchema.getAvroSchema("StockTicks.avsc"));
        FlinkDataStreamAvroWritesWithSchema flinkWritesJob2 = new FlinkDataStreamAvroWritesWithSchema(Utils.CatalogType.HIVE, "hive_catalog", "hive_db", "stock_ticks");
        flinkWritesJob2.writeAvroRecords(kafkaSource, FlinkDataStreamAvroWritesWithSchema.getAvroSchema("StockTicks.avsc"));
        env.execute("Writes Json data with schema in schema registry into iceberg tables");
    }
}