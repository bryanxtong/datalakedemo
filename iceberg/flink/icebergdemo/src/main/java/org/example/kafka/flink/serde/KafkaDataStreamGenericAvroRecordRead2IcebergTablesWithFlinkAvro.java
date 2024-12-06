package org.example.kafka.flink.serde;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.FlinkDataStreamAvroWritesWithSchema;
import org.example.Utils;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * using Flink Avro to deserialize avro generic records and write to iceberg tables of hadoop/hive catalog
 * and KafkaGenericAvroProducerWithFlinkAvro.java is used to send records into kafka for testing and then run the class below
 * <p>
 */
public class KafkaDataStreamGenericAvroRecordRead2IcebergTablesWithFlinkAvro {
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
     * generic records in kafka deserialize with Flink Avro
     *
     * @param kafkaBootStrapServers
     * @param topics
     * @param groupId
     * @return
     */
    public KafkaSource<GenericRecord> buildKafkaSource(String kafkaBootStrapServers, String[] topics, String groupId) throws URISyntaxException, IOException {
        KafkaSource<GenericRecord> kafkaSource = KafkaSource.<GenericRecord>builder()
                .setBootstrapServers(kafkaBootStrapServers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(AvroDeserializationSchema.<GenericRecord>forGeneric(Utils.getAvroSchema("StockTicks.avsc")))
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
    public SingleOutputStreamOperator<GenericRecord> createDataStreamSource(StreamExecutionEnvironment env, String kafkaBootStrapServers, String[] topics, String groupId) throws ClassNotFoundException, URISyntaxException, IOException {
        AvroKryoSerializerUtils.getAvroUtils().addAvroSerializersIfRequired(env.getConfig().getSerializerConfig(), GenericData.Record.class);
        //if no checkpoint, no data will write to iceberg tables
        setCheckpoint(env);
        KafkaSource<GenericRecord> source = this.buildKafkaSource(kafkaBootStrapServers, topics, groupId);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "bryan");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        KafkaDataStreamGenericAvroRecordRead2IcebergTablesWithFlinkAvro write2Tables = new KafkaDataStreamGenericAvroRecordRead2IcebergTablesWithFlinkAvro();
        DataStream<GenericRecord> kafkaSource = write2Tables.createDataStreamSource(env, "localhost:19092", new String[]{"StockTicksGenericAvro"}, "stock_sticks_client");
        FlinkDataStreamAvroWritesWithSchema flinkWritesJob = new FlinkDataStreamAvroWritesWithSchema(Utils.CatalogType.HADOOP, "hadoop_catalog", "default", "stock_ticks");
        flinkWritesJob.writeAvroRecords(kafkaSource, FlinkDataStreamAvroWritesWithSchema.getAvroSchema("StockTicks.avsc"));
        FlinkDataStreamAvroWritesWithSchema flinkWritesJob2 = new FlinkDataStreamAvroWritesWithSchema(Utils.CatalogType.HIVE, "hive_catalog", "hive_db", "stock_ticks");
        flinkWritesJob2.writeAvroRecords(kafkaSource, FlinkDataStreamAvroWritesWithSchema.getAvroSchema("StockTicks.avsc"));
        env.execute("Writes avo data with schema in schema registry into iceberg tables");
    }
}