package org.example;

import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.example.model.protobuf.StockTicksProto;

import java.util.HashMap;
import java.util.Map;

/**
 * Deserialize protobuf specific records as java Object using confluent KafkaProtobufDeserializer and write to iceberg tables of hadoop/hive catalog
 * and use the KafkaSpecificProtoProducer to send records into kafka for testing and then run the class below(Schema registry involved)
 * <p>
 * <p>
 */
public class KafkaDataStreamSpecificProtoRecordRead2IcebergTables {
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
        //TODO
        Class<?> unmodColl = Class.forName("java.util.Collections$UnmodifiableCollection");
        env.getConfig().addDefaultKryoSerializer(unmodColl, UnmodifiableCollectionsSerializer.class);

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
    public KafkaSource<StockTicksProto.StockTicks> buildKafkaSource(String kafkaBootStrapServers, String[] topics, String groupId) {
        Map<String, String> kafkaProps = new HashMap<>();
        kafkaProps.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:18081");
        kafkaProps.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, StockTicksProto.StockTicks.class.getName());

        KafkaSource<StockTicksProto.StockTicks> kafkaSource = KafkaSource.<StockTicksProto.StockTicks>builder()
                .setBootstrapServers(kafkaBootStrapServers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(KafkaStockTicksProtobufDeserializer.class, kafkaProps))
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
    public SingleOutputStreamOperator<RowData> createDataStreamSource(StreamExecutionEnvironment env, String kafkaBootStrapServers, String[] topics, String groupId) throws ClassNotFoundException {
        setCheckpoint(env);
        KafkaSource<StockTicksProto.StockTicks> source = this.buildKafkaSource(kafkaBootStrapServers, topics, groupId);
        return env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map((MapFunction<StockTicksProto.StockTicks, RowData>) value -> {
                    GenericRowData of = GenericRowData.of(value.getVolume(),
                            StringData.fromString(value.getSymbol()),
                            StringData.fromString(value.getTs()),
                            StringData.fromString(value.getMonth()),
                            value.getHigh(),
                            value.getLow(),
                            StringData.fromString(value.getKey()),
                            value.getYear(),
                            StringData.fromString(value.getDate()),
                            value.getClose(),
                            value.getOpen(),
                            StringData.fromString(value.getDay())
                    );
                    return of;
                });
    }

    public void writeToIcebergHadoopCatalogTables(DataStream<RowData> source, String catalogName, String databaseName, String tableName) {
        TableLoader tableLoader = TableLoader.fromCatalog(CatalogLoader.hadoop(catalogName,
                        new org.apache.hadoop.conf.Configuration(),
                        Utils.getHadoopProperties()),
                TableIdentifier.of(databaseName, tableName));
        FlinkSink.forRowData(source)
                .tableLoader(tableLoader)
                .append();
    }

    /**
     * Write rowdata into iceberg tables in hive catalogs
     *
     * @param source
     * @param catalogName
     * @param databaseName
     * @param tableName
     */
    public void writeToIcebergHiveCatalogTables(DataStream<RowData> source, String catalogName, String databaseName, String tableName) {
        TableLoader tableLoader = TableLoader.fromCatalog(
                CatalogLoader.hive(catalogName,
                        new org.apache.hadoop.conf.Configuration(),
                        Utils.getHiveProperties()),
                TableIdentifier.of(databaseName, tableName));
        FlinkSink.forRowData(source)
                .tableLoader(tableLoader)
                .append();
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "bryan");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        KafkaDataStreamSpecificProtoRecordRead2IcebergTables write2Tables = new KafkaDataStreamSpecificProtoRecordRead2IcebergTables();
        DataStream<RowData> kafkaSource = write2Tables.createDataStreamSource(env, "localhost:19092", new String[]{"StockTicksSpecificProtoBuf"}, "stock_sticks_client");
        write2Tables.writeToIcebergHadoopCatalogTables(kafkaSource, "hadoop_catalog", "default", "stock_ticks");
        write2Tables.writeToIcebergHiveCatalogTables(kafkaSource, "hive_catalog", "hive_db", "stock_ticks");
        env.execute("Writes Json data with schema in schema registry into iceberg tables");
    }
}