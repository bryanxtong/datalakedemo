package org.example.kafka.confluent.serde;

import com.fasterxml.jackson.databind.JsonNode;
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
import org.example.Utils;
import java.util.HashMap;
import java.util.Map;

/**
 * Deserialize Generic Json as java Object using confluent KafkaJsonSchemaDeserializer
 * and write to iceberg tables of hadoop/hive catalog and use the KafkaGenericJsonSchemaProducer
 * to send records into kafka(Schema registry involved)
 */
public class KafkaDataStreamGenericJsonSchemaRead2IcebergTables {
    public static String CONFLUENT_SCHEMA_REGISTRY_URL = "http://localhost:18081";

    public void setCheckpoint(StreamExecutionEnvironment env) {
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

    public KafkaSource<JsonNode> buildKafkaSource(String kafkaBootStrapServers, String[] topics, String groupId) {
        Map<String, String> kafkaProps = new HashMap<>();
        kafkaProps.put("schema.registry.url", CONFLUENT_SCHEMA_REGISTRY_URL);
        kafkaProps.put("json.fail.invalid.schema", String.valueOf(true));

        KafkaSource<JsonNode> kafkaSource = KafkaSource.<JsonNode>builder()
                .setBootstrapServers(kafkaBootStrapServers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(KafkaGenericJsonSchemaDeserializer.class, kafkaProps))
                .build();
        return kafkaSource;
    }

    public SingleOutputStreamOperator<RowData> createDataStreamSource(StreamExecutionEnvironment env, String kafkaBootStrapServers, String[] topics, String groupId) {
        setCheckpoint(env);
        KafkaSource<JsonNode> source = this.buildKafkaSource(kafkaBootStrapServers, topics, groupId);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").map((MapFunction<JsonNode, RowData>) value -> {
            GenericRowData rowData = new GenericRowData(12);
            //with JsonNode, Any Long type eg. value.get("volume") would change to Integer type if its value is less than Integer.MAX_VALUE
            rowData.setField(0, value.get("volume").asLong());
            rowData.setField(1, StringData.fromString(value.get("symbol").toString()));
            rowData.setField(2, StringData.fromString(value.get("ts").toString()));
            rowData.setField(3, StringData.fromString(value.get("month").toString()));
            rowData.setField(4, value.get("high").asDouble());
            rowData.setField(5, value.get("low").asDouble());
            rowData.setField(6, StringData.fromString(value.get("key").toString()));
            rowData.setField(7, value.get("year").asInt());
            rowData.setField(8, StringData.fromString(value.get("date").toString()));
            rowData.setField(9, value.get("close").asDouble());
            rowData.setField(10, value.get("open").asDouble());
            rowData.setField(11, StringData.fromString(value.get("day").toString()));
            System.out.println(rowData);
            return rowData;
        });
    }

    /**
     * Write rowdata into iceberg tables in hadoop catalogs
     *
     * @param source
     * @param catalogName
     * @param databaseName
     * @param tableName
     */
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
        KafkaDataStreamGenericJsonSchemaRead2IcebergTables write2Tables = new KafkaDataStreamGenericJsonSchemaRead2IcebergTables();
        DataStream<RowData> kafkaSource = write2Tables.createDataStreamSource(env, "localhost:19092", new String[]{"StockTicksJsonSchemaGeneric"}, "stock_sticks_client");
        kafkaSource.print();
        write2Tables.writeToIcebergHadoopCatalogTables(kafkaSource, "hadoop_catalog", "default", "stock_ticks");
        write2Tables.writeToIcebergHiveCatalogTables(kafkaSource, "hive_catalog", "hive_db", "stock_ticks");
        env.execute("Writes Json data with schema in schema registry into iceberg tables");
    }
}
