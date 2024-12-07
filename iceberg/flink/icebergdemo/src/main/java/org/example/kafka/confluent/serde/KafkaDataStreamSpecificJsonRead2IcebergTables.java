package org.example.kafka.confluent.serde;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
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
import org.example.model.json.StockTicks;

import java.util.HashMap;
import java.util.Map;

/**
 * Read Json byte as java Object using confluent KafkaJsonDeserializer(No schema registry) and write to iceberg tables of hive/hadoop catalog
 */
public class KafkaDataStreamSpecificJsonRead2IcebergTables {

    public static void setCheckpoint(StreamExecutionEnvironment env) {
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

    public KafkaSource<StockTicks> buildKafkaSource(String kafkaBootStrapServers, String[] topics, String groupId) {
        Map<String, String> kafkaProps = new HashMap<>();
        kafkaProps.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, StockTicks.class.getName());
        KafkaSource<StockTicks> kafkaSource = KafkaSource.<StockTicks>builder()
                .setBootstrapServers(kafkaBootStrapServers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(KafkaStockticksJsonDeserializer.class, kafkaProps))
                .build();
        return kafkaSource;
    }

    public SingleOutputStreamOperator<RowData> createDataStreamSource(StreamExecutionEnvironment env, String kafkaBootStrapServers, String[] topics, String groupId) {
        setCheckpoint(env);
        KafkaSource<StockTicks> source = this.buildKafkaSource(kafkaBootStrapServers, topics, groupId);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").map((MapFunction<StockTicks, RowData>) value -> {
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
        KafkaDataStreamSpecificJsonRead2IcebergTables write2Tables = new KafkaDataStreamSpecificJsonRead2IcebergTables();
        DataStream<RowData> kafkaSource = write2Tables.createDataStreamSource(env, "localhost:19092", new String[]{"stock_ticks"}, "stock_sticks_client");
        write2Tables.writeToIcebergHadoopCatalogTables(kafkaSource, "hadoop_catalog", "default", "stock_ticks");
        write2Tables.writeToIcebergHiveCatalogTables(kafkaSource, "hive_catalog", "hive_db", "stock_ticks");
        env.execute("Writes Json data into iceberg tables");
    }
}
