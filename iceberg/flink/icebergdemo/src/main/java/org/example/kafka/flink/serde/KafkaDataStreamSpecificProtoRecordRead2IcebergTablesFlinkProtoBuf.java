package org.example.kafka.flink.serde;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.deserialize.PbRowDataDeserializationSchema;
import org.apache.flink.formats.protobuf.util.PbToRowTypeUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.example.Utils;
import org.example.model.protobuf.StockTicksProto;

/**
 * Use flink-protobuf to deserialize protobuf specific records(eg StockTicks) as RowData using PbRowDataSerializationSchema
 * and write to iceberg tables of hadoop/hive catalog and use the KafkaSpecificProtoProducerWithFlinkProtoBuf to send records
 * into kafka for testing
 * <p>
 */
public class KafkaDataStreamSpecificProtoRecordRead2IcebergTablesFlinkProtoBuf {

    public void setCheckpoint(StreamExecutionEnvironment env) throws ClassNotFoundException {
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
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
     * Specfic protobuf records(Stockticks)
     *
     * @param kafkaBootStrapServers
     * @param topics
     * @param groupId
     * @return
     */
    public KafkaSource<RowData> buildKafkaSource(String kafkaBootStrapServers, String[] topics, String groupId) {
        PbFormatConfig formatConfig =
                new PbFormatConfig(StockTicksProto.StockTicks.class.getName(), false, false, "");
        RowType rowType = PbToRowTypeUtil.generateRowType(StockTicksProto.StockTicks.getDescriptor());
        KafkaSource<RowData> kafkaSource = KafkaSource.<RowData>builder()
                .setBootstrapServers(kafkaBootStrapServers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new PbRowDataDeserializationSchema(rowType, TypeInformation.of(RowData.class),formatConfig))
                .build();
        return kafkaSource;
    }

    public SingleOutputStreamOperator<RowData> createDataStreamSource(StreamExecutionEnvironment env, String kafkaBootStrapServers, String[] topics, String groupId) throws ClassNotFoundException {
        setCheckpoint(env);
        KafkaSource<RowData> source = this.buildKafkaSource(kafkaBootStrapServers, topics, groupId);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
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
     * Write RowData into iceberg tables in hive catalogs
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
        KafkaDataStreamSpecificProtoRecordRead2IcebergTablesFlinkProtoBuf write2Tables = new KafkaDataStreamSpecificProtoRecordRead2IcebergTablesFlinkProtoBuf();
        DataStream<RowData> kafkaSource = write2Tables.createDataStreamSource(env, "localhost:19092", new String[]{"StockTicksSpecificProtoBufWithFlinkProtoBuf"}, "stock_sticks_client1");
        kafkaSource.print("---------------");
        write2Tables.writeToIcebergHadoopCatalogTables(kafkaSource, "hadoop_catalog", "default", "stock_ticks");
        write2Tables.writeToIcebergHiveCatalogTables(kafkaSource, "hive_catalog", "hive_db", "stock_ticks");
        env.execute("Writes Json data with schema in schema registry into iceberg tables");
    }
}