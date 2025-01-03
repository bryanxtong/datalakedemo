package org.example.kafka.confluent.serde;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
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
import java.util.List;
import java.util.Map;

/**
 * Deserialize Avro generic records as java Object using confluent KafkaAvroDeserializer and write to iceberg tables of hadoop/hive catalog
 * and use the KafkaGenericAvroProducer to send records into kafka for testing and then run the class below(Schema registry involved)
 * <p>
 * <p>
 */
public class KafkaDataStreamGenericAvroRecordRead2IcebergTables {
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

    public KafkaSource<Object> buildKafkaSource(String kafkaBootStrapServers, String[] topics, String groupId) {
        Map<String, String> kafkaProps = new HashMap<>();
        kafkaProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:18081");

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
     * transform to rowdata
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
        //copy from flink-avro AvroKryoSerializerUtils.java to eliminate com.esotericsoftware.kryo.KryoException: java.lang.UnsupportedOperationException
        env.getConfig().getSerializerConfig().addDefaultKryoSerializer(Schema.class, AvroKryoSerializerUtils.AvroSchemaSerializer.class);

        KafkaSource<Object> source = this.buildKafkaSource(kafkaBootStrapServers, topics, groupId);
        return env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map((MapFunction<Object, RowData>) value -> {
                    GenericRowData rowData = new GenericRowData(12);
                    GenericRecord rc = (GenericRecord) value;
                    Schema schema = rc.getSchema();
                    List<Schema.Field> fields = schema.getFields();
                    for (int i = 0; i < fields.size(); i++) {
                        Object o = rc.get(fields.get(i).name());
                        if (o instanceof Long l) {
                            rowData.setField(i, l);
                        } else if (o instanceof Utf8 str) {
                            rowData.setField(i, StringData.fromString(new String(str.getBytes())));
                        } else if (o instanceof Double d) {
                            rowData.setField(i, d);
                        } else if (o instanceof Integer integer) {
                            rowData.setField(i, integer);
                        }
                    }
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
        KafkaDataStreamGenericAvroRecordRead2IcebergTables write2Tables = new KafkaDataStreamGenericAvroRecordRead2IcebergTables();
        DataStream<RowData> kafkaSource = write2Tables.createDataStreamSource(env, "localhost:19092", new String[]{"StockTicksGenericAvro"}, "stock_sticks_client");
        kafkaSource.print("----------------");
        write2Tables.writeToIcebergHadoopCatalogTables(kafkaSource, "hadoop_catalog", "default", "stock_ticks");
        write2Tables.writeToIcebergHiveCatalogTables(kafkaSource, "hive_catalog", "hive_db", "stock_ticks");
        env.execute("Writes Avro data with schema in schema registry into iceberg tables");
    }
}