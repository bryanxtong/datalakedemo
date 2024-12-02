package org.example;

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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * Read Json byte as java Object using confluent KafkaJsonDeserializer and write to iceberg tables of hive catalog
 *
 *  CREATE CATALOG hive_catalog WITH (
 *  'type'='iceberg',
 *  'catalog-type'='hive',
 *  'uri'='thrift://localhost:9083',
 *  'clients'='5',
 *  'property-version'='1',
 *  'warehouse'='hdfs://localhost:9000/user/hive/warehouse'
 *  );
 *
 * create table `stock_ticks`(
 *     `volume`  bigint,
 *     `symbol` string,
 *     `ts` string,
 *     `month` string,
 *    ` high` double,
 *     `low` double,
 *     `key` string,
 *     `year` int,
 *     `date` string,
 *     `close` double,
 *     `open` double,
 *     `day` string
 * ) WITH (
 *     'catalog-name'='hive_catalog',
 *     'catalog-database'='hive_db',
 *     'uri'='thrift://localhost:9083',
 *     'warehouse'='hdfs://localhost:9000/user/hive/warehouse'
 * );
 */
public class KafkaDataStreamJsonRead2IcebergHiveTables {

    public static void setCheckpoint(StreamExecutionEnvironment env) {
        env.enableCheckpointing(10000);
        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
        // make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // only two consecutive checkpoint failures are tolerated
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // enable externalized checkpoints which are retained
        // after job cancellation
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        // enables the unaligned checkpoints
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        // sets the checkpoint storage where checkpoint snapshots will be written
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///c://flink/checkpoint");
        env.configure(config);
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "bryan");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        setCheckpoint(env);
        Map<String, String> kafkaConfig = new HashMap<>();
        kafkaConfig.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, StockTicks.class.getName());
        KafkaSource<StockTicks> source = KafkaSource.<StockTicks>builder()
                .setBootstrapServers("localhost:19092")
                .setTopics("stock_ticks")
                .setGroupId("stock_sticks_client")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(KafkaStockticksJsonDeserializer.class, kafkaConfig))
                .build();

        SingleOutputStreamOperator<RowData> kafka_source = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").map((MapFunction<StockTicks, RowData>) value -> {
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
        TableLoader tableLoader = TableLoader.fromCatalog(
                CatalogLoader.hive("hive_catalog", new org.apache.hadoop.conf.Configuration(), Utils.getHiveProperties()),
                TableIdentifier.of("hive_db", "stock_ticks"));
        FlinkSink.forRowData(kafka_source)
                .tableLoader(tableLoader)
                //.overwrite(true)
                .append();
        env.execute("Writes Json data into iceberg tables in hive catalogs");
    }
}
