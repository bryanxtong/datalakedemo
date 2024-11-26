package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.hive.HiveCatalog;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Read catalog data from hive metastore
 *
 * CREATE CATALOG hive_catalog WITH (
 *   'type'='iceberg',
 *   'catalog-type'='hive',
 *   'uri'='thrift://localhost:9083',
 *   'clients'='5',
 *   'property-version'='1',
 *   'warehouse'='hdfs://localhost:9000/user/hive/warehouse'
 * );
 *
 * CREATE TABLE flink_table (
 *     id   BIGINT,
 *     data STRING
 * ) WITH (
 *     'connector'='iceberg',
 *     'catalog-name'=' hive_catalog',
 *     'catalog-database'='hive_db',
 *     'catalog-table'='hive_iceberg_table',
 *     'uri'='thrift://localhost:9083',
 *     'warehouse'='hdfs://localhost:9000/user/hive/warehouse'
 * );
 *
 * INSERT INTO `hive_catalog`.`hive_db`.`hive_iceberg_table` VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC');
 */
public class HiveCatalogTableReadExample {

    public static void streamingReadHiveCatalogTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        HiveCatalog catalog = new HiveCatalog();
        Map<String, String> hiveProperties = new HashMap<>();
        hiveProperties.put("warehouse", "hdfs://172.30.69.222:9000/user/hive/warehouse");
        hiveProperties.put("uri", "thrift://172.30.69.222:9083");
        catalog.initialize("hive", hiveProperties);
        TableLoader tableLoader = TableLoader.fromCatalog(CatalogLoader.hive("hive", new Configuration(), hiveProperties),
                TableIdentifier.of("hive_db", "hive_iceberg_table"));
        IcebergSource<RowData> source = IcebergSource.forRowData()
                .tableLoader(tableLoader)
                .assignerFactory(new SimpleSplitAssignerFactory())
                .streaming(true)
                .streamingStartingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
                .monitorInterval(Duration.ofSeconds(2))
                .build();

        DataStreamSource<RowData> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "My Iceberg Source",
                TypeInformation.of(RowData.class));

        stream.print();
        env.execute("Test Iceberg Streaming Read");
    }

    public static void batchReadHiveCatalogTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        HiveCatalog catalog = new HiveCatalog();
        Map<String, String> hiveProperties = new HashMap<>();
        hiveProperties.put("warehouse", "hdfs://172.30.69.222:9000/user/hive/warehouse");
        hiveProperties.put("uri", "thrift://172.30.69.222:9083");
        catalog.initialize("hive", hiveProperties);
        TableLoader tableLoader = TableLoader.fromCatalog(CatalogLoader.hive("hive", new Configuration(), hiveProperties),
                TableIdentifier.of("hive_db", "hive_iceberg_table"));
        IcebergSource<org.apache.flink.table.data.RowData> source = IcebergSource.forRowData()
                .tableLoader(tableLoader)
                .assignerFactory(new SimpleSplitAssignerFactory())
                .streaming(false)
                .build();

        DataStreamSource<RowData> batch = env.fromSource(source, WatermarkStrategy.noWatermarks(), "My Iceberg Source", TypeInformation.of(RowData.class));

        batch.print();
        env.execute("Test Iceberg Batch Read");
    }

    public static void main(String[] args) throws Exception {
        //HiveCatalogTableReadExample.batchReadHiveCatalogTable();
        HiveCatalogTableReadExample.streamingReadHiveCatalogTable();
    }
}
