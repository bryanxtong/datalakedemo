package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;

import java.time.Duration;
/**
 * CREATE CATALOG hadoop_catalog WITH (
 * 'type'='iceberg',
 * 'catalog-type'='hadoop',
 * 'warehouse'='hdfs://localhost:9000/user/hdfs/warehouse',
 * 'property-version'='1'
 * );
 * INSERT INTO `hadoop_catalog`.`default`.`sample` VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC');
 * select * from `hadoop_catalog`.`default`.`sample`;
 */
public class HadoopCatalogTableReadExample {

    public static void batchReadHadoopTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://172.30.69.222:9000/user/hdfs/warehouse/default/sample");
        IcebergSource<RowData> source = IcebergSource.forRowData()
                .tableLoader(tableLoader)
                .assignerFactory(new SimpleSplitAssignerFactory())
                .streaming(false)
                .build();

        DataStreamSource<RowData> batch = env.fromSource(source,
                WatermarkStrategy.noWatermarks(),
                "My Iceberg Source",
                TypeInformation.of(RowData.class));

        batch.print();
        env.execute("Test Iceberg Batch Read");
    }

    public static void streamingReadHadoopTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://172.30.69.222:9000/user/hdfs/warehouse/default/sample");
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

    public static void main(String[] args) throws Exception {
        //HadoopCatalogTableReadExample.batchReadHadoopTable();
        HadoopCatalogTableReadExample.streamingReadHadoopTable();
    }
}
