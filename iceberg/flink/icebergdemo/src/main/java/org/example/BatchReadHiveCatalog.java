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
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.hive.HiveCatalog;

import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;

@Deprecated
public class BatchReadHiveCatalog {
    public static void main(String[] args) throws Exception {
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
}
