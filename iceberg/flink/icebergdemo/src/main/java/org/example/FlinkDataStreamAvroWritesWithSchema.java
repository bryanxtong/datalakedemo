package org.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.AvroGenericRecordToRowDataMapper;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.hadoop.HadoopCatalog;

/**
 * Write avro records with pre-defined schema
 * write avro data to hive_catalog or hadoop_catalog tables
 */
public class FlinkDataStreamAvroWritesWithSchema {
    private int parallelism = 10;
    private Table table;
    private TableLoader tableLoader;

    public FlinkDataStreamAvroWritesWithSchema(Utils.CatalogType catalogType, String catalogName, TableIdentifier tableIdentifier) {
        if (catalogType.equals(Utils.CatalogType.HIVE)) {
            this.tableLoader = TableLoader.fromCatalog(CatalogLoader.hive(catalogName, new Configuration(), Utils.getHiveProperties()), tableIdentifier);
            Catalog hiveCatalog = Utils.getHiveCatalog(catalogName);
            this.table = hiveCatalog.loadTable(tableIdentifier);
        } else if (catalogType.equals(Utils.CatalogType.HADOOP)) {
            this.tableLoader = TableLoader.fromCatalog(CatalogLoader.hadoop(catalogName, new Configuration(), Utils.getHadoopProperties()), tableIdentifier);
            Catalog hadoopCatalog = Utils.getHadoopCatalog();
            this.table = hadoopCatalog.loadTable(tableIdentifier);
        }
    }

    public void write(DataStream<GenericRecord> outputStream, Schema avroSchema) throws Exception {
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        RowType rowType = FlinkSchemaUtil.convert(icebergSchema);
        FlinkSink.builderFor(
                        outputStream,
                        AvroGenericRecordToRowDataMapper.forAvroSchema(avroSchema),
                        FlinkCompatibilityUtil.toTypeInfo(rowType))
                .table(table)
                .tableLoader(tableLoader)
                .writeParallelism(parallelism)
                .upsert(true)
                .append();
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "bryan");
        //FlinkDataStreamAvroWritesWithSchema flinkDataStreamAvroWrites =
                //new FlinkDataStreamAvroWritesWithSchema(Utils.CatalogType.HADOOP, "hadoop_catalog", TableIdentifier.of("default", "sample"));
        FlinkDataStreamAvroWritesWithSchema flinkDataStreamAvroWrites =
                new FlinkDataStreamAvroWritesWithSchema(Utils.CatalogType.HIVE, "hive_catalog", TableIdentifier.of("default", "sample"));
        String schemaStr = """
                        {
                          "type": "record",
                          "name": "sample",
                          "namespace": "org.example",
                          "fields": [
                            {
                              "name": "id",
                              "type": "long"
                            },
                            {
                              "name": "data",
                              "type": "string"
                            }
                          ]
                        }
                """;
        Schema.Parser parser = new Schema.Parser();
        Schema avroSchema = parser.parse(schemaStr);
        GenericRecord genericRecord = new GenericData.Record(avroSchema);
        genericRecord.put("id", 19);
        genericRecord.put("data", "VVVVV");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        GenericRecordAvroTypeInfo avroTypeInfo = new GenericRecordAvroTypeInfo(avroSchema);
        DataStream<GenericRecord> dataStream = env.fromData(genericRecord).returns(avroTypeInfo);
        flinkDataStreamAvroWrites.write(dataStream, avroSchema);
        env.execute();
    }

}
