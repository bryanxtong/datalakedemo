package org.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.types.logical.RowType;
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
/**
 * get the avro schema from the iceberg schema
 * and write avro data to iceberg table of hadoop catalog or hive catalog
 */
public class FlinkDataStreamAvroWrites {

    private int parallelism = 10;
    private Table table;
    private TableLoader tableLoader;
    private org.apache.iceberg.Schema icebergSchema;
    /**
     * get the avro schema from the iceberg schema
     */
    private Schema avroSchema;

    public FlinkDataStreamAvroWrites(Utils.CatalogType catalogType,String catalogName,String databaseName, String tableName) {
        TableIdentifier tableIdentifier = TableIdentifier.of(databaseName, tableName);
        if(catalogType.equals(Utils.CatalogType.HIVE)){
           this.tableLoader = TableLoader.fromCatalog(CatalogLoader.hive(catalogName, new Configuration(), Utils.getHiveProperties()), tableIdentifier);
           Catalog hiveCatalog = Utils.getHiveCatalog(catalogName);
           this.table = hiveCatalog.loadTable(tableIdentifier);
       }else if(catalogType.equals(Utils.CatalogType.HADOOP)){
           this.tableLoader = TableLoader.fromCatalog(CatalogLoader.hadoop(catalogName, new Configuration(), Utils.getHadoopProperties()), tableIdentifier);
           Catalog hadoopCatalog = Utils.getHadoopCatalog();
           this.table = hadoopCatalog.loadTable(tableIdentifier);
       }
        this.icebergSchema = table.schema();
        this.avroSchema = AvroSchemaUtil.convert(icebergSchema, tableIdentifier.name());
    }

    public Schema getAvroSchema() {
        return avroSchema;
    }

    public void write(DataStream<GenericRecord> outputStream) throws Exception {
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
        //FlinkDataStreamAvroWrites flinkDataStreamAvroWrites = new FlinkDataStreamAvroWrites(Utils.CatalogType.HADOOP,"hadoop_catalog","default", "sample");
        FlinkDataStreamAvroWrites flinkDataStreamAvroWrites = new FlinkDataStreamAvroWrites(Utils.CatalogType.HIVE,"hive_catalog","default", "sample");
        Schema avroSchema = flinkDataStreamAvroWrites.getAvroSchema();
        GenericRecord genericRecord = new GenericData.Record(avroSchema);
        genericRecord.put("id", 100);
        genericRecord.put("data", "MMMMM1MMMMM");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        GenericRecordAvroTypeInfo avroTypeInfo = new GenericRecordAvroTypeInfo(avroSchema);
        DataStream<GenericRecord> dataStream = env.fromData(genericRecord).returns(avroTypeInfo);
        flinkDataStreamAvroWrites.write(dataStream);
        env.execute();
    }

}
