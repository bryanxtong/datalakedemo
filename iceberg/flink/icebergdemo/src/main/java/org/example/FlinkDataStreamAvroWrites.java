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
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.AvroGenericRecordToRowDataMapper;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.hadoop.HadoopCatalog;

/**
 * get the avro schema from the iceberg schema
 */
public class FlinkDataStreamAvroWrites {
    public static String DEFAULT_WAREHOUSE_LOCATION = "hdfs://172.30.69.222:9000/user/hdfs/warehouse";
    private int parallelism = 10;
    private Table table;
    private TableLoader tableLoader;
    private org.apache.iceberg.Schema icebergSchema;
    /**
     * get the avro schema from the iceberg schema
     */
    private Schema avroSchema;

    public FlinkDataStreamAvroWrites(String dataWarehouseLocation, TableIdentifier tableIdentifier) {
        if (dataWarehouseLocation != null || !dataWarehouseLocation.equals("")) {
            this.tableLoader = TableLoader.fromHadoopTable(dataWarehouseLocation + "/" + tableIdentifier.namespace() + "/" + tableIdentifier.name());
        } else {
            this.tableLoader = TableLoader.fromHadoopTable(DEFAULT_WAREHOUSE_LOCATION + "/" + tableIdentifier.namespace() + "/" + tableIdentifier.name());
        }
        HadoopCatalog hadoopCatalog = Utils.getHadoopCatalog(dataWarehouseLocation);
        this.table = hadoopCatalog.loadTable(tableIdentifier);
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
        FlinkDataStreamAvroWrites flinkDataStreamAvroWrites = new FlinkDataStreamAvroWrites("hdfs://172.30.69.222:9000/user/hdfs/warehouse", TableIdentifier.of("default", "sample"));
        Schema avroSchema = flinkDataStreamAvroWrites.getAvroSchema();
        GenericRecord genericRecord = new GenericData.Record(avroSchema);
        genericRecord.put("id", 100);
        genericRecord.put("data", "MMMMM1");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        GenericRecordAvroTypeInfo avroTypeInfo = new GenericRecordAvroTypeInfo(avroSchema);
        DataStream<GenericRecord> dataStream = env.fromData(genericRecord).returns(avroTypeInfo);
        flinkDataStreamAvroWrites.write(dataStream);
        env.execute();
    }

}
