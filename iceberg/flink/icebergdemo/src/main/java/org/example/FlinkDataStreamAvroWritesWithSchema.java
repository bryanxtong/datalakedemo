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
 * Write avro records with pre-defined schema
 */
public class FlinkDataStreamAvroWritesWithSchema {

    public static String DEFAULT_WAREHOUSE_LOCATION = "hdfs://172.30.69.222:9000/user/hdfs/warehouse";
    private int parallelism = 10;
    private Table table;
    private TableLoader tableLoader;

    public FlinkDataStreamAvroWritesWithSchema(String dataWarehouseLocation, TableIdentifier tableIdentifier) {
        if (dataWarehouseLocation != null || !dataWarehouseLocation.equals("")) {
            this.tableLoader = TableLoader.fromHadoopTable(dataWarehouseLocation + "/" + tableIdentifier.namespace() + "/" + tableIdentifier.name());
        } else {
            this.tableLoader = TableLoader.fromHadoopTable(DEFAULT_WAREHOUSE_LOCATION + "/" + tableIdentifier.namespace() + "/" + tableIdentifier.name());
        }
        HadoopCatalog hadoopCatalog = Utils.getHadoopCatalog(dataWarehouseLocation);
        this.table = hadoopCatalog.loadTable(tableIdentifier);
    }

    /*@Deprecated
    protected void output(DataStream<GenericRecord> outputStream, Schema avroSchema) {
        DataStream<Row> rowDataStream = outputStream.map(gc -> {
            int columnNum = gc.getSchema().getFields().size();
            Object[] rowData = new Object[columnNum];
            for (int i = 0; i < columnNum; i++) {
                Object o = gc.get(i);
                if (o instanceof Boolean m) {
                    rowData[i] = m;
                }
                if (o instanceof Long m) {
                    rowData[i] = m;
                }
                if (o instanceof Integer m) {
                    rowData[i] = m;
                }
                if (o instanceof Float m) {
                    rowData[i] = m;
                }
                if (o instanceof Double m) {
                    rowData[i] = m;
                } else if (o instanceof Utf8 m) {
                    rowData[i] = new String(m.getBytes());
                } else {
                    System.out.println(gc.get(i).getClass());
                    rowData[i] = gc.get(i);
                }
            }
            return Row.of(rowData);
        });

        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        RowType rowType = FlinkSchemaUtil.convert(icebergSchema);
        TableSchema tableSchema = FlinkSchemaUtil.toSchema(rowType);
        FlinkSink.forRow(rowDataStream, tableSchema)
                .table(table)
                .tableLoader(tableLoader)
                .tableSchema(tableSchema)
                .writeParallelism(parallelism)
                .upsert(true)
                .append();
    }*/

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
        FlinkDataStreamAvroWritesWithSchema flinkDataStreamAvroWrites =
                new FlinkDataStreamAvroWritesWithSchema("hdfs://172.30.69.222:9000/user/hdfs/warehouse", TableIdentifier.of("default", "sample"));
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
        genericRecord.put("id", 1);
        genericRecord.put("data", "VVVVV");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        GenericRecordAvroTypeInfo avroTypeInfo = new GenericRecordAvroTypeInfo(avroSchema);
        DataStream<GenericRecord> dataStream = env.fromData(genericRecord).returns(avroTypeInfo);
        flinkDataStreamAvroWrites.write(dataStream, avroSchema);
        env.execute();
    }

}
