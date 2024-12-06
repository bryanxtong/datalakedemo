package org.example.kafka.flink.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.serialize.PbRowDataSerializationSchema;
import org.apache.flink.formats.protobuf.util.PbToRowTypeUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.example.Utils;
import org.example.model.protobuf.StockTicksProto;

import java.util.ArrayList;
import java.util.List;

/**
 * Use Flink-Protobuf to serialize protobuf format to kafka
 */
public class KafkaSpecificProtoProducerWithFlinkProtoBuf {

    public static void main(String[] args) throws Exception {
        PbFormatConfig formatConfig = new PbFormatConfig(StockTicksProto.StockTicks.class.getName(), false, false, "");
        RowType rowType = PbToRowTypeUtil.generateRowType(StockTicksProto.StockTicks.getDescriptor());
        KafkaSink<RowData> sink = KafkaSink.<RowData>builder()
                .setBootstrapServers("localhost:19092,localhost:29092,localhost:39092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("StockTicksSpecificProtoBufWithFlinkProtoBuf")
                        .setValueSerializationSchema(new PbRowDataSerializationSchema(rowType, formatConfig))
                        .build())
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //read json lines
        List<String> lines = Utils.readJsonLines("batch_1.json");
        List<RowData> messages = new ArrayList<>();
        for (String line : lines) {
            ObjectMapper objectMapper = new ObjectMapper();
            org.example.model.json.StockTicks stockTicksJson = objectMapper.readValue(line, org.example.model.json.StockTicks.class);
            GenericRowData of = GenericRowData.of(stockTicksJson.getVolume(),
                    StringData.fromString(stockTicksJson.getSymbol()),
                    StringData.fromString(stockTicksJson.getTs()),
                    StringData.fromString(stockTicksJson.getMonth()),
                    stockTicksJson.getHigh(),
                    stockTicksJson.getLow(),
                    StringData.fromString(stockTicksJson.getKey()),
                    stockTicksJson.getYear(),
                    StringData.fromString(stockTicksJson.getDate()),
                    stockTicksJson.getClose(),
                    stockTicksJson.getOpen(),
                    StringData.fromString(stockTicksJson.getDay())
            );
            messages.add(of);
        }
        DataStreamSource<RowData> rowDataDataStreamSource = env.fromData(messages);
        rowDataDataStreamSource.print();
        rowDataDataStreamSource.sinkTo(sink);
        env.execute();
    }
}
