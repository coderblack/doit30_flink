package cn.doitedu.flink.java.demos;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

/**
 *
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/26
 * @Desc: 要把处理好的数据流，输出到文件系统（hdfs）
 *   使用的sink算子，是扩展包中的 StreamFileSink
 **/
public class _09_StreamFileSinkOperator_Demo1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        // 构造好一个数据流
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        // 将上面的数据流输出到文件系统（假装成一个经过了各种复杂计算后的结果数据流）


        /**
         * 方式一：
         * 核心逻辑：
         *   - 构造一个schema
         *   - 利用schema构造一个parquetWriterFactory
         *   - 利用parquetWriterFactory构造一个FileSink算子
         *   - 将原始数据转成GenericRecord流，输出到FileSink算子
         */
        // 1. 先定义GenericRecord的数据模式
        Schema schema = SchemaBuilder.builder()
                .record("DataRecord")
                .namespace("cn.doitedu.flink.avro.schema")
                .doc("用户行为事件数据模式")
                .fields()
                    .requiredInt("gid")
                    .requiredLong("ts")
                    .requiredString("eventId")
                    .requiredString("sessionId")
                    .name("eventInfo")
                    .type()
                    .map()
                    .values()
                    .type("string")
                    .noDefault()
                .endRecord();


        // 2. 通过定义好的schema模式，来得到一个parquetWriter
        ParquetWriterFactory<GenericRecord> writerFactory = ParquetAvroWriters.forGenericRecord(schema);

        // 3. 利用生成好的parquetWriter，来构造一个 支持列式输出parquet文件的 sink算子
        FileSink<GenericRecord> sink1 = FileSink.forBulkFormat(new Path("d:/datasink/"), writerFactory)
                .withBucketAssigner(new DateTimeBucketAssigner<GenericRecord>("yyyy-MM-dd--HH"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("doit_edu").withPartSuffix(".parquet").build())
                .build();


        // 4. 将自定义javabean的流，转成 上述sink算子中parquetWriter所需要的  GenericRecord流
        SingleOutputStreamOperator<GenericRecord> recordStream = streamSource
                .map((MapFunction<EventLog, GenericRecord>) eventLog -> {
                    // 构造一个Record对象
                    GenericData.Record record = new GenericData.Record(schema);

                    // 将数据填入record
                    record.put("gid", (int) eventLog.getGuid());
                    record.put("eventId", eventLog.getEventId());
                    record.put("ts", eventLog.getTimeStamp());
                    record.put("sessionId", eventLog.getSessionId());
                    record.put("eventInfo", eventLog.getEventInfo());

                    return record;
                }).returns(new GenericRecordAvroTypeInfo(schema));  // 由于avro的相关类、对象需要用avro的序列化器，所以需要显式指定AvroTypeInfo来提供AvroSerializer

        // 5. 输出数据
        recordStream.sinkTo(sink1);

        env.execute();


    }
}
