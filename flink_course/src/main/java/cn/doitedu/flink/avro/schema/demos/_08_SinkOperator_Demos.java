//package cn.doitedu.flink.java.demos;
//
//import cn.doitedu.flink.avro.schema.AvroEventLogBean;
//import com.alibaba.fastjson.JSON;
//import org.apache.avro.Schema;
//import org.apache.avro.SchemaBuilder;
//import org.apache.avro.data.RecordBuilder;
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.flink.api.common.serialization.SimpleStringEncoder;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.api.java.tuple.Tuple5;
//import org.apache.flink.connector.file.sink.FileSink;
//import org.apache.flink.core.fs.FileSystem;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
//import org.apache.flink.formats.parquet.ParquetWriterFactory;
//import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
//import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
//import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
//import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
//import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
//
//import java.util.HashMap;
//import java.util.Map;
//
//public class _08_SinkOperator_Demos {
//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
//
//        env.setParallelism(2);
//
//        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());
//
//
//        // 把它打印: 输出到控制台
//        /*streamSource.print();*/
//
//
//        // 输出到文件
//        /*streamSource.writeAsText("d:/sink_test", FileSystem.WriteMode.OVERWRITE);*/
//        streamSource.map(bean -> Tuple5.of(bean.getEventId(), bean.getGuid(), bean.getEventInfo(), bean.getSessionId(), bean.getTimeStamp())).returns(new TypeHint<Tuple5<String, Long, Map<String, String>, String, Long>>() {
//        })
//        /*.writeAsCsv("d:/sink_test2", FileSystem.WriteMode.OVERWRITE)*/;
//
//
//        /**
//         * 应用  StreamFileSink 算子，来将数据输出到  文件系统
//         */
//
//        /**
//         * 1. 输出为 行格式
//         */
//        // 构造一个FileSink对象
//        FileSink<String> rowSink = FileSink
//                .forRowFormat(new Path("d:/filesink/"), new SimpleStringEncoder<String>("utf-8"))
//                // 文件的滚动策略 （间隔时长10s，或文件大小达到 5M，就进行文件切换
//                .withRollingPolicy(DefaultRollingPolicy.builder().withRolloverInterval(10000).withMaxPartSize(5 * 1024 * 1024).build())
//                // 分桶的策略（划分子文件夹的策略）
//                .withBucketAssigner(new DateTimeBucketAssigner<String>())
//                .withBucketCheckInterval(5)
//                // 输出文件的文件名相关配置
//                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("doitedu").withPartSuffix(".txt").build())
//                .build();
//
//        // 然后添加到流，进行输出
//        streamSource.map(JSON::toJSONString)
//                //.addSink()  /* SinkFunction实现类对象,用addSink() 来添加*/
//                .sinkTo(rowSink); /*Sink 的实现类对象,用 sinkTo()来添加  */
//
//
//        /**
//         * 2. 输出为 列格式
//         *
//         * 要构造一个列模式的 FileSink，需要一个ParquetAvroWriterFactory
//         * 而获得ParquetAvroWriterFactory的方式是，利用一个工具类： ParquetAvroWriters
//         * 这个工具类提供了3种方法，来为用户构造一个ParquetAvroWriterFactory
//         *
//         * ## 方法1：
//         * writerFactory = ParquetAvroWriters.forGenericRecord(schema)
//         *
//         * ## 方法2：
//         * writerFactory = ParquetAvroWriters.forSpecificRecord(AvroEventLogBean.class)
//         *
//         * ## 方法3：
//         * writerFactory = ParquetAvroWriters.forReflectRecord(EventLog.class);
//         *
//         * 一句话：  3种方式需要传入的参数类型不同
//         * 1. 需要传入schema对象
//         * 2. 需要传入一种特定的JavaBean类class
//         * 3. 需要传入一个普通的JavaBean类class
//         *
//         * 传入这些参数，有何用？
//         * 这个工具 ParquetAvroWriters. 需要你提供你的数据模式schema（你要生成的parquet文件中数据模式schema）
//         *
//         * 上述的3种参数，都能让这个工具明白你所指定的数据模式（schema）
//         *
//         * 1. 传入Schema类对象，它本身就是parquet框架中用来表达数据模式的内生对象
//         *
//         * 2. 传入特定JavaBean类class，它就能通过调用传入的类上的特定方法，来获得Schema对象
//         * （这种特定JavaBean类，不用开发人员自己去写，而是用avro的schema描述文件+代码生产插件，来自动生成）
//         *
//         * 3. 传入普通JavaBean,然后工具可以自己通过反射手段来获取用户的普通JavaBean中的包名、类名、字段名、字段类型等信息，来翻译成一个符合Avro要求的Schema
//         *
//         *
//         */
//        // ### 方式 1： 手动构造schema，来生成ParquetAvroWriter工厂
//         Schema schema = Schema.createRecord("id", "用户id", "cn.doitedu.User", true);
//         ParquetWriterFactory<GenericRecord> writerFactory0 = ParquetAvroWriters.forGenericRecord(schema);// 根据调用的方法及传入的信息，来获取avro模式schema，并生成对应的parquetWriter
//
//
//        Schema schema1 = SchemaBuilder.builder().record("cn.doitedu.avro.schema")
//                .fields()
//                .requiredLong("guid")
//                .name("info")
//                .type()
//                .map()
//                .values()
//                .type("string")
//                .noDefault()
//                .endRecord();
//
//        GenericData.Record record = new GenericData.Record(schema);
//        record.put("guid",1);
//
//        new GenericRecordAvroTypeInfo(schema);
//
//        // ### 方式 2： 利用Avro的规范Bean对象，来生成ParquetAvroWriter工厂
//        ParquetWriterFactory<AvroEventLogBean> writerFactory1 = ParquetAvroWriters.forSpecificRecord(AvroEventLogBean.class);// 需要写avsc文件，并根据文件生成 javaBean，然后该方法可以从这种javabean来自动反射获取 schema
//
//        FileSink<AvroEventLogBean> parquetSink1 = FileSink
//                .forBulkFormat(new Path("d:/bulksink/"), writerFactory1)  // 核心点： 要一个parquet文件输出器 writerFactory
//                .withBucketAssigner(new DateTimeBucketAssigner<AvroEventLogBean>())  // 分桶策略
//                .withBucketCheckInterval(5)   //分桶检查间隔
//                .withRollingPolicy(OnCheckpointRollingPolicy.build())  // bulk模式下的文件滚动策略，只有一种： 当 checkpoint发生时，进行文件滚动
//                .withOutputFileConfig(OutputFileConfig.builder().withPartSuffix(".parquet").withPartPrefix("doitedu").build())  // 输出文件名的前后缀策略
//                .build();
//
//        // 将上面构造好的 sink算子对象，添加到数据流上，进行数据输出
//        streamSource
//                .map(eventLogBean -> {
//                    HashMap<CharSequence, CharSequence> eventInfo = new HashMap<>();
//                    for (Map.Entry<String, String> entry : eventLogBean.getEventInfo().entrySet()) {
//                        eventInfo.put(entry.getKey(), entry.getValue());
//                    }
//                    return new AvroEventLogBean(eventLogBean.getGuid(), eventLogBean.getSessionId(), eventLogBean.getEventId(), eventLogBean.getTimeStamp(), eventInfo);
//                }).returns(AvroEventLogBean.class)
//                /*.sinkTo(parquetSink1)*/;
//
//
//        // ### 方式 3： 利用Avro的规范Bean对象，来生成ParquetAvroWriter工厂
//        ParquetWriterFactory<EventLog> writerFactory2 = ParquetAvroWriters.forReflectRecord(EventLog.class);// 该方法，传入一个普通的JavaBean类，就可以自动通过反射来生成 Schema
//
//        FileSink<EventLog> parquetSink2 = FileSink
//                .forBulkFormat(new Path("d:/bulksink"), writerFactory2)
//                .withBucketAssigner(new DateTimeBucketAssigner<EventLog>())
//                .withBucketCheckInterval(5)
//                .withRollingPolicy(OnCheckpointRollingPolicy.build())  // bulk模式下的文件滚动策略，只有一种： 当 checkpoint发生时，进行文件滚动
//                .withOutputFileConfig(OutputFileConfig.builder().withPartSuffix(".parquet").withPartPrefix("doitedu").build())
//                .build();
//        streamSource.sinkTo(parquetSink2);
//
//
//        env.execute();
//    }
//}
