package cn.doitedu.flink.exercise;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/28
 * @Desc: 创建两个流
 * 流1 ：
 * “id,eventId,cnt”
 * 1,event01,3
 * 1,event02,2
 * 2,event02,4
 * 流2 ：
 * “id,gender,city”
 * 1, male, shanghai
 * 2, female, beijing
 * <p>
 * 需求：
 * 1 , 将流1的数据展开
 * 比如，一条数据： 1,event01,3
 * 需要展开成3条:
 * 1,event01,随机数1
 * 1,event01,随机数2
 * 1,event01,随机数3
 *
 * 2 , 流1的数据，还需要关联上 流2 的数据  （性别，城市）
 * 并且把关联失败的流1的数据，写入一个侧流；否则输出到主流
 * 4 , 对主流数据按性别分组， 取 最大随机数所在的那一条数据 作为结果输出
 * 5 , 把侧流处理结果，写入 文件系统，并写成 parquet格式
 * 6 , 把主流处理结果，写入  mysql， 并实现幂等更新
 **/
public class Exercise_1 {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建流1
        DataStreamSource<String> ds1 = env.socketTextStream("localhost", 9991);

        SingleOutputStreamOperator<EventCount> s1 = ds1.map(s -> {
            String[] arr = s.split(",");
            return new EventCount(Integer.parseInt(arr[0]), arr[1], Integer.parseInt(arr[2]));
        });

        // 创建流2
        DataStreamSource<String> ds2 = env.socketTextStream("localhost", 9992);

        SingleOutputStreamOperator<UserInfo> s2 = ds2.map(s -> {
            String[] arr = s.split(",");
            return new UserInfo(Integer.parseInt(arr[0]), arr[1], arr[2]);
        });


        // 对流1 的数据按 count值 展开
        SingleOutputStreamOperator<EventCount> flattend = s1.process(new ProcessFunction<EventCount, EventCount>() {
            @Override
            public void processElement(EventCount value, ProcessFunction<EventCount, EventCount>.Context ctx, Collector<EventCount> out) throws Exception {
                // 取出count值
                int cnt = value.getCnt();
                // 循环cnt次，输出结果
                for (int i = 0; i < cnt; i++) {
                    out.collect(new EventCount(value.getId(), value.getEventId(), RandomUtils.nextInt(10, 100)));
                }
            }
        });


        // 准备一个广播状态描述器
        MapStateDescriptor<Integer, UserInfo> stateDescriptor = new MapStateDescriptor<>("s", Integer.class, UserInfo.class);
        // 准备一个测流输出标签
        OutputTag<EventCount> cOutputTag = new OutputTag<>("c", TypeInformation.of(EventCount.class));


        // 关联打宽需求（通过场景分析，用广播状态最合适），并将关联失败的数据写入侧流c
        // 广播流2
        BroadcastStream<UserInfo> broadcastS2 = s2.broadcast(stateDescriptor);
        // 连接 流1 和 广播流2
        BroadcastConnectedStream<EventCount, UserInfo> connectedStream = flattend.connect(broadcastS2);

        // 对连接流进行process处理，来实现数据的打宽
        SingleOutputStreamOperator<EventUserInfo> joinedResult = connectedStream.process(new BroadcastProcessFunction<EventCount, UserInfo, EventUserInfo>() {
            // 主流处理方法
            @Override
            public void processElement(EventCount eventCount, BroadcastProcessFunction<EventCount, UserInfo, EventUserInfo>.ReadOnlyContext ctx, Collector<EventUserInfo> out) throws Exception {
                ReadOnlyBroadcastState<Integer, UserInfo> broadcastState = ctx.getBroadcastState(stateDescriptor);

                UserInfo userInfo = null;
                if (broadcastState != null && (userInfo = broadcastState.get(eventCount.getId())) != null) {
                    // 关联成功的，输出到主流
                    out.collect(new EventUserInfo(eventCount.getId(), eventCount.getEventId(), eventCount.getCnt(), userInfo.getGender(), userInfo.getCity()));
                } else {
                    // 关联失败的，输出到侧流
                    ctx.output(cOutputTag, eventCount);
                }
            }

            // 广播流处理方法
            @Override
            public void processBroadcastElement(UserInfo userInfo, BroadcastProcessFunction<EventCount, UserInfo, EventUserInfo>.Context ctx, Collector<EventUserInfo> out) throws Exception {
                // 把数据放入广播状态
                BroadcastState<Integer, UserInfo> broadcastState = ctx.getBroadcastState(stateDescriptor);

                broadcastState.put(userInfo.getId(), userInfo);

            }
        });


        // 对主流数据按性别分组，取 最大随机数所在的那一条数据作为输出结果
        SingleOutputStreamOperator<EventUserInfo> mainResult = joinedResult
                .keyBy(EventUserInfo::getGender)
                .maxBy("cnt");


        // TODO 把主流结果，写入mysql，并实现幂等更新
        /*mainResult.print("main");*/
        SinkFunction<EventUserInfo> jdbcSink = JdbcSink.sink(
                "insert into t_eventuser values(?,?,?,?,?) on duplicate key update eventId=? , cnt =? ,gender =? ,city = ?",
                new JdbcStatementBuilder<EventUserInfo>() {
                    @Override
                    public void accept(PreparedStatement stmt, EventUserInfo eventUserInfo) throws SQLException {
                        stmt.setInt(1, eventUserInfo.getId());
                        stmt.setString(2, eventUserInfo.getEventId());
                        stmt.setInt(3, eventUserInfo.getCnt());
                        stmt.setString(4, eventUserInfo.getGender());
                        stmt.setString(5, eventUserInfo.getCity());

                        stmt.setString(6, eventUserInfo.getEventId());
                        stmt.setInt(7, eventUserInfo.getCnt());
                        stmt.setString(8, eventUserInfo.getGender());
                        stmt.setString(9, eventUserInfo.getCity());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/abc?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );
        mainResult.addSink(jdbcSink);


        // TODO 把侧流数据，写入文件系统，并生成parquet文件
        /*joinedResult.getSideOutput(cOutputTag).print("side");*/

        ParquetWriterFactory<EventCount> parquetWriterFactory = ParquetAvroWriters.forReflectRecord(EventCount.class);

        // 3. 利用生成好的parquetWriter，来构造一个 支持列式输出parquet文件的 sink算子
        FileSink<EventCount> bulkSink = FileSink.forBulkFormat(new Path("d:/sidesink/"), parquetWriterFactory)
                .withBucketAssigner(new DateTimeBucketAssigner<EventCount>("yyyy-MM-dd--HH"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("doit_edu").withPartSuffix(".parquet").build())
                .build();
        joinedResult.getSideOutput(cOutputTag).sinkTo(bulkSink);


        env.execute();


    }

}
