package cn.doitedu.flinksql.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/12
 * @Desc: 学大数据，到多易教育
 *    流  ===>  表  ，过程中如何传承  事件时间  和  watermark
 **/
public class Demo9_EventTimeAndWatermark2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        // {"guid":1,"eventId":"e02","eventTime":1655017433000,"pageId":"p001"}
        DataStreamSource<String> s1 = env.socketTextStream("doitedu", 9999);

        SingleOutputStreamOperator<Event> s2 = s1.map(s -> JSON.parseObject(s, Event.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.eventTime;
                            }
                        })
                );

        /* 观察流上的watermark推进
        s2.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                long wm = ctx.timerService().currentWatermark();
                out.collect(value + " => " + wm);
            }
        }).print();*/

        // 这样，直接把流 转成 表，会丢失watermark
        tenv.createTemporaryView("t_events", s2);

        /*tenv.executeSql("select guid,eventId,eventTime,pageId,current_watermark(eventTime) from t_events").print();*/

        // 测试验证watermark的丢失
        Table table = tenv.from("t_events");
        DataStream<Row> ds = tenv.toDataStream(table);
        ds.process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row row, ProcessFunction<Row, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(row + " => " + ctx.timerService().currentWatermark());
            }
        })/*.print()*/;


        // 可以在  流  转 表 时，显式声明 watermark策略
        tenv.createTemporaryView("t_events2", s2, Schema.newBuilder()
                .column("guid", DataTypes.INT())
                .column("eventId", DataTypes.STRING())
                .column("eventTime", DataTypes.BIGINT())
                .column("pageId", DataTypes.STRING())

                .columnByExpression("rt","to_timestamp_ltz(eventTime,3)")  // 重新利用一个bigint转成 timestamp后，作为事件时间属性
                .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")  // 利用底层流连接器暴露的 rowtime 元数据（代表的就是底层流中每条数据上的eventTime），声明成事件时间属性字段

                .watermark("rt","rt - interval '1' second ")  // 重新定义表上的watermark策略
                .watermark("rt", "source_watermark()") // 声明 watermark 直接 引用 底层流的watermark
                .build());

        tenv.executeSql("select  guid,eventId,eventTime,pageId,rt,current_watermark(rt) as wm from t_events2").print();


        env.execute();


    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Event {
        public int guid;
        public String eventId;
        public long eventTime;
        public String pageId;
    }


}
