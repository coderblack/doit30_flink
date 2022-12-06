package cn.doitedu.flink;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TestWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();  // 流批一体的入口环境
        env.setParallelism(1);

        DataStreamSource<String> st = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<String, Long>> map = st.map(s -> {
            String[] split = s.split(",");
            return Tuple2.of(split[0], Long.parseLong(split[1]));
        }).returns(new TypeHint<Tuple2<String, Long>>() {
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> wmed = map.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String,Long>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                return element.f1;
            }
        }));
        SingleOutputStreamOperator<String> wind = wmed.keyBy(tp -> tp.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        System.out.println(s + "====  window中用户函数触发开始：" + Thread.currentThread().getId());
                        // System.out.println(s + " window中的线程号： " + Thread.currentThread().getId());
                        // System.out.println(s + " window中的watermark : " + context.currentWatermark());
                        Thread.sleep(10000);
                        //System.out.println(s + "睡眠完毕");
                        int i = RandomUtils.nextInt(1, 100);
                        //System.out.println(s+ " 准备返回数据： " + i);
                        out.collect(s+ "," + i);
                        System.out.println(s + "====  window中用户函数触发结束： " + Thread.currentThread().getId());
                    }
                });


        wind.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                System.out.println("%%%% 下游processElement开始:" +value + "线程号： " + Thread.currentThread().getId());
                System.out.println("%%%% 下游当前watermark： " + ctx.timerService().currentWatermark());
                System.out.println("%%%% 下游processElement结束:" + "线程号： " + Thread.currentThread().getId());
            }
        }).startNewChain();


        env.execute();
    }
}
