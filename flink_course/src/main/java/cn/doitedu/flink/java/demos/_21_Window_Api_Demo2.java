package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class _21_Window_Api_Demo2 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // 1,e01,3000,pg02
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<EventBean2> beanStream = source.map(s -> {
            String[] split = s.split(",");
            return new EventBean2(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3], Integer.parseInt(split[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<EventBean2>forBoundedOutOfOrderness(Duration.ofMillis(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean2>() {
                    @Override
                    public long extractTimestamp(EventBean2 element, long recordTimestamp) {
                        return element.getTimeStamp();
                    }
                }));
        ;


        /**
         * 一、各种全局窗口开窗api
         */

        // 全局 计数滚动窗口
        beanStream.countWindowAll(10)  // 10条数据一个窗口
                .apply(new AllWindowFunction<EventBean2, String, GlobalWindow>() {
                    @Override
                    public void apply(GlobalWindow window, Iterable<EventBean2> values, Collector<String> out) throws Exception {

                    }
                });


        // 全局 计数滑动窗口
        beanStream.countWindowAll(10, 2); // 窗口长度为10条数据，滑动步长为2条数据
        /*.apply()*/


        // 全局 事件时间滚动窗口
        beanStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(30))) // 窗口长度为30s的滚动窗口
                .apply(new AllWindowFunction<EventBean2, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<EventBean2> values, Collector<String> out) throws Exception {

                    }
                });


        // 全局 事件时间滑动窗口
        beanStream.windowAll(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10))); // 窗口长度：30s，滑动步长：10s
        /*.apply()*/

        // 全局  事件时间会话窗口
        beanStream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(30)));  // 前后两个事件的间隙超过30s就划分窗口
        /*.apply()*/

        // 全局  处理时间滚动窗口
        beanStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)));


        // 全局  处理时间滑动窗口
        beanStream.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)));

        // 全局  处理间会话窗口
        beanStream.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));


        /**
         * 二、各种Keyed窗口开窗api
         */

        KeyedStream<EventBean2, Long> keyedStream = beanStream.keyBy(EventBean2::getGuid);

        // Keyed 计数滚动窗口
        keyedStream.countWindow(10);


        // Keyed 计数滑动窗口
        keyedStream.countWindow(10, 2);


        // Keyed 事件时间滚动窗口
        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(30)));


        // Keyed 事件时间滑动窗口
        keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)));


        // Keyed  事件时间会话窗口
        keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(30)));


        // Keyed  处理时间滚动窗口
        keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(30)));


        // Keyed  处理时间滑动窗口
        keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)));


        // Keyed  处理时间会话窗口
        keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));


    }


}
