package cn.doitedu.flink.java.demos;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/5/2
 * @Desc:
 *
 * 测试数据  ：
 * 1,e01,10000,p01,10
 * 1,e02,11000,p02,20
 * 1,e02,12000,p03,40
 * 1,e03,20000,p02,10
 * 1,e01,21000,p03,50
 * 1,e04,22000,p04,10
 * 1,e06,28000,p05,60
 * 1,e07,30000,p02,10
 **/
public class _20_Window_Api_Demo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // 1,e01,3000,pg02
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<EventBean2> beanStream = source.map(s -> {
            String[] split = s.split(",");
            return new EventBean2(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3], Integer.parseInt(split[4]));
        }).returns(EventBean2.class);


        // 分配 watermark ，以推进事件时间
        SingleOutputStreamOperator<EventBean2> watermarkedBeanStream = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<EventBean2>forBoundedOutOfOrderness(Duration.ofMillis(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<EventBean2>() {
                            @Override
                            public long extractTimestamp(EventBean2 eventBean, long recordTimestamp) {
                                return eventBean.getTimeStamp();
                            }
                        })
        );

        /**
         * 滚动聚合api使用示例
         * 需求 一 ：  每隔10s，统计最近 30s 的数据中，每个用户的行为事件条数
         * 使用aggregate算子来实现
         */
        SingleOutputStreamOperator<Integer> resultStream = watermarkedBeanStream
                .keyBy(EventBean2::getGuid)
                // 参数1： 窗口长度 ； 参数2：滑动步长
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                // reduce ：滚动聚合算子，它有个限制 ，聚合结果的数据类型 与  数据源中的数据类型 ，是一致
                /*.reduce(new ReduceFunction<EventBean>() {
                    @Override
                    public EventBean reduce(EventBean value1, EventBean value2) throws Exception {
                        return null;
                    }
                })*/
                .aggregate(new AggregateFunction<EventBean2, Integer, Integer>() {
                    /**
                     * 初始化累加器
                     * @return
                     */
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    /**
                     * 滚动聚合的逻辑（拿到一条数据，如何去更新累加器）
                     * @param value The value to add
                     * @param accumulator The accumulator to add the value to
                     * @return
                     */
                    @Override
                    public Integer add(EventBean2 value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    /**
                     * 从累加器中，计算出最终要输出的窗口结算结果
                     * @param accumulator The accumulator of the aggregation
                     * @return
                     */
                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    /**
                     *  批计算模式下，可能需要将多个上游的局部聚合累加器，放在下游进行全局聚合
                     *  因为需要对两个累加器进行合并
                     *  这里就是合并的逻辑
                     *  流计算模式下，不用实现！
                     * @param a An accumulator to merge
                     * @param b Another accumulator to merge
                     * @return
                     */
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });
        /*resultStream.print();*/


        /**
         * 需求 二 ：  每隔10s，统计最近 30s 的数据中，每个用户的平均每次行为时长
         * 要求用 aggregate 算子来做聚合
         * 滚动聚合api使用示例
         */
        SingleOutputStreamOperator<Double> resultStream2 = watermarkedBeanStream
                .keyBy(EventBean2::getGuid)
                /*.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))*/
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.milliseconds(10)))
                // 泛型1： 输入的数据的类型  ；  泛型2： 累加器的数据类型  ；  泛型3： 最终结果的类型
                .aggregate(new AggregateFunction<EventBean2, Tuple2<Integer, Integer>, Double>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return Tuple2.of(0, 0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(EventBean2 eventBean, Tuple2<Integer, Integer> accumulator) {
                        // accumulator.setField(accumulator.f0+1,0);
                        // accumulator.setField(accumulator.f1+eventBean.getActTimelong(),1);
                        // return accumulator;

                        return Tuple2.of(accumulator.f0 + 1, accumulator.f1 + eventBean.getActTimelong());
                    }

                    @Override
                    public Double getResult(Tuple2<Integer, Integer> accumulator) {

                        return accumulator.f1 / (double) accumulator.f0;
                    }

                    /**
                     * 在批计算模式中，shuffle的上游可以做局部聚合，然后会把局部聚合结果交给下游去做全局聚合
                     * 因此，就需要提供  两个局部聚合结果进行合并的逻辑
                     *
                     * 在流式计算中，不存在这种 上游局部聚合和交给下游全局聚合的机制！
                     * 所以，在流式计算模式下，不用实现下面的方法
                     * @param a An accumulator to merge
                     * @param b Another accumulator to merge
                     * @return
                     */
                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                    }
                });
        /*resultStream2.print();*/


        /**
         * TODO 补充练习 1
         * 需求 一 ：  每隔10s，统计最近 30s 的数据中，每个用户的行为事件条数
         * 滚动聚合api使用示例
         * 使用sum算子来实现
         */
        watermarkedBeanStream
                .map(bean->Tuple2.of(bean,1)).returns(new TypeHint<Tuple2<EventBean2, Integer>>() {})
                .keyBy(tp->tp.f0.getGuid())
                .window(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)))
                // 数据: Tuple2<Bean,1>
                .sum("f1")
                /*.print()*/;


        /**
         * TODO 补充练习 2
         * 需求 一 ：  每隔10s，统计最近 30s 的数据中，每个用户的最大行为时长
         * 滚动聚合api使用示例
         * 用max算子来实现
         */
        watermarkedBeanStream
                .keyBy(EventBean2::getGuid)
                .window(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)))
                .max("actTimelong")
                /*.print()*/;



        /**
         * TODO 补充练习 3
         * 需求 一 ：  每隔10s，统计最近 30s 的数据中，每个用户的最大行为时长及其所在的那条行为记录
         * 滚动聚合api使用示例
         * 用maxBy算子来实现
         */
        watermarkedBeanStream
                .keyBy(EventBean2::getGuid)
                .window(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)))
                .maxBy("actTimelong")
                /*.print()*/;



        /**
         * TODO 补充练习 4
         * 需求 一 ：  每隔10s，统计最近 30s 的数据中，每个页面上发生的行为中，平均时长最大的前2种事件及其平均时长
         * 用 process算子来实现
         */
        watermarkedBeanStream
                .keyBy(bean->bean.getPageId())
                .window(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(10)))
                .process(new ProcessWindowFunction<EventBean2, Tuple3<String,String,Double>, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<EventBean2, Tuple3<String,String,Double>, String, TimeWindow>.Context context, Iterable<EventBean2> elements, Collector<Tuple3<String,String,Double>> out) throws Exception {
                        // 构造一个hashmap来记录每一个事件的发生总次数，和行为总时长
                        HashMap<String, Tuple2<Integer, Long>> tmpMap = new HashMap<>();

                        // 遍历窗口中的每一条数据
                        for (EventBean2 element : elements) {
                            String eventId = element.getEventId();
                            Tuple2<Integer, Long> countAndTimelong = tmpMap.getOrDefault(eventId,Tuple2.of(0,0L));

                            tmpMap.put(eventId,Tuple2.of(countAndTimelong.f0+1,countAndTimelong.f1+element.getActTimelong()) );
                        }

                        // 然后，从tmpMap中，取到 平均时长 最大的前两个事件
                        ArrayList<Tuple2<String, Double>> tmpList = new ArrayList<>();
                        for (Map.Entry<String, Tuple2<Integer, Long>> entry : tmpMap.entrySet()) {
                            String eventId = entry.getKey();
                            Tuple2<Integer, Long> tuple = entry.getValue();
                            double avgTimelong = tuple.f1/ (double)tuple.f0;
                            tmpList.add(Tuple2.of(eventId,avgTimelong));
                        }

                        // 然后对tmpList按平均时长排序
                        Collections.sort(tmpList, new Comparator<Tuple2<String, Double>>() {
                            @Override
                            public int compare(Tuple2<String, Double> tp1, Tuple2<String, Double> tp2) {
                               /* return tp2.f1.compareTo(tp1.f1);*/
                                return Double.compare(tp2.f1,tp1.f1);
                            }
                        });

                        // 输出前2个
                        for(int i=0;i<Math.min(tmpList.size(),2);i++){
                            out.collect(Tuple3.of(key,tmpList.get(i).f0,tmpList.get(i).f1));
                        }
                    }
                })
                .print();



        /**
         * 全窗口计算api使用示例
         * 需求 三 ：  每隔10s，统计最近 30s 的数据中，每个用户的行为事件中，行为时长最长的前2条记录
         * 要求用 apply 或者  process 算子来实现
         *
         */
        // 1. 用apply算子来实现需求
        SingleOutputStreamOperator<EventBean2> resultStream3 = watermarkedBeanStream.keyBy(EventBean2::getGuid)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))

                // 泛型1： 输入数据类型；  泛型2：输出结果类型；  泛型3： key的类型， 泛型4：窗口类型
                .apply(new WindowFunction<EventBean2, EventBean2, Long, TimeWindow>() {
                    /**
                     *
                     * @param key 本次传给咱们的窗口是属于哪个key的
                     * @param window 本次传给咱们的窗口的各种元信息（比如本窗口的起始时间，结束时间）
                     * @param input 本次传给咱们的窗口中所有数据的迭代器
                     * @param out 结果数据输出器
                     * @throws Exception
                     */
                    @Override
                    public void apply(Long key, TimeWindow window, Iterable<EventBean2> input, Collector<EventBean2> out) throws Exception {

                        // low bi写法： 从迭代器中迭代出数据，放入一个arraylist，然后排序，输出前2条
                        ArrayList<EventBean2> tmpList = new ArrayList<>();

                        // 迭代数据，存入list
                        for (EventBean2 eventBean2 : input) {
                            tmpList.add(eventBean2);
                        }
                        // 排序
                        Collections.sort(tmpList, new Comparator<EventBean2>() {
                            @Override
                            public int compare(EventBean2 o1, EventBean2 o2) {
                                return o2.getActTimelong() - o1.getActTimelong();
                            }
                        });

                        // 输出前2条
                        for (int i = 0; i < Math.min(tmpList.size(), 2); i++) {
                            out.collect(tmpList.get(i));
                        }

                    }
                });
        /*resultStream3.print();*/


        // 2. 用process算子来实现需求
        SingleOutputStreamOperator<String> resultStream4 = watermarkedBeanStream.keyBy(EventBean2::getGuid)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .process(new ProcessWindowFunction<EventBean2, String, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, ProcessWindowFunction<EventBean2, String, Long, TimeWindow>.Context context, Iterable<EventBean2> input, Collector<String> out) throws Exception {

                        // 本次窗口的元信息
                        TimeWindow window = context.window();
                        long maxTimestamp = window.maxTimestamp();// 本窗口允许的最大时间戳  [1000,2000) ，其中 1999就是允许的最大时间戳； 2000就是窗口的end
                        long windowStart = window.getStart();
                        long windowEnd = window.getEnd();


                        // low bi写法： 从迭代器中迭代出数据，放入一个arraylist，然后排序，输出前2条
                        ArrayList<EventBean2> tmpList = new ArrayList<>();

                        // 迭代数据，存入list
                        for (EventBean2 eventBean2 : input) {
                            tmpList.add(eventBean2);
                        }
                        // 排序
                        Collections.sort(tmpList, new Comparator<EventBean2>() {
                            @Override
                            public int compare(EventBean2 o1, EventBean2 o2) {
                                return o2.getActTimelong() - o1.getActTimelong();
                            }
                        });

                        // 输出前2条
                        for (int i = 0; i < Math.min(tmpList.size(), 2); i++) {
                            EventBean2 bean = tmpList.get(i);
                            out.collect( "窗口start:"+windowStart + "," +"窗口end:"+ windowEnd + "," +  bean.getGuid() + "," + bean.getEventId() + "," + bean.getTimeStamp() + "," +bean.getPageId() + "," +bean.getActTimelong());
                        }
                    }

                });
        /*resultStream4.print();*/

        env.execute();



    }

}



