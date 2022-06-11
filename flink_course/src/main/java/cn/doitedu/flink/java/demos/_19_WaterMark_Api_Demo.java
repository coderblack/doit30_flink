package cn.doitedu.flink.java.demos;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/5/1
 * @Desc:  watermark生成设置相关代码演示
 *       及单并行度情况下的watermark推进观察
 *
 *   ==> 在socket端口依次输入如下两条数据：
 *   1,e06,3000,page02
 *   1,e06,3000,page02
 *
 *   ==> 程序的控制台上会依次输出如下信息：
 *     本次收到的数据EventBean(guid=1, eventId=e05, timeStamp=2000, pageId=page01)
 *     此刻的watermark： -9223372036854775808
 *     此刻的处理时间（processing time）： 1651396210778
 *  ----------------------
 *     本次收到的数据EventBean(guid=1, eventId=e06, timeStamp=3000, pageId=page02)
 *     此刻的watermark： 1999
 *     此刻的处理时间（processing time）： 1651396273755
 *
 **/
public class _19_WaterMark_Api_Demo {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);


        // 1,e01,168673487846,pg01
        SingleOutputStreamOperator<String> s1 = env.socketTextStream("localhost", 9999).disableChaining();


        // 策略1： WatermarkStrategy.noWatermarks()  不生成 watermark，禁用了事件时间的推进机制
        // 策略2： WatermarkStrategy.forMonotonousTimestamps()  紧跟最大事件时间
        // 策略3： WatermarkStrategy.forBoundedOutOfOrderness()  允许乱序的 watermark生成策略
        // 策略4： WatermarkStrategy.forGenerator()  自定义watermark生成算法

        /*
         * 示例 一 ：  从最源头算子开始，生成watermark
         */
        // 1、构造一个watermark的生成策略对象（算法策略，及事件时间的抽取方法）
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofMillis(0))  // 允许乱序的算法策略
                .withTimestampAssigner((element, recordTimestamp) -> Long.parseLong(element.split(",")[2]));  // 时间戳抽取方法
        // 2、将构造好的 watermark策略对象，分配给流（source算子）
        /*s1.assignTimestampsAndWatermarks(watermarkStrategy);*/



        /*
         * 示例 二：  不从最源头算子开始生成watermark，而是从中间环节的某个算子开始生成watermark
         * 注意！：如果在源头就已经生成了watermark， 就不要在下游再次产生watermark
         */
        SingleOutputStreamOperator<EventBean> s2 = s1.map(s -> {
                    String[] split = s.split(",");
                   /* Thread.sleep(50000);*/
                    return new EventBean(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3]);
                }).returns(EventBean.class).disableChaining()
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<EventBean>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                                    @Override
                                    public long extractTimestamp(EventBean eventBean, long recordTimestamp) {
                                        return eventBean.getTimeStamp();
                                    }
                                })
                );

        s2.process(new ProcessFunction<EventBean, EventBean>() {
            @Override
            public void processElement(EventBean eventBean, ProcessFunction<EventBean, EventBean>.Context ctx, Collector<EventBean> out) throws Exception {

                /*Thread.sleep(1000);
                System.out.println("睡醒了，准备打印");*/

                // 打印此刻的 watermark
                /*long processTime = ctx.timerService().currentProcessingTime();
                long watermark = ctx.timerService().currentWatermark();

                System.out.println("本次收到的数据" + eventBean);
                System.out.println("此刻的watermark： " + watermark);
                System.out.println("此刻的处理时间（processing time）： " + processTime );*/

                out.collect(eventBean);
            }
        }).startNewChain().print();


        env.execute();


    }
}

@Data
@NoArgsConstructor
@AllArgsConstructor
class EventBean {
    private long guid;
    private String eventId;
    private long timeStamp;
    private String pageId;
}
