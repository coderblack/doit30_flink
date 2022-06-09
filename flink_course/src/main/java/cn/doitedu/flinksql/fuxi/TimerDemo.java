package cn.doitedu.flinksql.fuxi;


import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 需求场景：
 * 实时监测 用户的行为事件流，如果发现有用户下单事件，则检查下单后30分钟内，该用户是否有订单支付
 * 如果没有支付，则输出一条催支付的信息
 * <p>
 * 关键技术： 定时器功能（定时器就是一个闹钟）
 */
public class TimerDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.socketTextStream("doitedu", 9999);

        SingleOutputStreamOperator<EventBean> stream2 = stream.map(s -> JSON.parseObject(s, EventBean.class));

        stream2
                .keyBy(new KeySelector<EventBean, Long>() {
                    @Override
                    public Long getKey(EventBean value) throws Exception {
                        return value.getGuid();
                    }
                })
                .process(new KeyedProcessFunction<Long, EventBean, String>() {

                    ValueState<Long> timerTimeState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        timerTimeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTimeState", Long.class));
                    }

                    @Override
                    public void processElement(EventBean eventBean, KeyedProcessFunction<Long, EventBean, String>.Context ctx, Collector<String> out) throws Exception {

                        if (eventBean.getEventId().equals("submitOrder")) {
                            // 注册一个定时器，所定的时间在  ： 当前处理时间+30S
                            long timerTime = ctx.timerService().currentProcessingTime() + 30 * 1000L;
                            ctx.timerService().registerProcessingTimeTimer(timerTime);
                            // 将定时器时间，放入状态管理器中
                            timerTimeState.update(timerTime);

                            out.collect("检测到用户：" + ctx.getCurrentKey() + ",下单了,注册了一个定时器： " + timerTimeState.value());
                        }

                        if (eventBean.getEventId().equals("payOrder")) {
                            ctx.timerService().deleteProcessingTimeTimer(timerTimeState.value());
                            out.collect("检测到用户：" + ctx.getCurrentKey() + ",在下单后的30s内已经支付，取消定时器 " + timerTimeState.value());
                        }

                    }

                    /**
                     * 定期器被触发时，会调用的方法
                     */
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Long, EventBean, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        Long guid = ctx.getCurrentKey();
                        out.collect("用户: " + guid + ", 您的订单快超时了，赶紧支付！");
                    }
                })
                .print();


        env.execute();
    }
}
