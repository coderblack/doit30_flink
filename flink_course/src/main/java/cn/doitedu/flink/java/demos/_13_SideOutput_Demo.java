package cn.doitedu.flink.java.demos;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/4/26
 * @Desc: 测流输出 代码示例（process算子）
 **/
public class _13_SideOutput_Demo {


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);


        // 开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        // 构造好一个数据流
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());


        // 需求： 将行为事件流，进行分流
        //     appLaunch 事件 ，分到一个流
        //     putBack 事件，分到一个流
        //     其他事件保留在主流
        SingleOutputStreamOperator<EventLog> processed = streamSource.process(new ProcessFunction<EventLog, EventLog>() {
            /**
             *
             * @param eventLog  输入数据
             * @param ctx 上下文，它能提供“测输出“功能
             * @param out 主流输出收集器
             * @throws Exception
             */
            @Override
            public void processElement(EventLog eventLog, ProcessFunction<EventLog, EventLog>.Context ctx, Collector<EventLog> out) throws Exception {
                String eventId = eventLog.getEventId();

                if ("appLaunch".equals(eventId)) {

                    ctx.output(new OutputTag<EventLog>("launch", TypeInformation.of(EventLog.class)), eventLog);

                } else if ("putBack".equals(eventId)) {

                    ctx.output(new OutputTag<String>("back",TypeInformation.of(String.class)), JSON.toJSONString(eventLog));
                }

                out.collect(eventLog);

            }
        });

        // 获取  launch 测流数据
        DataStream<EventLog> launchStream = processed.getSideOutput(new OutputTag<EventLog>("launch", TypeInformation.of(EventLog.class)));

        // 获取back 测流数据
        DataStream<String> backStream = processed.getSideOutput(new OutputTag<String>("back",TypeInformation.of(String.class)));

        launchStream.print("launch");

        backStream.print("back");


        env.execute();

    }


}
