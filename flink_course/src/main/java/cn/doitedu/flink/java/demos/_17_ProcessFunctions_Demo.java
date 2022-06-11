package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/26
 * @Desc: process算子及ProcessFunction示例
 *
 * 在不同类型的 数据流上，调用process算子时，所需要传入的ProcessFunction也会有不同
 **/
public class _17_ProcessFunctions_Demo {


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // id,eventId
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9998);

        /**
         * 在普通的datastream上调用process算子，传入的是 "ProcessFunction"
         */
        SingleOutputStreamOperator<Tuple2<String, String>> s1 = stream1.process(new ProcessFunction<String, Tuple2<String, String>>() {
            // 可以使用  生命周期 open 方法
            @Override
            public void open(Configuration parameters) throws Exception {
                // 可以调用 getRuntimeContext 方法拿到各种运行时上下文信息
                RuntimeContext runtimeContext = getRuntimeContext();
                runtimeContext.getTaskName();

                super.open(parameters);
            }

            @Override
            public void processElement(String value, ProcessFunction<String, Tuple2<String, String>>.Context ctx, Collector<Tuple2<String, String>> out) throws Exception {

                // 可以做测流输出
                ctx.output(new OutputTag<String>("s1", Types.STRING),value);

                // 可以做主流输出
                String[] arr = value.split(",");
                out.collect(Tuple2.of(arr[0], arr[1]));
            }

            // 可以使用  生命周期close方法
            @Override
            public void close() throws Exception {
                super.close();
            }
        });



        /**
         * 在 keyedStream上调用 process算子，传入的是 "KeyedProcessFunction"
         * KeyedProcessFunction 中的 ，泛型1： 流中的 key 的类型；  泛型2： 流中的数据的类型  ；  泛型3： 处理后的输出结果的类型
         */
        // 对s1流进行keyby分组
        KeyedStream<Tuple2<String, String>, String> keyedStream = s1.keyBy(tp2 -> tp2.f0);
        // 然后在keyby后的数据流上调用process算子
        SingleOutputStreamOperator<Tuple2<Integer, String>> s2 = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<Integer, String>>() {
            @Override
            public void processElement(Tuple2<String, String> value, KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<Integer, String>>.Context ctx, Collector<Tuple2<Integer, String>> out) throws Exception {
                // 把id变整数，把eventId变大写
                out.collect(Tuple2.of(Integer.parseInt(value.f0), value.f1.toUpperCase()));
            }
        });


        s2.print();


        env.execute();

    }


}
