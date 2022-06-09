package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class _18_ChannalSelector_Partitioner_Demo {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);


        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9999);

        DataStream<String> s2 = s1
                .map(s -> s.toUpperCase())
                .setParallelism(4)
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] arr = value.split(",");
                        for (String s : arr) {
                            out.collect(s);
                        }
                    }
                })
                .setParallelism(4)
                .forward();

        SingleOutputStreamOperator<String> s3 = s2.map(s -> s.toLowerCase()).setParallelism(4);


        SingleOutputStreamOperator<String> s4 = s3.keyBy(s -> s.substring(0, 2))
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String value, KeyedProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(value + ">");
                    }
                }).setParallelism(4);

        DataStream<String> s5 = s4.filter(s -> s.startsWith("b")).setParallelism(4);

        s5.print().setParallelism(4);

        env.execute();
    }

}
