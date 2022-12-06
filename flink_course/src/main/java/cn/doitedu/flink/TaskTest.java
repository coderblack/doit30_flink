package cn.doitedu.flink;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class TaskTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();  // 流批一体的入口环境
        env.setParallelism(1);

        SingleOutputStreamOperator<String> st = env.socketTextStream("localhost", 9999)
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                        System.out.println("第1级map收到数据： " + value + "线程号：" + Thread.currentThread().getId());
                        System.out.println("第1级subtask:" + getRuntimeContext().getTaskNameWithSubtasks());

                        // System.out.println("1-  aaa");
                        // System.out.println("1-  bbb");
                        out.collect(value);
                    }
                });

        SingleOutputStreamOperator<String> map = st.process(
                new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                        System.out.println("第2级map算子，收到数据 "+ value + ", 线程" + Thread.currentThread().getId());
                        System.out.println("第2级subtask:" +  getRuntimeContext().getTaskNameWithSubtasks());
                        out.collect(value);
                    }
                }
        ).setParallelism(2);


        map.print();
        env.execute();


    }
}
