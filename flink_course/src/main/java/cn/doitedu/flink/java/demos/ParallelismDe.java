package cn.doitedu.flink.java.demos;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ParallelismDe {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Integer> s1 = env.fromElements(1, 2, 3, 4, 5, 6).map(s -> s).setParallelism(3);
        SingleOutputStreamOperator<Integer> s2 = env.fromElements(11, 12, 13, 14, 15, 16).map(s -> s).setParallelism(5);
        DataStream<Integer> s3 = s2.union(s1);
        System.out.println(s3.getParallelism());


    }
}
