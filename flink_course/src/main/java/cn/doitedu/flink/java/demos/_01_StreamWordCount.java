package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 通过socket数据源，去请求一个socket服务（doit01:9999）得到数据流
 * 然后统计数据流中出现的单词及其个数
 */
public class _01_StreamWordCount {

    public static void main(String[] args) throws Exception {


        // 创建一个编程入口环境
        // ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();   // 批处理的入口环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();  // 流批一体的入口环境


        // 显式声明为本地运行环境，且带webUI
        //Configuration configuration = new Configuration();
        //configuration.setInteger("rest.port", 8081);
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);


        /**
         * 本地运行模式时，程序的默认并行度为 ，你的cpu的逻辑核数
         */
        env.setParallelism(1);  // 默认并行度可以通过env人为指定


        // 通过source算子，把socket数据源加载为一个dataStream（数据流）
        // [root@doit01 ~]# nc -lk 9999
        SingleOutputStreamOperator<String> source = env.socketTextStream("localhost", 9999)
                .setParallelism(1)
                .slotSharingGroup("g1");

        // 然后通过算子对数据流进行各种转换（计算逻辑）
        DataStream<Tuple2<String, Integer>> words = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        // 切单词
                        String[] split = s.split("\\s+");
                        for (String word : split) {
                            // 返回每一对  (单词,1)
                            collector.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                /*.setParallelism(10)
                .slotSharingGroup("g2")
                .shuffle()*/;

        //SingleOutputStreamOperator<Tuple2<String, Integer>> words2 = words.map(tp -> Tuple2.of(tp.f0, tp.f1 * 10));


        KeyedStream<Tuple2<String, Integer>, String> keyed = words.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {

                return tuple2.f0;
            }
        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = keyed.sum("f1")/*.slotSharingGroup("g1")*/;

        // 通过sink算子，将结果输出
        resultStream.print("wcSink");

        // 触发程序的提交运行
        env.execute();


    }
}
