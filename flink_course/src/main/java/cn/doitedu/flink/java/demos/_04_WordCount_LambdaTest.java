package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _04_WordCount_LambdaTest {

    public static void main(String[] args) throws Exception {

        // 创建一个编程入口（执行环境）

        // 流式处理入口环境
        StreamExecutionEnvironment envStream = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = envStream.readTextFile("flink_course/data/wc/input/wc.txt");

        // 先把句子变大写
        /* 从map算子接收的MapFunction接口实现来看，它是一个单抽象方法的接口
        所以这个接口的实现类的核心功能，就在它的方法上
        那就可以用lambda表达式来简洁实现
        streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return null;
            }
        });*/

        /**
         * lambda表达式怎么写，看你要实现的那个接口的方法接收什么参数，返回什么结果
         */
        // 然后就按lambda语法来表达：  (参数1,参数2,...) -> { 函数体 }
        // streamSource.map( (value) -> { return  value.toUpperCase();});

        // 由于上面的lambda表达式，参数列表只有一个，且函数体只有一行代码，则可以简化
        // streamSource.map( value ->  value.toUpperCase() ) ;

        // 由于上面的lambda表达式， 函数体只有一行代码，且参数只使用了一次，可以把函数调用转成  “方法引用”
        SingleOutputStreamOperator<String> upperCased = streamSource.map(String::toUpperCase);

        // 然后切成单词，并转成（单词,1），并压平
        /*upperCased.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

            }
        });*/
        // 从上面的接口来看，它依然是一个   单抽象方法的 接口，所以它的方法实现，依然可以用lambda表达式来实现
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = upperCased.flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
                    String[] words = s.split("\\s+");
                    for (String word : words) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                })
                // .returns(new TypeHint<Tuple2<String, Integer>>() {});   // 通过 TypeHint 传达返回数据类型
                // .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));  // 更通用的，是传入TypeInformation,上面的TypeHint也是封装了TypeInformation
                .returns(Types.TUPLE(Types.STRING, Types.INT));  // 利用工具类Types的各种静态方法，来生成TypeInformation


        // 按单词分组
        /*wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return null;
            }
        })*/
        // 从上面的KeySelector接口来看，它依然是一个 单抽象方法的 接口，所以它的方法实现，依然可以用lambda表达式来实现
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOne.keyBy((value) -> value.f0);


        // 统计单词个数
        keyedStream.sum(1)
                .print();


        envStream.execute();


    }
}
