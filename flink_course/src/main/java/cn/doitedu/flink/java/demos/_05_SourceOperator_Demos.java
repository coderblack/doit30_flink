package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.LongValueSequenceIterator;
import org.apache.flink.util.SplittableIterator;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class _05_SourceOperator_Demos {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);  // 默认并行度

        /**
         * 从集合得到数据流
         */
        DataStreamSource<Integer> fromElements = env.fromElements(1, 2, 3, 4, 5);
        fromElements.map(d -> d * 10)/*.print()*/;

        List<String> dataList = Arrays.asList("a", "b", "a", "c");
        // fromCollection方法所返回的source算子，是一个单并行度的source算子
        DataStreamSource<String> fromCollection = env.fromCollection(dataList)/*.setParallelism(5)*/;  // 单并行算子如果显式设置>1的并行度，会抛异常
        fromCollection.map(String::toUpperCase)/*.print()*/;


        // fromParallelCollection所返回的source算子，是一个多并行度的source算子
        DataStreamSource<LongValue> parallelCollection = env.fromParallelCollection(new LongValueSequenceIterator(1, 100), TypeInformation.of(LongValue.class)).setParallelism(2);
        parallelCollection.map(lv -> lv.getValue() + 100)/*.print()*/;

        DataStreamSource<Long> sequence = env.generateSequence(1, 100);
        sequence.map(x -> x - 1)/*.print()*/;

        /**
         * 从 socket 端口获取数据得到数据流
         * socketTextStream方法产生的source算子，是一个单并行度的source算子
         */
        // DataStreamSource<String> socketSource = env.socketTextStream("localhost", 9999);
        // socketSource.print();


        /**
         * 从文件得到数据流
         */
        DataStreamSource<String> fileSource = env.readTextFile("flink_course/data/wc/input/wc.txt", "utf-8");
        fileSource.map(String::toUpperCase)/*.print()*/;


        // FileProcessingMode.PROCESS_ONCE  表示，对文件只读一次，计算一次，然后程序就退出
        // FileProcessingMode.PROCESS_CONTINUOUSLY 表示，会监视着文件的变化，一旦发现文件有变化，则会再次对整个文件进行重新计算
        DataStreamSource<String> fileSource2 = env.readFile(new TextInputFormat(null), "flink_course/data/wc/input/wc.txt", FileProcessingMode.PROCESS_CONTINUOUSLY, 1000);
        fileSource2.map(String::toUpperCase)/*.print()*/;


        /**
         * 引入扩展包 ：  flink-connector-kafka
         * 从kafka中读取数据得到数据流
         */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                // 设置订阅的目标主题
                .setTopics("tp01")

                // 设置消费者组id
                .setGroupId("gp01")

                // 设置kafka服务器地址
                .setBootstrapServers("doit01:9092")

                // 起始消费位移的指定：
                //    OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST) 消费起始位移选择之前所提交的偏移量（如果没有，则重置为LATEST）
                //    OffsetsInitializer.earliest()  消费起始位移直接选择为 “最早”
                //    OffsetsInitializer.latest()  消费起始位移直接选择为 “最新”
                //    OffsetsInitializer.offsets(Map<TopicPartition,Long>)  消费起始位移选择为：方法所传入的每个分区和对应的起始偏移量
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))

                // 设置value数据的反序列化器
                .setValueOnlyDeserializer(new SimpleStringSchema())

                // 开启kafka底层消费者的自动位移提交机制
                //    它会把最新的消费位移提交到kafka的consumer_offsets中
                //    就算把自动位移提交机制开启，KafkaSource依然不依赖自动位移提交机制
                //    （宕机重启时，优先从flink自己的状态中去获取偏移量<更可靠>）
                .setProperty("auto.offset.commit", "true")

                // 把本source算子设置成  BOUNDED属性（有界流）
                //     将来本source去读取数据的时候，读到指定的位置，就停止读取并退出
                //     常用于补数或者重跑某一段历史数据
                // .setBounded(OffsetsInitializer.committedOffsets())

                // 把本source算子设置成  UNBOUNDED属性（无界流）
                //     但是并不会一直读数据，而是达到指定位置就停止读取，但程序不退出
                //     主要应用场景：需要从kafka中读取某一段固定长度的数据，然后拿着这段数据去跟另外一个真正的无界流联合处理
                //.setUnbounded(OffsetsInitializer.latest())

                .build();

        // env.addSource();  //  接收的是  SourceFunction接口的 实现类
        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk-source");//  接收的是 Source 接口的实现类
        streamSource.print();


        env.execute();


    }
}
