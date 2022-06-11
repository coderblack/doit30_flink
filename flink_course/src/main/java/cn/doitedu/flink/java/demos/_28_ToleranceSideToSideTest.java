package cn.doitedu.flink.java.demos;

import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/5/8
 * @Desc: flink的 端到端精确一次 容错能力测试
 * <p>
 * 从 kafka 读数据（里面有operator-state状态）
 * <p>
 * 处理过程中用到了带状态的 map算子（里面用了keyed-State状态，逻辑：输入一个字符串，变大写拼接此前字符串，输出）
 * <p>
 * 用 exactly-once 的 mysql-sink算子输出数据（并附带主键的幂等特性）
 * <p>
 * 测试用的 kafka-topic ：
 * [root@doit01 ~]# kafka-topics.sh --create --topic eos --partitions 1 --replication-factor 1 --zookeeper doit01:2181
 * <p>
 * 测试用的输入数据：
 * a
 * b
 * c
 * d
 * <p>
 * <p>
 * 测试用的mysql表：
 * CREATE TABLE `t_eos` (
 * `str` varchar(255) NOT NULL,
 * PRIMARY KEY (`str`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 **/
public class _28_ToleranceSideToSideTest {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        //conf.setString("execution.savepoint.path", "file:///D:/checkpoint/7ecbd4f9106957c42109bcde/chk-544");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        /* *
         * checkpoint 容错相关参数设置
         */
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/eos_ckpt");

        /* *
         * task级别故障自动重启策略
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.milliseconds(1000)));


        /* *
         * 状态后端设置 ，默认 ：  HashMapStateBackend
         */
        env.setStateBackend(new HashMapStateBackend());


        /**
         * 构造一个支持eos语义的  kafkasource
         */
        KafkaSource<String> sourceOperator = KafkaSource.<String>builder()
                .setBootstrapServers("doit01:9092")
                .setTopics("eos")
                .setGroupId("eos01")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")  // 允许 kafkaconsumer自动提交消费位移到 __consumer_offsets
                // kafkaSource的做状态checkpoint时，默认会向__consumer_offsets提交一下状态中记录的偏移量
                // 但是，flink的容错并不优选依赖__consumer_offsets中的记录，所以可以关闭该默认机制
                .setProperty("commit.offsets.on.checkpoint", "false") // 默认是true
                // kafkaSource启动时，获取起始位移的策略设置，如果是 committedOffsets ，则是从之前所记录的偏移量开始
                // 如果没有可用的之前记录的偏移量, 则用策略 OffsetResetStrategy.LATEST 来决定
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .build();

        /**
         * 构造一个支持精确一次的 jdbcSink
         */
        SinkFunction<String> exactlyOnceJdbcSink = JdbcSink.exactlyOnceSink(
                "insert into t_eos values (?) on duplicate key update str = ? ",
                new JdbcStatementBuilder<String>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, String s) throws SQLException {
                        preparedStatement.setString(1, s);
                        preparedStatement.setString(2, s);
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(1)
                        .build(),
                JdbcExactlyOnceOptions.builder()
                        .withTransactionPerConnection(true) // mysql不支持同一个连接上存在并行的多个未完成的事务，必须把该参数设置为true
                        .build(),
                new SerializableSupplier<XADataSource>() {
                    @Override
                    public XADataSource get() {
                        // XADataSource就是jdbc连接，不过它是支持分布式事务的连接
                        // 而且它的构造方法，不同的数据库构造方法不同
                        MysqlXADataSource xaDataSource = new MysqlXADataSource();
                        xaDataSource.setUrl("jdbc:mysql://doit01:3306/abc");
                        xaDataSource.setUser("root");
                        xaDataSource.setPassword("ABC123.abc123");
                        return xaDataSource;
                    }
                }
        );


        /* *
         * 数据处理逻辑
         */
        DataStreamSource<String> stream1 = env.fromSource(sourceOperator, WatermarkStrategy.noWatermarks(), "kfksource");

        SingleOutputStreamOperator<String> stream2 = stream1.keyBy(s -> "group1")
                .map(new RichMapFunction<String, String>() {
                    ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("preStr", String.class));
                    }

                    @Override
                    public String map(String element) throws Exception {

                        // 从状态中取出上一条字符串
                        String preStr = valueState.value();
                        if (preStr == null) preStr = "";

                        // 更新状态
                        valueState.update(element);

                        // 埋一个异常：当接收到 x 的时候，有 1/3 的概率异常
                        if(element.equals("x") && RandomUtils.nextInt(0,4)%3==0){
                            throw new Exception("发生异常了.............");
                        }

                        return preStr + ":" + element.toUpperCase();
                    }
                });


        stream2.addSink(exactlyOnceJdbcSink);

        env.execute();


    }
}
