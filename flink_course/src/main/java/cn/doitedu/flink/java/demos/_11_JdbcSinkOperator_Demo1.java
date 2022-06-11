package cn.doitedu.flink.java.demos;

import com.alibaba.fastjson.JSON;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/26
 * @Desc: 将数据流写入 mysql，利用 JdbcSink算子
 **/
public class _11_JdbcSinkOperator_Demo1 {


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);


        // 开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        // 构造好一个数据流
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        /**
         *  一、 不保证 EOS语义的方式
         */
        SinkFunction<EventLog> jdbcSink = JdbcSink.sink(
                "insert into t_eventlog values (?,?,?,?,?) on duplicate key update guid=?,sessionId=?,eventId=?,ts=?,eventInfo=? ",
                new JdbcStatementBuilder<EventLog>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, EventLog eventLog) throws SQLException {
                        preparedStatement.setLong(1, eventLog.getGuid());
                        preparedStatement.setString(2, eventLog.getSessionId());
                        preparedStatement.setString(3, eventLog.getEventId());
                        preparedStatement.setLong(4, eventLog.getTimeStamp());
                        preparedStatement.setString(5, JSON.toJSONString(eventLog.getEventInfo()));

                        preparedStatement.setString(6, eventLog.getSessionId());
                        preparedStatement.setString(7, eventLog.getEventId());
                        preparedStatement.setLong(8, eventLog.getTimeStamp());
                        preparedStatement.setString(9, JSON.toJSONString(eventLog.getEventInfo()));
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://doit01:3306/abc?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("ABC123.abc123")
                        .build()
        );

        // 输出数据
        /*streamSource.addSink(jdbcSink);*/


        /**
         * 二、可以提供 EOS 语义保证的 sink
         */
        SinkFunction<EventLog> exactlyOnceSink = JdbcSink.exactlyOnceSink(
                "insert into t_eventlog values (?,?,?,?,?) on duplicate key update guid=?,sessionId=?,eventId=?,ts=?,eventInfo=? ",
                new JdbcStatementBuilder<EventLog>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, EventLog eventLog) throws SQLException {
                        preparedStatement.setLong(1, eventLog.getGuid());
                        preparedStatement.setString(2, eventLog.getSessionId());
                        preparedStatement.setString(3, eventLog.getEventId());
                        preparedStatement.setLong(4, eventLog.getTimeStamp());
                        preparedStatement.setString(5, JSON.toJSONString(eventLog.getEventInfo()));

                        preparedStatement.setString(6, eventLog.getSessionId());
                        preparedStatement.setString(7, eventLog.getEventId());
                        preparedStatement.setLong(8, eventLog.getTimeStamp());
                        preparedStatement.setString(9, JSON.toJSONString(eventLog.getEventInfo()));
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(1)
                        .build(),
                JdbcExactlyOnceOptions.builder()
                        // mysql不支持同一个连接上存在并行的多个事务，必须把该参数设置为true
                        .withTransactionPerConnection(true)
                        .build(),
                new SerializableSupplier<XADataSource>() {
                    @Override
                    public XADataSource get() {
                        // XADataSource就是jdbc连接，不过它是支持分布式事务的连接
                        // 而且它的构造方法，不同的数据库构造方法不同
                        MysqlXADataSource xaDataSource = new MysqlXADataSource();
                        xaDataSource.setUrl("jdbc:mysql://doit01:3306/abc?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8");
                        xaDataSource.setUser("root");
                        xaDataSource.setPassword("ABC123.abc123");
                        return xaDataSource;
                    }
                }
        );

        // 输出数据
        streamSource.addSink(exactlyOnceSink);

        env.execute();
    }
}
