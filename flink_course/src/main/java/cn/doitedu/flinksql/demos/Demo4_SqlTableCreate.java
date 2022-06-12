package cn.doitedu.flinksql.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * 带sql表名的 表创建
 * 各种方式
 */
/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/12
 * @Desc: 学大数据，到多易教育
 *   表创建方式示例：  带sql表名称的
 **/
public class Demo4_SqlTableCreate {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        EnvironmentSettings environmentSettings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, environmentSettings);

        /**
         * 一、  通过构建一个 TableDescriptor 来创建一个  “有名” 表（sql表）
         */
        tenv.createTable("table_a",  // 表名
                TableDescriptor.forConnector("filesystem")
                        .schema(Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .column("age", DataTypes.INT())
                                .column("gender", DataTypes.STRING())
                                .build())
                        .format("csv")
                        .option("path", "data/sqldemo/a.txt")
                        .option("csv.ignore-parse-errors", "true")
                        .build());


        tenv.executeSql("select * from table_a").print();
        System.exit(1);
        tenv.executeSql("select gender,max(age) as max_age from table_a group by gender")/*.print()*/;


        /**
         * 二、 从一个dataStream 上创建“有名”的 视图
         */
        DataStreamSource<String> stream1 = env.socketTextStream("doit01", 9999);
        SingleOutputStreamOperator<Demo3_TableObjectCreate.Person> javaBeanStream = stream1.map(s -> {
            String[] split = s.split(",");
            return new Demo3_TableObjectCreate.Person(Integer.parseInt(split[0]), split[1], Integer.parseInt(split[2]), split[3]);
        });
        tenv.createTemporaryView("t_person", javaBeanStream);
        tenv.executeSql("select gender,max(age) as max_age from t_person group by gender")/*.print()*/;


        /**
         *  三、 从一个已存在Table对象，得到一个 “有名”的视图
         */
        Table table_a = tenv.from("table_a");
        tenv.createTemporaryView("table_x",table_a);
        tenv.executeSql("select * from table_x").print();
    }

}
