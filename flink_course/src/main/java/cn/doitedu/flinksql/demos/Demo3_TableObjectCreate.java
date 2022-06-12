package cn.doitedu.flinksql.demos;


import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;


/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/12
 * @Desc: 学大数据，到多易教育
 *        Table对象创建方式示例代码
 **/
public class Demo3_TableObjectCreate {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        /* 假设  table_a 已经被创建过
        tenv.executeSql("create table table_a(id int,name string) " +
                "with ('connector'='kafka'," +
                "......");*/


        /**
         * 一、从一个已存在的表名，来创建Table对象
         */
        // Table table_a = tenv.from("table_a");



        /**
         * 二、从一个TableDescriptor来创建table对象
         */
        Table table = tenv.from(TableDescriptor
                .forConnector("kafka")  // 指定连接器
                .schema(Schema.newBuilder()  // 指定表结构
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("gender", DataTypes.STRING())
                        .build())
                .format("json")  // 指定数据源的数据格式
                .option("topic", "doit30-3")  // 连接器及format格式的相关参数
                .option("properties.bootstrap.servers", "doit01:9092")
                .option("properties.group.id", "g2")
                .option("scan.startup.mode", "earliest-offset")
                .option("json.fail-on-missing-field", "false")
                .option("json.ignore-parse-errors", "true")
                .build());


        /**
         * 三、从数据流来创建table对象
         */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("doit01:9092")
                .setTopics("doit30-3")
                .setGroupId("g2")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();


        DataStreamSource<String> kfkStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk");
        /*kfk.print();*/


        // 3.1 不指定schema，将流创建成Table对象，表的schema是默认的，往往不符合我们的要求
        Table table1 = tenv.fromDataStream(kfkStream);
        /*table1.execute().print();*/

        // 3.2 为了获得更理想的表结构，可以先把数据流中的数据转成javabean类型
        SingleOutputStreamOperator<Person> javaBeanStream = kfkStream.map(json -> JSON.parseObject(json, Person.class));
        Table table2 = tenv.fromDataStream(javaBeanStream);
        /*table2.execute().print();*/

        // 3.3 手动指定 schema定义，来将一个javabean流，转成Table对象
        Table table3 = tenv.fromDataStream(javaBeanStream,
                Schema.newBuilder()
                        .column("id",DataTypes.BIGINT())
                        .column("name",DataTypes.STRING())
                        .column("age",DataTypes.INT())
                        .column("gender",DataTypes.STRING())
                        .build());
        /*table3.printSchema();
        table3.execute().print();*/


        /**
         * 四、用测试数据来得到一个表对象
         */
        // 4.1 单字段数据建测试表
        Table table4 = tenv.fromValues(1, 2, 3, 4, 5);
        /*table4.printSchema();
        table4.execute().print();*/

        // 4.2 多字段数据建测试表
        Table table5 = tenv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id",DataTypes.INT()),
                        DataTypes.FIELD("name",DataTypes.STRING()),
                        DataTypes.FIELD("age",DataTypes.DOUBLE())
                ),
                Row.of(1, "zs", 18.2),
                Row.of(2, "bb", 28.2),
                Row.of(3, "cc", 16.2),
                Row.of(4, "dd", 38.2)
        );
        table5.printSchema();
        table5.execute().print();

        env.execute();
    }

    @NoArgsConstructor
    @AllArgsConstructor
    public static class Person{
        public int id;
        public String name;
        public int age;
        public String gender;
    }

}
