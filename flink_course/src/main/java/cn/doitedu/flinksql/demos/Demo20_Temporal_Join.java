package cn.doitedu.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/16
 * @Desc: 学大数据，到多易教育
 * 时态join代码示例
 **/
public class Demo20_Temporal_Join {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        /**
         * 订单Id，币种，金额，订单时间
         * 1,a,100,167438436400
         */
        DataStreamSource<String> s1 = env.socketTextStream("doitedu", 9998);

        SingleOutputStreamOperator<Order> ss1 = s1.map(s -> {
            String[] arr = s.split(",");
            return new Order(Integer.parseInt(arr[0]), arr[1], Double.parseDouble(arr[2]), Long.parseLong(arr[3]));
        });


        // 创建主表（需要声明处理时间属性字段）
        tenv.createTemporaryView("orders", ss1, Schema.newBuilder()
                .column("orderId", DataTypes.INT())
                .column("currency", DataTypes.STRING())
                .column("price", DataTypes.DOUBLE())
                .column("orderTime", DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(orderTime,3)")  // 定义处理时间属性字段
                .watermark("rt","rt")
                .build());


        //tenv.executeSql("select orderId,currency,price,orderTime,rt from orders").print();

        // 创建 temporal 表
        tenv.executeSql("CREATE TABLE currency_rate (\n" +
                "      currency STRING, \n" +
                "      rate double ,  \n" +
                "      update_time bigint ,  \n" +
                "      rt  as to_timestamp_ltz(update_time,3)   ," +
                "      watermark for rt as rt - interval '0' second ," +
                "      PRIMARY KEY(currency) NOT ENFORCED\n" +
                "     ) WITH (  \n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'doitedu',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = 'root',\n" +
                "     'database-name' = 'flinktest',\n" +
                "     'table-name' = 'currency_rate'\n" +
                ")");

        //tenv.executeSql("select * from currency_rate").print();




        // temporal 关联查询
        tenv.executeSql(
                "SELECT                  \n" +
                        "     orders.orderId,      \n" +
                        "     orders.currency,     \n" +
                        "     orders.price,        \n" +
                        "     orders.orderTime,    \n" +
                        "     rate  \n" +
                        "FROM orders   \n" +
                        "LEFT JOIN currency_rate FOR SYSTEM_TIME AS OF orders.rt  \n" +
                        "ON orders.currency = currency_rate.currency"
        ).print();


        env.execute();
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        // 订单Id，币种，金额，订单时间
        public int orderId;
        public String currency;
        public double price;
        public long orderTime;

    }
}
