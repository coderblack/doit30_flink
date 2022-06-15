package cn.doitedu.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/13
 * @Desc: 学大数据，到多易教育
 *     mysql的cdc连接器使用测试
 **/
public class Demo16_TimeWindowDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // bidtime | price | item | supplier_id |
        DataStreamSource<String> s1 = env.socketTextStream("doit01", 9999);
        SingleOutputStreamOperator<Bid> s2 = s1.map(s -> {
            String[] split = s.split(",");
            return new Bid(split[0], Double.parseDouble(split[1]), split[2], split[3]);
        });

        // 把流变成表
        tenv.createTemporaryView("t_bid",s2, Schema.newBuilder()
                        .column("bidtime", DataTypes.STRING())
                        .column("price", DataTypes.DOUBLE())
                        .column("item", DataTypes.STRING())
                        .column("supplier_id", DataTypes.STRING())
                        .columnByExpression("rt",$("bidtime").toTimestamp())
                        .watermark("rt","rt - interval '1' second")
                .build());

        // 查询
        // tenv.executeSql("select bidtime,price,item,supplier_id,current_watermark(rt) as wm from t_bid").print();

        // 每分钟，计算最近5分钟的交易总额
        tenv.executeSql(
                "select\n" +
                        "  window_start,\n" +
                        "  window_end,\n" +
                        "  sum(price) as price_amt\n" +
                        "from table(\n" +
                        " hop(table t_bid,descriptor(rt), interval '1' minutes, interval '5' minutes)\n" +
                        ")\n" +
                        "group by window_start,window_end"
        )/*.print()*/;


        // 每2分钟计算最近2分钟的交易总额
        tenv.executeSql(
                "select\n" +
                        "  window_start,\n" +
                        "  window_end,\n" +
                        "  sum(price) as price_amt\n" +
                        "from table(\n" +
                        " tumble(table t_bid,descriptor(rt), interval '2' minutes)\n" +
                        ")\n" +
                        "group by window_start,window_end"
        )/*.print()*/;



        // 每2分钟计算今天以来的总交易额
        tenv.executeSql(
                "select\n" +
                        "  window_start,\n" +
                        "  window_end,\n" +
                        "  sum(price) as price_amt\n" +
                        "from table(\n" +
                        " cumulate(table t_bid,descriptor(rt),interval '2' minutes, interval '24' hour)\n" +
                        ")\n" +
                        "group by window_start,window_end"
        )/*.print()*/;



        // 每10分钟计算一次，最近10分钟内交易总额最大的前3个供应商及其交易单数
        tenv.executeSql(
                "select\n" +
                        "  *\n" +
                        "from\n" +
                        "(\n" +
                        "   select\n" +
                        "     window_start,window_end,\n" +
                        "\t supplier_id,\n" +
                        "\t price_amt,\n" +
                        "\t bid_cnt,\n" +
                        "\t row_number() over(partition by window_start,window_end order by price_amt desc) as rn\n" +
                        "   from (\n" +
                        "      select\n" +
                        "        window_start,\n" +
                        "        window_end,\n" +
                        "        supplier_id,\n" +
                        "        sum(price) as price_amt,\n" +
                        "        count(1) as bid_cnt\n" +
                        "      from table( tumble(table t_bid,descriptor(rt),interval '10' minutes) ) \n" +
                        "      group by window_start,window_end,supplier_id\n" +
                        "   )\n" +
                        ") \n" +
                        "where rn<=2"
        )/*.print()*/;


        tenv.executeSql("SELECT\n" +
                "  *\n" +
                "FROM \n" +
                "(\n" +
                "SELECT\n" +
                "   bidtime,\n" +
                "   price,\n" +
                "   item,\n" +
                "   supplier_id,\n" +
                "   row_number() over(partition by window_start,window_end order by price desc) as rn\n" +
                "FROM TABLE(TUMBLE(table t_bid,descriptor(rt),interval '10' minute))\n" +
                ")\n" +
                "WHERE rn<=2").print();


        env.execute();

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bid{
        private String bidtime;    // "2020-04-15 08:05:00.000"
        private double price;
        private String item;
        private String supplier_id;
    }
}
