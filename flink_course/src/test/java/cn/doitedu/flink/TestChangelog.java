package cn.doitedu.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

public class TestChangelog {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStreamSource<String> s1 = env.socketTextStream("192.168.77.128", 9990);
        DataStreamSource<String> s2 = env.socketTextStream("192.168.77.128", 9991);

        SingleOutputStreamOperator<Tuple3<Integer, String, Long>> d1 = s1.map(new MapFunction<String, Tuple3<Integer, String, Long>>() {
            @Override
            public Tuple3<Integer, String, Long> map(String value) throws Exception {
                String[] split = value.split(",");

                return Tuple3.of(Integer.parseInt(split[0]), split[1], Long.parseLong(split[2]));
            }
        });


        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> d2 = s2.map(new MapFunction<String, Tuple3<Integer, Integer, Long>>() {
            @Override
            public Tuple3<Integer, Integer, Long> map(String value) throws Exception {
                String[] split = value.split(",");

                return Tuple3.of(Integer.parseInt(split[0]), Integer.parseInt(split[1]), Long.parseLong(split[2]));
            }
        });


        tenv.createTemporaryView("t1", d1, Schema.newBuilder()
                .columnByExpression("id", "f0")
                .columnByExpression("gender", "f1")
                .columnByExpression("rt", "to_timestamp_ltz(f2,3)")
                .watermark("rt", "rt")
                .build());


        tenv.createTemporaryView("t2", d2, Schema.newBuilder()
                .columnByExpression("id", "f0")
                .columnByExpression("score", "f1")
                .columnByExpression("rt", "to_timestamp_ltz(f2,3)")
                .watermark("rt", "rt")
                .build());

        tenv.executeSql("create temporary view vx as " +
                "select t1.id,t1.gender,t2.score,t1.rt from t1 left join t2 on t1.id=t2.id");

        /*tenv.executeSql("select    " +
                "id,gender,score,rn    " +
                "from (    " +
                "select    " +
                "id,gender,score,    " +
                "row_number() over(partition by gender order by score desc) as rn    " +
                "from vx ) tmp    " +
                "where rn <=3 ").print()*/;

        /*tenv.executeSql("desc vx").print();*/


        /*tenv.executeSql("select    " +
                "id,gender,score,rn    " +
                "from (    " +
                "select    " +
                "id,gender,score,    " +
                "row_number() over(partition by gender,window_start,window_end order by score desc) as rn    " +
                "from table( tumble(table vx,descriptor(rt),interval '5' seconds) ) " +
                ") tmp    " +
                "where rn <=3 ").print()*/;

        /*tenv.executeSql("SELECT    " +
                "  a.id,a.gender,a.rt,b.score " +
                "FROM    " +
                "(SELECT id,gender,rt,window_start,window_end FROM table(tumble(table t1,descriptor(rt),INTERVAL '5' seconds ))) a    " +
                "left join    " +
                "(SELECT id,score,window_start,window_end FROM table(tumble(table t2,descriptor(rt),INTERVAL '5' seconds ))) b    " +
                "on a.window_start=b.window_start and a.window_end=b.window_end and a.id=b.id").print();*/

        /*tenv.executeSql("create temporary view vx2 as SELECT         " +
                "a.gender,a.window_start,a.window_end,max(b.score) as score    " +
                "FROM         " +
                "(SELECT id,gender,rt,window_start,window_end FROM table(tumble(table t1,descriptor(rt),INTERVAL '5' seconds ))) a        " +
                "left join        " +
                "(SELECT id,score,window_start,window_end FROM table(tumble(table t2,descriptor(rt),INTERVAL '5' seconds ))) b         " +
                "on a.window_start=b.window_start and a.window_end=b.window_end and a.id=b.id    " +
                "group by a.gender,a.window_start,a.window_end");*/

        /*tenv.executeSql("SELECT    " +
                "gender,window_start,window_end,rn    " +
                "FROM(    " +
                "SELECT    " +
                "gender,    " +
                "window_start,    " +
                "window_end,    " +
                "row_number() over(partition by gender order by score desc) as rn    " +
                "FROM vx2) tmp    " +
                "WHERE rn<=3").print();*/

        tenv.executeSql("create temporary view vx3 as SELECT         " +
                "a.gender,a.window_start as st,a.window_end as ed,a.rt ,b.score    " +
                "FROM         " +
                "(SELECT id,gender,rt,window_start,window_end FROM table(tumble(table t1,descriptor(rt),INTERVAL '5' seconds ))) a        " +
                "left join        " +
                "(SELECT id,score,window_start,window_end FROM table(tumble(table t2,descriptor(rt),INTERVAL '5' seconds ))) b         " +
                "on a.window_start=b.window_start and a.window_end=b.window_end and a.id=b.id    ");

        tenv.executeSql("SELECT    " +
                "window_start,window_end,sum(score) as score    " +
                "from TABLE(tumble(table vx3,descriptor(rt),interval '10' second) )   " +
                "group by window_start,window_end").print();

        //env.execute();

    }
}
