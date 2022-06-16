package cn.doitedu.flinksql.demos;

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
 * 常规join示例
 * 常规join的底层实现，是通过在用状态来缓存两表数据实现的
 * 所以，状态体积可能持续膨胀，为了安全起见，可以设置状态的 ttl 时长，来控制状态的体积上限
 **/
public class Demo19_LookupJoin {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        // 设置table环境中的状态ttl时长
        tenv.getConfig().getConfiguration().setLong("table.exec.state.ttl", 60 * 60 * 1000L);


        /**
         * 1,a
         * 2,b
         * 3,c
         * 4,d
         * 5,e
         */
        DataStreamSource<String> s1 = env.socketTextStream("doitedu", 9998);
        SingleOutputStreamOperator<Tuple2<Integer, String>> ss1 = s1.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.of(Integer.parseInt(arr[0]), arr[1]);
        }).returns(new TypeHint<Tuple2<Integer, String>>() {
        });


        // 创建主表（需要声明处理时间属性字段）
        tenv.createTemporaryView("a", ss1, Schema.newBuilder()
                .column("f0", DataTypes.INT())
                .column("f1", DataTypes.STRING())
                .columnByExpression("pt", "proctime()")  // 定义处理时间属性字段
                .build());

        // 创建lookup维表（jdbc connector表）
        tenv.executeSql(
                "create table b(   \n" +
                        "   id  int  , \n" +
                        "   name string, \n" +
                        "   gender STRING, \n" +
                        "   primary key(id) not enforced  \n" +
                        ") with (\n" +
                        "  'connector' = 'jdbc',\n" +
                        "  'url' = 'jdbc:mysql://doitedu:3306/flinktest',\n" +
                        "  'table-name' = 'stu2',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = 'root' \n" +
                        ")"
        );

        // lookup join 查询
        tenv.executeSql("select a.*,c.*   from  a  JOIN  b FOR SYSTEM_TIME AS OF a.pt AS c  \n" +
                "    ON a.f0 = c.id").print();


        env.execute();
    }
}
