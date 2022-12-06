package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/13
 * @Desc: 学大数据，到多易教育
 *     mysql的cdc连接器使用测试
 **/
public class Demo14_MysqlCdcConnector {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 建表
        tenv.executeSql("CREATE TABLE flink_score (\n" +
                "      id INT,\n" +
                "      name string,\n" +
                "      gender string,\n" +
                "      score double,\n" +
                "     PRIMARY KEY(id) NOT ENFORCED\n" +
                "     ) WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'doitedu',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = 'root',\n" +
                "     'database-name' = 'flinktest',\n" +
                "     'table-name' = 'score'\n" +
                ")");

        tenv.executeSql("CREATE TABLE t1 (\n" +
                "      id INT,\n" +
                "      name string,\n" +
                "      PRIMARY KEY(id) NOT ENFORCED\n" +
                "     ) WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'doitedu',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = 'root',\n" +
                "     'database-name' = 'doitedu',\n" +
                "     'table-name' = 't1'\n" +
                ")");

        tenv.executeSql("select * from t1").print();
        System.exit(1);

        // 查询
        tenv.executeSql("select * from flink_score")/*.print()*/;


        tenv.executeSql("select  gender,avg(score) as avg_score  from  flink_score group by gender")/*.print()*/;

        // 建一个目标表，用来存放查询结果： 每种性别中，总分最高的前2个人
        tenv.executeSql(
                "create table flink_rank(\n" +
                        "   gender  string  , \n" +
                        "   name string, \n" +
                        "   score_amt double, \n" +
                        "   rn bigint  , \n" +
                        "   primary key(gender,rn) not enforced  \n" +
                        ") with (\n" +
                        "  'connector' = 'jdbc',\n" +
                        "  'url' = 'jdbc:mysql://doitedu:3306/flinktest',\n" +
                        "  'table-name' = 'score_rank',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = 'root' \n" +
                        ")"
        );


        tenv.executeSql("insert into flink_rank  \n" +
                "SELECT\n" +
                "  gender,\n" +
                "  name,\n" +
                "  score_amt,\n" +
                "  rn\n" +
                "from(\n" +
                "SELECT\n" +
                "  gender,\n" +
                "  name,\n" +
                "  score_amt,\n" +
                "  row_number() over(partition by gender order by score_amt desc) as rn\n" +
                "from \n" +
                "(\n" +
                "SELECT\n" +
                "gender,\n" +
                "name,\n" +
                "sum(score) as score_amt\n" +
                "from flink_score\n" +
                "group by gender,name\n" +
                ") o1\n" +
                ") o2\n" +
                "where rn<=2");
    }
}
