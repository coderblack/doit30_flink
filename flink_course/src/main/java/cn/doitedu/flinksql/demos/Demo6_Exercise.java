package cn.doitedu.flinksql.demos;


import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 *

 *
 */
/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/12
 * @Desc: 学大数据，到多易教育
 * >>>>> 练习题需求 >>>>>>>
 *     基本： kafka中有如下数据：
 *         {"id":1,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
 *
 *     高级：kafka中有如下数据：
 *      {"id":1,"name":{"formal":"zs","nick":"tiedan"},"age":18,"gender":"male"}
 *
 *      现在需要用flinkSql来对上述数据进行查询统计：
 *        截止到当前,每个昵称,都有多少个用户
 *        截止到当前,每个性别,年龄最大值
 **/
public class Demo6_Exercise {
    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        // 建表（数据源表）
        tenv.executeSql(
                "create table t_person                                 "
                        + " (                                                   "
                        + "   id int,                                           "
                        + "   name string,                                      "
                        + "   nick string,                                      "
                        + "   age int,                                          "
                        + "   gender string                                     "
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'doit30-4',                              "
                        + "  'properties.bootstrap.servers' = 'doitedu:9092',   "
                        + "  'properties.group.id' = 'g1',                      "
                        + "  'scan.startup.mode' = 'earliest-offset',           "
                        + "  'format' = 'json',                                 "
                        + "  'json.fail-on-missing-field' = 'false',            "
                        + "  'json.ignore-parse-errors' = 'true'                "
                        + " )                                                   "
        );


        // 建表（目标表）
        // kafka 连接器，不能接受  UPDATE 修正模式的数据，只能接受INSERT模式的数据
        // 而我们的查询语句产生的结果，存在UPDATE模式，就需要另一种 连接器表（upsert-kafka）来接收
        tenv.executeSql(
                "create table t_nick_cnt                               "
                        + " (                                                   "
                        + "   nick string primary key not enforced,             "
                        + "   user_cnt bigint                                   "
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'upsert-kafka',                      "
                        + "  'topic' = 'doit30-nick',                           "
                        + "  'properties.bootstrap.servers' = 'doitedu:9092',   "
                        + "  'key.format' = 'json' ,                            "
                        + "  'value.format' = 'json'                            "
                        + " )                                                   "
        );


        // 查询 并 打印
        //TableResult tableResult = tenv.executeSql("select nick,count(distinct id) as user_cnt from t_person group by nick");
        tenv.executeSql(
                "insert into t_nick_cnt " +
                "select nick,count(distinct id) as user_cnt from t_person group by nick");

    }

}
