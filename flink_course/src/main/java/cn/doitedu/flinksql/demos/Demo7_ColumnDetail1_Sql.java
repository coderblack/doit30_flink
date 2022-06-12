package cn.doitedu.flinksql.demos;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/11
 * @Desc: schema定义详细示例 （sql DDL语句定义表结构）
 **/
public class Demo7_ColumnDetail1_Sql {
    public static void main(String[] args) {

        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        // 建表（数据源表）
        // {"id":4,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
        tenv.executeSql(
                "create table t_person                                 "
                        + " (                                                   "
                        + "   id int ,                                          "  // -- 物理字段
                        + "   name string,                                      "  // -- 物理字段
                        + "   nick string,                                      "
                        + "   age int,                                          "
                        + "   gender string ,                                   "
                        + "   guid as id,                                       "  // -- 表达式字段（逻辑字段）
                        + "   big_age as age + 10 ,                             "  // -- 表达式字段（逻辑字段）
                        + "   offs  bigint metadata from 'offset' ,             "   // -- 元数据字段
                        + "   ts TIMESTAMP_LTZ(3) metadata from 'timestamp',    "   // -- 元数据字段
                        /*+ "   PRIMARY KEY(id,name) NOT ENFORCED                 "*/    // -- 主键约束
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

        tenv.executeSql("desc t_person").print();
        tenv.executeSql("select * from t_person where id>2").print();


    }
}
