package cn.doitedu.flinksql.demos;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.HashMap;

import static org.apache.flink.table.api.Expressions.$;

public class Demo1_TableSql {

    public static void main(String[] args) throws DatabaseAlreadyExistException {

        EnvironmentSettings envSettings = EnvironmentSettings.inStreamingMode();  // 流计算模式
        TableEnvironment tableEnv = TableEnvironment.create(envSettings);

        // 把kafka中的一个topic： doit30-2 数据，映射成一张flinkSql表
        // json :  {"id":1,"name":"zs","age":28,"gender":"male"}
        // create table_x (id int,name string,age int,gender string)
        tableEnv.executeSql(
                "create table t_kafka                                  "
                        + " (                                                   "
                        + "   id int,                                           "
                        + "   name string,                                      "
                        + "   age int,                                          "
                        + "   gender string                                     "
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'doit30-3',                              "
                        + "  'properties.bootstrap.servers' = 'doitedu:9092',   "
                        + "  'properties.group.id' = 'g1',                      "
                        + "  'scan.startup.mode' = 'earliest-offset',           "
                        + "  'format' = 'json',                                 "
                        + "  'json.fail-on-missing-field' = 'false',            "
                        + "  'json.ignore-parse-errors' = 'true'                "
                        + " )                                                   "
        );


        /**
         * 把sql表名， 转成 table对象
         */
        Table table = tableEnv.from("t_kafka");
        // 利用table api进行查询计算
        table.groupBy($("gender"))
                .select($("gender"), $("age").avg())
                .execute()
                .print();


        tableEnv.executeSql("select gender,avg(age) as avg_age  from  t_kafka group by gender").print();


    }
}
