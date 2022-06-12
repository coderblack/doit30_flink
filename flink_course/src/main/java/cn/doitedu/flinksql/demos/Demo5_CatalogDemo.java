package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/12
 * @Desc: 学大数据，到多易教育
 *        hive元数据空间 catalog 原理测试
 **/
public class Demo5_CatalogDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 环境创建之初，底层会自动初始化一个 元数据空间实现对象（default_catalog => GenericInMemoryCatalog）
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 创建了一个hive元数据空间的实现对象
        HiveCatalog hiveCatalog = new HiveCatalog("hive", "default", "D:\\devworks\\IdeaProjects\\doit30_flink\\conf\\hiveconf");
        // 将hive元数据空间对象注册到 环境中
        tenv.registerCatalog("mycatalog",hiveCatalog);

        tenv.executeSql(
                "create temporary table `mycatalog`.`default`.`t_kafka`    "
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

        tenv.executeSql(
                "create temporary table `t_kafka2`    "
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


        tenv.executeSql("create  view if not exists `mycatalog`.`default`.`t_kafka_view` as select id,name,age from `mycatalog`.`default`.`t_kafka`");


        // 列出当前会话中所有的catalog
        tenv.listCatalogs();

        // 列出 default_catalog中的库和表
        tenv.executeSql("show catalogs").print();
        tenv.executeSql("use catalog default_catalog");
        tenv.executeSql("show databases").print();
        tenv.executeSql("use default_database");
        tenv.executeSql("show tables").print();

        System.out.println("----------------------");

        // 列出 mycatalog中的库和表
        tenv.executeSql("use catalog mycatalog");
        tenv.executeSql("show databases").print();
        tenv.executeSql("use  `default`");
        tenv.executeSql("show tables").print();

        System.out.println("----------------------");

        // 列出临时表
        tenv.listTemporaryTables();
    }
}
