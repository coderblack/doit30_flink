package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/12
 * @Desc: 学大数据，到多易教育
 *        watermark 在DDL中的定义示例代码
 *
 *    测试数据：
 *    {"guid":1,"eventId":"e02","eventTime":1655017433000,"pageId":"p001"}
 *    {"guid":1,"eventId":"e03","eventTime":1655017434000,"pageId":"p001"}
 *    {"guid":1,"eventId":"e04","eventTime":1655017435000,"pageId":"p001"}
 *    {"guid":1,"eventId":"e05","eventTime":1655017436000,"pageId":"p001"}
 *    {"guid":1,"eventId":"e06","eventTime":1655017437000,"pageId":"p001"}
 *    {"guid":1,"eventId":"e07","eventTime":1655017438000,"pageId":"p001"}
 *    {"guid":1,"eventId":"e08","eventTime":1655017439000,"pageId":"p001"}
 *
 *
 *
 **/
public class Demo9_EventTimeAndWatermark {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);


        /**
         * 只有  TIMESTAMP 或  TIMESTAMP_LTZ 类型的字段可以被声明为rowtime（事件时间属性）
         */
        tenv.executeSql(
                " create table t_events(                                          "
                        + "   guid int,                                                     "
                        + "   eventId string,                                               "
                        /*+ "   eventTime timestamp(3),                                     "*/
                        + "   eventTime bigint,                                             "
                        + "   pageId  string,                                               "
                        + "   pt AS proctime(),                                             "  // 利用一个表达式字段，来声明 processing time属性
                        + "   rt as to_timestamp_ltz(eventTime,3),                          "
                        + "   watermark for rt  as rt - interval '0.001' second             "  // 用watermark for xxx，来将一个已定义的TIMESTAMP/TIMESTAMP_LTZ字段声明成 eventTime属性及指定watermark策略
                        + " )                                                               "
                        + " with (                                                          "
                        + "   'connector' = 'kafka',                                        "
                        + "   'topic' = 'doit30-events2',                                   "
                        + "   'properties.bootstrap.servers' = 'doitedu:9092',              "
                        + "   'properties.group.id' = 'g1',                                 "
                        + "   'scan.startup.mode' = 'earliest-offset',                      "
                        + "   'format' = 'json',                                            "
                        + "   'json.fail-on-missing-field' = 'false',                       "
                        + "   'json.ignore-parse-errors' = 'true'                           "
                        + " )                                                               "
        );

        tenv.executeSql("desc t_events")/*.print()*/;
        tenv.executeSql("select guid,eventId,eventTime,pageId,pt,rt,CURRENT_WATERMARK(rt) as wm from t_events").print();


    }

}
