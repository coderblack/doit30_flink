package cn.doitedu.flinksql.demos;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/12
 * @Desc: 学大数据，到多易教育
 * 流  ===>  表  ，过程中如何传承  事件时间  和  watermark
 **/
public class Demo10_KafkaConnectorDetail {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);


        /**
         *  对应的kafka中的数据：
         *      key: {"k1":100,"k2":200}
         *      value: {"guid":1,"eventId":"e02","eventTime":1655017433000,"pageId":"p001"}
         *      headers:
         *          h1 ->  vvvv
         *          h2 ->  tttt
         */
        tenv.executeSql(
                " CREATE TABLE t_kafka_connector (                       "
                        + "     guid   int,                                        "
                        + "     eventId string,                                    "
                        + "     eventTime bigint,                                  "
                        + "     pageId    string,                                  "
                        + "     k1        int,                                     "
                        + "     k2        int,                                     "
                        + " 	rec_ts   timestamp(3) metadata from 'timestamp' ,  "
                        + " 	`offset` bigint metadata ,                         "
                        + " 	headers map<string,bytes> metadata,                "
                        + " 	rt as to_timestamp_ltz(eventTime,3) ,              "
                        + " 	watermark for rt as rt - interval '0.001' second   "
                        + " ) WITH (                                               "
                        + "  'connector' = 'kafka',                                "
                        + "  'topic' = 'doit30-kafka',                             "
                        + "  'properties.bootstrap.servers' = 'doitedu:9092',      "
                        + "  'properties.group.id' = 'testGroup',                  "
                        + "  'scan.startup.mode' = 'earliest-offset',           "
                        + "  'key.format'='json',                               "
                        + "  'key.json.ignore-parse-errors' = 'true',           "
                        + "  'key.fields'='k1;k2',                              "
                        /* + "  'key.fields-prefix'='',                   "     */
                        + "  'value.format'='json',                             "
                        + "  'value.json.fail-on-missing-field'='false',        "
                        + "  'value.fields-include' = 'EXCEPT_KEY'              "
                        + " )                                                   "

        );

        tenv.executeSql("select *  from  t_kafka_connector ")/*.print()*/;
        tenv.executeSql("select guid,eventId,cast(headers['h1'] as string) as h1, cast(headers['h2'] as string) as h2 from  t_kafka_connector ").print();


        env.execute();


    }

}
