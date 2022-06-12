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
 *        json format详解
 **/
public class Demo8_JsonFormat {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);


        /**
         * 一、 简单嵌套json格式，建表示例
         * 嵌套对象解析成 Map类型
         * {"id":12,"name":{"nick":"doe3","formal":"doit edu3","height":170}}
         */
        tenv.executeSql(
                "create table t_json1(                       "
                        + "  id int,                                   "
                        + "  name map<string,string>,                  "
                        + "  bigid as id*10                            "
                        /*+ "  `file.name` string metadata               "*/
                        + ")with(                                      "
                        + " 'connector' = 'filesystem',                "
                        + " 'path' = 'data/json/qiantao/',             "
                        + " 'format'='json'                            "
                        + ")                                           "
        );


        tenv.executeSql("desc t_json1")/*.print()*/;
        tenv.executeSql("select * from t_json1")/*.print()*/;
        // 查询每个人的id和nick
        tenv.executeSql("select id,name['nick'] as nick from t_json1")/*.print()*/;


        /**
         *   二、简单嵌套json建表示例
         *   嵌套对象，解析成Row类型
         *   {"id":12,"name":{"nick":"doe3","formal":"doit edu3","height":170}}
         *
         *   id  int,
         *   name row<nick string,formal string, height int>
         *
         */
        tenv.createTable("t_json2",
                TableDescriptor
                        .forConnector("filesystem")
                        .schema(Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.ROW(
                                        DataTypes.FIELD("nick", DataTypes.STRING()),
                                        DataTypes.FIELD("formal", DataTypes.STRING()),
                                        DataTypes.FIELD("height", DataTypes.INT())
                                ))
                                .build())
                        .format("json")
                        .option("path","data/json/qiantao2")
                        .build());

        tenv.executeSql("desc t_json2")/*.print()*/;
       /*+------+---------------------------------------------------+------+-----+--------+-----------+
        | name |                                              type | null | key | extras | watermark |
        +------+---------------------------------------------------+------+-----+--------+-----------+
        |   id |                                               INT | true |     |        |           |
        | name | ROW<`nick` STRING, `formal` STRING, `height` INT> | true |     |        |           |
        +------+---------------------------------------------------+------+-----+--------+-----------+  */
        tenv.executeSql("select * from t_json2")/*.print()*/;
        // 查询每个人的 id 和 formal名 和  height
        tenv.executeSql("select id, name.formal, name.height  from t_json2")/*.print()*/;




        /**
         * 三、复杂嵌套json，建表示例
         * {"id":1,"friends":[{"name":"a","info":{"addr":"bj","gender":"male"}},{"name":"b","info":{"addr":"sh","gender":"female"}}]}
         */
        tenv.executeSql(
                "create table t_json3(                                      "
                        + "   id  int,                                                "
                        + "   friends array<row<name string,info map<string,string>>> "
                        + ")with(                                      "
                        + " 'connector' = 'filesystem',                "
                        + " 'path' = 'data/json/qiantao3/',            "
                        + " 'format'='json'                            "
                        + ")                                           "
        );

        tenv.executeSql("desc t_json3").print();
        /*
        +---------+-------------------------------------------------------+------+-----+--------+-----------+
        |    name |                                                  type | null | key | extras | watermark |
        +---------+-------------------------------------------------------+------+-----+--------+-----------+
        |      id |                                                   INT | true |     |        |           |
        | friends | ARRAY<ROW<`name` STRING, `info` MAP<STRING, STRING>>> | true |     |        |           |
        +---------+-------------------------------------------------------+------+-----+--------+-----------+
        */
        tenv.executeSql("select * from t_json3")/*.print()*/;
        tenv.executeSql("select id," +
                "friends[1].name as name1,friends[1].info['addr'] as addr1, friends[1].info['gender'] as gender1,   " +
                "friends[2].name as name2,friends[2].info['addr'] as addr2, friends[2].info['gender'] as gender2    " +
                "from  t_json3")/*.print()*/;

    }

}
