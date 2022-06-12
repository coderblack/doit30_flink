package cn.doitedu.flinksql.demos;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/11
 * @Desc: schema定义详细示例（tableApi方式定义表结构）
 **/
public class Demo7_ColumnDetail2_TableApi {
    public static void main(String[] args) {

        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        // 建表（数据源表）
        // {"id":4,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
        tenv.createTable("t_person",
                TableDescriptor
                        .forConnector("kafka")
                        .schema(Schema.newBuilder()
                                .column("id", DataTypes.INT())   // column是声明物理字段到表结构中来
                                .column("name", DataTypes.STRING())   // column是声明物理字段到表结构中来
                                .column("nick", DataTypes.STRING())   // column是声明物理字段到表结构中来
                                .column("age", DataTypes.INT())   // column是声明物理字段到表结构中来
                                .column("gender", DataTypes.STRING())   // column是声明物理字段到表结构中来
                                .columnByExpression("guid","id")  // 声明表达式字段
                                /*.columnByExpression("big_age",$("age").plus(10))*/     // 声明表达式字段
                                .columnByExpression("big_age","age + 10")  // 声明表达式字段
                                // isVirtual 是表示： 当这个表被sink表时，该字段是否出现在schema中
                                .columnByMetadata("offs",DataTypes.BIGINT(),"offset",true)  // 声明元数据字段
                                .columnByMetadata("ts",DataTypes.TIMESTAMP_LTZ(3),"timestamp",true)  // 声明元数据字段
                                /*.primaryKey("id","name")*/
                                .build())
                        .format("json")
                        .option("topic","doit30-4")
                        .option("properties.bootstrap.servers","doitedu:9092")
                        .option("properties.group.id","g1")
                        .option("scan.startup.mode","earliest-offset")
                        .option("json.fail-on-missing-field","false")
                        .option("json.ignore-parse-errors","true")
                        .build()
        );

        tenv.executeSql("select * from t_person").print();


    }
}
