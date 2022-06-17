package cn.doitedu.flinksql.demos;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;


public class Demo23_TableFunction {

    public static void main(String[] args) {

        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

/*
        Table table = tenv.fromValues(DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("phone_numbers", DataTypes.ARRAY(DataTypes.STRING()))),
                Row.of(1, "zs", Expressions.array("138","139","135")),
                Row.of(2, "bb", Expressions.array("135","136"))
        );

        tenv.createTemporaryView("t",table);
        tenv.executeSql("select t.id,t.name,t2.phone_number  from t cross join unnest(phone_numbers) as t2(phone_number)").print();
*/

        Table table = tenv.fromValues(DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("phone_numbers", DataTypes.STRING())),
                Row.of(1, "zs", "13888,137,1354455"),
                Row.of(2, "bb",  "1366688,1374,132224455")
        );
        tenv.createTemporaryView("t",table);


        // 注册函数
        tenv.createTemporarySystemFunction("mysplit",MySplit.class);

        // 展开手机号字符串
        tenv.executeSql("select  *  from  t , lateral  table(mysplit(phone_numbers,',')) as t1(p,l) ")/*.print()*/;
        tenv.executeSql("select  *  from  t  left join lateral  table(mysplit(phone_numbers,',')) as t1(p,l) on true ").print();



    }

    @FunctionHint(output = @DataTypeHint("ROW<phone STRING, length INT>"))
    public static class MySplit extends TableFunction<Row>{

        public void eval(String str,String delimiter){
            for (String s : str.split(delimiter)) {
                collect(Row.of(s,s.length()));
            }
        }
    }

}
