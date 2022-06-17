package cn.doitedu.flinksql.demos;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/17
 * @Desc: 学大数据，到多易教育
 * 自定义表聚合函数示例
 * 什么叫做表聚合函数：
 * 1,male,zs,88
 * 2,male,bb,99
 * 3,male,cc,76
 * 4,female,dd,78
 * 5,female,ee,92
 * 6,female,ff,86
 * <p>
 * -- 求每种性别中，分数最高的两个学生
 * -- 常规写法
 * SELECT
 * *
 * FROM
 * (
 * SELECT
 * id,
 * gender,
 * name,
 * score,
 * row_number() over(partition by gender order by score desc) as rn
 * FROM  t
 * )
 * where rn<=2
 * <p>
 * <p>
 * -- 如果有一种聚合函数，能在分组聚合的模式中，对每组数据输出多行多列聚合结果
 * SELECT
 * id,
 * name,
 * gender,
 * score,
 * funciont_xxx(score,2)
 * from t
 * group by gender
 * <p>
 * 1,male,zs,88
 * 2,male,bb,99
 * 5,female,ee,92
 * 6,female,ff,86
 **/
public class Demo24_TableAggregateFunction2 {

    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        Table table = tenv.fromValues(DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("gender", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.DOUBLE())),
                Row.of(1, "male", 67),
                Row.of(2, "male", 88),
                Row.of(3, "male", 98),
                Row.of(4, "female", 99),
                Row.of(5, "female", 84),
                Row.of(6, "female", 89)
        );
        tenv.createTemporaryView("t", table);

        // 用一个聚合函数直接求出每种性别中成绩最高的2个人
        table
                .groupBy($("gender"))
                .flatAggregate(call(MyTop2.class, row($("id"), $("gender"), $("score"))))
                .select($("id"), $("gender"), $("score"))
                .execute().print();


    }

    public static class MyAccumulator {

        public @DataTypeHint("ROW<id INT,rgender STRING,score DOUBLE>") Row first;
        public @DataTypeHint("ROW<id INT,rgender STRING,score DOUBLE>") Row second;

    }

    @FunctionHint(input =@DataTypeHint("ROW<id INT,gender STRING,score DOUBLE>") ,output = @DataTypeHint("ROW<id INT,rgender STRING,score DOUBLE>"))
    public static class MyTop2 extends TableAggregateFunction<Row, MyAccumulator> {

        @Override
        public MyAccumulator createAccumulator() {

            MyAccumulator acc = new MyAccumulator();
            acc.first = null;
            acc.second = null;

            return acc;
        }


        /**
         * 累加更新逻辑
         *
         * @param acc
         * @param value
         */
        public void accumulate(MyAccumulator acc, Row row) {

            double score = (double) row.getField(2);
            if (acc.first == null || score > (double)acc.first.getField(2)) {
                acc.second = acc.first;
                acc.first = row;
            } else if (acc.second == null || score > (double)acc.second.getField(2)) {
                acc.second = row;
            }
        }

        public void merge(MyAccumulator acc, Iterable<MyAccumulator> it) {
            for (MyAccumulator otherAcc : it) {
                accumulate(acc, otherAcc.first);
                accumulate(acc, otherAcc.second);
            }
        }

        /**
         * 输出结果： 可以输出多行，多列
         *
         * @param acc
         * @param out
         */
        public void emitValue(MyAccumulator acc, Collector<Row> out) {
            if (acc.first != null) {
                out.collect(acc.first);
            }
            if (acc.second != null) {
                out.collect(acc.second);
            }
        }
    }


}
