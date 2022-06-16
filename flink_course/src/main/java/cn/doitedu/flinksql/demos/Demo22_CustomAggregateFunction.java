package cn.doitedu.flinksql.demos;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/16
 * @Desc: 学大数据，到多易教育
 *   自定义聚合函数
 **/
public class Demo22_CustomAggregateFunction {
    public static void main(String[] args) {

        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        Table table = tenv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("uid", DataTypes.INT()),
                        DataTypes.FIELD("gender", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.DOUBLE())
                ),
                Row.of(1,"male",80),
                Row.of(2,"male",100),
                Row.of(3,"female",90)
        );

        tenv.createTemporaryView("t",table);

        // 注册自定义的函数
       tenv.createTemporarySystemFunction("myavg",MyAvg.class);

        // 注册后，就能在sql中使用了
        tenv.executeSql("select gender,myavg(score) as avg_score  from t group by gender ").print();

    }


    public static class MyAccumulator{
        public int count;
        public double sum;
    }

    public static class MyAvg extends AggregateFunction<Double,MyAccumulator> {

        /**
         * 获取累加器的值
         * @param accumulator the accumulator which contains the current intermediate results
         * @return
         */
        @Override
        public Double getValue(MyAccumulator accumulator) {
            return accumulator.sum/ accumulator.count;
        }

        /**
         *  创建累加器
         * @return
         */
        @Override
        public MyAccumulator createAccumulator() {
            MyAccumulator myAccumulator = new MyAccumulator();
            myAccumulator.count = 0;
            myAccumulator.sum = 0;


            return myAccumulator;
        }


        /**
         * 进来输入数据后，如何更新累加器
         * @param accumulator
         * @param score
         */
        public void accumulate(MyAccumulator accumulator,Double score){

            accumulator.count = accumulator.count + 1;
            accumulator.sum = accumulator.sum + score;

        }


    }

}
