package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/26
 * @Desc: coGroup协同分组算子代码示例
 **/
public class _15_StreamCoGroup_Join_Demo {


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // id,name
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9998);

        SingleOutputStreamOperator<Tuple2<String, String>> s1 = stream1.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.of(arr[0], arr[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {
        });

        // id,age,city
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple3<String, String, String>> s2 = stream2.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1], arr[2]);
        }).returns(new TypeHint<Tuple3<String, String, String>>() {
        });

        /**
         * 流的 cogroup
         * 案例背景：
         *    流1数据：  id,name
         *    流2数据：  id,age,city
         *    利用coGroup算子，来实现两个流的数据按id相等进行窗口关联（包含inner ，left， right， outer）
         */
        DataStream<String> resultStream = s1.coGroup(s2)
                .where(tp -> tp.f0)  // 左流的  f0 字段
                .equalTo(tp -> tp.f0)   // 右流的 f0 字段
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))  // 划分窗口
                .apply(new CoGroupFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {
                    /**
                     *
                     * @param first  是协同组中的第一个流的数据
                     * @param second 是协同组中的第二个流的数据
                     * @param out 是处理结果的输出器
                     * @throws Exception
                     */
                    @Override
                    public void coGroup(Iterable<Tuple2<String, String>> first, Iterable<Tuple3<String, String, String>> second, Collector<String> out) throws Exception {

                        // 在这里实现  left out  join
                        for (Tuple2<String, String> t1 : first) {
                            boolean flag = false;
                            for (Tuple3<String, String, String> t2 : second) {
                                // 拼接两表字段输出
                                out.collect(t1.f0 + "," + t1.f1 + "," + t2.f0 + "," + t2.f1 + "," + t2.f2);
                                flag = true;
                            }

                            if (!flag) {
                                // 如果能走到这里面，说明右表没有数据，则直接输出左表数据
                                out.collect(t1.f0 + "," + t1.f1 + "," + null + "," + null + "," + null);
                            }
                        }

                        // TODO  实现以下 right out join


                        // TODO 实现  full out join


                        // TODO  实现  inner join


                    }
                });
        /*resultStream.print();*/

        /**
         * 流的 join 算子
         * 案例背景：
         *    流1数据：  id,name
         *    流2数据：  id,age,city
         *    利用join算子，来实现两个流的数据按id关联
         */
        DataStream<String> joinedStream = s1.join(s2)
                .where(tp2 -> tp2.f0)
                .equalTo(tp3 -> tp3.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {
                    @Override
                    public String join(Tuple2<String, String> t1, Tuple3<String, String, String> t2) throws Exception {
                        return t1.f0 + "," + t1.f1 + "," + t2.f0 + "," + t2.f1 + "," + t2.f2;
                    }
                });

        joinedStream.print();



        env.execute();

    }


}
