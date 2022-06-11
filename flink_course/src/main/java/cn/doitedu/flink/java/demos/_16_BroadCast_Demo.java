package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/26
 * @Desc: 广播流及广播状态的使用示例
 **/
public class _16_BroadCast_Demo {


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // id,eventId
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
         * 案例背景：
         *    流 1：  用户行为事件流（持续不断，同一个人也会反复出现，出现次数不定
         *    流 2：  用户维度信息（年龄，城市），同一个人的数据只会来一次，来的时间也不定 （作为广播流）
         *
         *    需要加工流1，把用户的维度信息填充好，利用广播流来实现
         */

        // 将字典数据所在流： s2  ，  转成 广播流
        MapStateDescriptor<String, Tuple2<String, String>> userInfoStateDesc = new MapStateDescriptor<>("userInfoStateDesc", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));
        BroadcastStream<Tuple3<String, String, String>> s2BroadcastStream = s2.broadcast(userInfoStateDesc);

        // 哪个流处理中需要用到广播状态数据，就要 去  连接 connect  这个广播流
        BroadcastConnectedStream<Tuple2<String, String>, Tuple3<String, String, String>> connected = s1.connect(s2BroadcastStream);


        /**
         *   对 连接了广播流之后的 ”连接流“ 进行处理
         *   核心思想：
         *      在processBroadcastElement方法中，把获取到的广播流中的数据，插入到 “广播状态”中
         *      在processElement方法中，对取到的主流数据进行处理（从广播状态中获取要拼接的数据，拼接后输出）
         */
        SingleOutputStreamOperator<String> resultStream = connected.process(new BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {

            /*BroadcastState<String, Tuple2<String, String>> broadcastState;*/

            /**
             * 本方法，是用来处理 主流中的数据（每来一条，调用一次）
             * @param element  左流（主流）中的一条数据
             * @param ctx  上下文
             * @param out  输出器
             * @throws Exception
             */
            @Override
            public void processElement(Tuple2<String, String> element, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {

                // 通过 ReadOnlyContext ctx 取到的广播状态对象，是一个 “只读 ” 的对象；
                ReadOnlyBroadcastState<String, Tuple2<String, String>> broadcastState = ctx.getBroadcastState(userInfoStateDesc);

                if (broadcastState != null) {
                    Tuple2<String, String> userInfo = broadcastState.get(element.f0);
                    out.collect(element.f0 + "," + element.f1 + "," + (userInfo == null ? null : userInfo.f0) + "," + (userInfo == null ? null : userInfo.f1));
                } else {
                    out.collect(element.f0 + "," + element.f1 + "," + null + "," + null);
                }

            }

            /**
             *
             * @param element  广播流中的一条数据
             * @param ctx  上下文
             * @param out 输出器
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(Tuple3<String, String, String> element, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.Context ctx, Collector<String> out) throws Exception {

                // 从上下文中，获取广播状态对象（可读可写的状态对象）
                BroadcastState<String, Tuple2<String, String>> broadcastState = ctx.getBroadcastState(userInfoStateDesc);

                // 然后将获得的这条  广播流数据， 拆分后，装入广播状态
                broadcastState.put(element.f0, Tuple2.of(element.f1, element.f2));
            }
        });


        resultStream.print();


        env.execute();

    }


}
