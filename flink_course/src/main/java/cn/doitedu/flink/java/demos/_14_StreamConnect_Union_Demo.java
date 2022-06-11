package cn.doitedu.flink.java.demos;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/26
 * @Desc: 流的连接connect算子  及   流的关联join算子  代码示例
 **/
public class _14_StreamConnect_Union_Demo {


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // 数字字符流
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9998);
        
        // 字母字符流
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9999);

        /**
         * 流的 connect
         */
        ConnectedStreams<String, String> connectedStreams = stream1.connect(stream2);

        SingleOutputStreamOperator<String> resultStream = connectedStreams.map(new CoMapFunction<String, String, String>() {
            // 共同的状态数据

            String  prefix = "doitedu_";

            /**
             * 对 左流 处理的逻辑
             * @param value
             * @return
             * @throws Exception
             */
            @Override
            public String map1(String value) throws Exception {
                // 把数字*10，再返回字符串
                return  prefix + (Integer.parseInt(value)*10) + "";
            }

            /**
             * 对 右流 处理的逻辑
             * @param value
             * @return
             * @throws Exception
             */
            @Override
            public String map2(String value) throws Exception {

                return prefix + value.toUpperCase();
            }
        });
        /*resultStream.print();*/


        /**
         * 流的 union
         * 参与 union的流，必须数据类型一致
         */
        // stream1.map(Integer::parseInt).union(stream2); // union左右两边的流类型不一致，不通过
        DataStream<String> unioned = stream1.union(stream2);
        unioned.map(s-> "doitedu_"+s).print();


        env.execute();

    }


}
