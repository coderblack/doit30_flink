package cn.doitedu.flinksql.demos;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

public class Demo25_MetricDemos {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> ds = env.socketTextStream("doitedu", 9999);

        ds.process(new ProcessFunction<String, String>() {
            LongCounter longCounter;

            MyGuage gauge ;
            @Override
            public void open(Configuration parameters) throws Exception {

                longCounter = getRuntimeContext().getLongCounter("doitedu-counter1");


                gauge = getRuntimeContext().getMetricGroup().gauge("doitedu-gauge", new MyGuage());
            }

            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {

                // 业务逻辑之外的  metric代码，度量task所输入的数据条数
                longCounter.add(1);

                gauge.add(1);


                out.collect(value.toUpperCase());
            }
        }).print();

        env.execute();


    }


    public static class MyGuage implements Gauge<Integer>{

        int recordCount = 0;

        public void add(int i){
            recordCount += i;
        }

        @Override
        public Integer getValue() {
            return recordCount;
        }
    }

}
