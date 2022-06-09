package cn.doitedu.flinksql.fuxi;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class KeyedStateDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.socketTextStream("doitedu", 9999);

        SingleOutputStreamOperator<String> words = stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        KeyedStream<String, String> keyed = words.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });


        keyed.process(new KeyedProcessFunction<String, String, Tuple2>() {
            ValueState<Integer> cntState;
            @Override
            public void open(Configuration parameters) throws Exception {
                // 利用一个键控状态来进行计数
                cntState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("cnt", Integer.class));
            }

            @Override
            public void processElement(String word, KeyedProcessFunction<String, String, Tuple2>.Context ctx, Collector<Tuple2> out) throws Exception {
                cntState.update(cntState.value()+1);
                out.collect(Tuple2.of(word,cntState.value()));
            }
        });

    }


    /**
     * 利用算子状态做wordcount
     */
    public static class MyBieniuProcessFuntion extends KeyedProcessFunction<String, String, Tuple2> implements CheckpointedFunction{

        ListState<Map<String, Integer>> cntState;

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            // 这种状态叫做 “算子状态”
            cntState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Map<String, Integer>>("cnt", TypeInformation.of(new TypeHint<Map<String, Integer>>() {
            })));
            Map<String, Integer> cntMap;
            Iterator<Map<String, Integer>> iter = cntState.get().iterator();
            if(iter.hasNext()){
                cntMap = iter.next();
                if(cntMap!=null) return;
            }

            cntMap = new HashMap<>();

            cntState.update(Arrays.asList(cntMap));

        }


        @Override
        public void processElement(String word, KeyedProcessFunction<String, String, Tuple2>.Context ctx, Collector<Tuple2> out) throws Exception {

            Iterator<Map<String, Integer>> iter = cntState.get().iterator();
            iter.hasNext();
            Map<String, Integer> cntMap = iter.next();

            int newCount = cntMap.getOrDefault(word, 0) + 1;
            cntMap.put(word,newCount);

            cntState.update(Arrays.asList(cntMap));

            out.collect(Tuple2.of(word,newCount));
        }
    }
}
