package cn.doitedu.flink.java.demos;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/5/5
 * @Desc: 键控状态使用演示
 **/
public class _24_State_KeyedState_Demo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // 开启状态数据的checkpoint机制（快照的周期，快照的模式）
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        // 开启快照后，就需要指定快照数据的持久化存储位置
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint/");


        // 开启  task级别故障自动 failover
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000));


        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        // 需要使用map算子来达到一个效果：
        // 没来一条数据（字符串），输出 该条字符串拼接此前到达过的所有字符串
        source
                .keyBy(s->"0")
                .map(new RichMapFunction<String, String>() {

                    ListState<String> lstState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        RuntimeContext runtimeContext = getRuntimeContext();
                        // 获取一个List结构的状态存储器
                        lstState = runtimeContext.getListState(new ListStateDescriptor<String>("lst", String.class));

                        // 获取一个 单值 结构的状态存储器
                        // TODO 自己去点一点  ValueState 的 各种操作方法

                        // 获取一个 Map 结构的状态存储器
                        MapState<String, String> mapState = runtimeContext.getMapState(new MapStateDescriptor<String, String>("xx", String.class, String.class));
                        // TODO 自己去点一点  MapState的各种操作方法
                    }

                    @Override
                    public String map(String value) throws Exception {

                        // 将本条数据，装入状态存储器
                        lstState.add(value);

                        // 遍历所有的历史字符串，拼接结果
                        StringBuilder sb = new StringBuilder();
                        for (String s : lstState.get()) {
                            sb.append(s);
                        }

                        return sb.toString();
                    }
                }).setParallelism(2)
                .print().setParallelism(2);

        // 提交一个job
        env.execute();

    }

}

