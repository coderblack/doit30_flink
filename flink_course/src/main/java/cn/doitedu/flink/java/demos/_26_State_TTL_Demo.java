package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/5/5
 * @Desc: 状态数据的TTL管理
 **/
public class _26_State_TTL_Demo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        /**
         * 设置要使用的状态后端
         */
        HashMapStateBackend hashMapStateBackend = new HashMapStateBackend();
        env.setStateBackend(hashMapStateBackend); // 使用HashMapStateBackend  作为状态后端

        EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend();
        //env.setStateBackend(embeddedRocksDBStateBackend);  // 设置 EmbeddedRocksDBStateBackend 作为状态后端

        // 已过期
        /*new FsStateBackend()*/
        /*new MemoryStateBackend()*/

        // 开启状态数据的checkpoint机制（快照的周期，快照的模式）
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        // 开启快照后，就需要指定快照数据的持久化存储位置
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint/");


        // 开启  task级别故障自动 failover
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));


        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        // 需要使用map算子来达到一个效果：
        // 没来一条数据（字符串），输出 该条字符串拼接此前到达过的所有字符串
        source
                .keyBy(s -> "0")
                .map(new RichMapFunction<String, String>() {

                    // 单值状态
                    ValueState<String> valueState;
                    // list状态
                    ListState<String> lstState;
                    // map状态
                    MapState<String, String> mapState;
                    // reducing状态
                    ReducingState<Integer> reduceState;
                    // aggregate状态
                    AggregatingState<Integer, Double> aggState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        RuntimeContext runtimeContext = getRuntimeContext();

                        // 获取一个 单值 结构的状态存储器，并设置TTL参数
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.milliseconds(1000)) // 配置数据的存活时长为4s
                                .setTtl(Time.milliseconds(4000))  // 配置数据的存活时长为4s

                                .updateTtlOnReadAndWrite()  //  读、写，都导致该条数据的ttl计时重置
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) // 设置ttl计时重置的策略
                                .updateTtlOnCreateAndWrite()  // 当插入和更新的时候，导致该条数据的ttl计时重置

                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)  // 不允许返回已过期但尚未被清理的数据
                                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) // 允许返回已过期但尚未被清理的数据

                                .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)  // ttl计时的时间语义:设置为处理时间
                                .useProcessingTime()  // ttl计时的时间语义:设置为处理时间

                                // 下面3种过期数据检查清除策略，不是覆盖的关系，而是添加的关系
                                .cleanupIncrementally(1,false)  // 增量清理（每当一条状态数据被访问，则会检查这条状态数据的ttl是否超时，是就删除）
                                .cleanupFullSnapshot()  // 全量快照清理策略（在checkpoint的时候，保存到快照文件中的只包含未过期的状态数据，但是它并不会清理算子本地的状态数据）
                                //.cleanupInRocksdbCompactFilter(1000) // 在rocksdb的compact机制中添加过期数据过滤器，以在compact过程中清理掉过期状态数据

                                //.disableCleanupInBackground()

                                .build();


                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("vstate", String.class);
                        valueStateDescriptor.enableTimeToLive(ttlConfig); // 开启TTL管理


                        // 本状态管理器就会执行ttl管理
                        //valueState = runtimeContext.getState(valueStateDescriptor);


                        // 本状态管理器就会执行ttl管理
                        ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("lst", String.class);
                        listStateDescriptor.enableTimeToLive(ttlConfig);
                        lstState = runtimeContext.getListState(listStateDescriptor);


                        // 获取一个 Map 结构的状态存储器
                        //mapState = runtimeContext.getMapState(new MapStateDescriptor<String, String>("xx", String.class, String.class));

                    }

                    @Override
                    public String map(String value) throws Exception {
                        lstState.add(value);

                        StringBuilder sb = new StringBuilder();
                        for (String s : lstState.get()) {
                            sb.append(s);
                        }

                        return sb.toString();
                    }
                }).setParallelism(1)
                .print().setParallelism(1);

        // 提交一个job
        env.execute();


    }
}
