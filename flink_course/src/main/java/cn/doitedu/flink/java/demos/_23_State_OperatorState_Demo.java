package cn.doitedu.flink.java.demos;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
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
 * @Desc: 算子状态使用演示
 **/
public class _23_State_OperatorState_Demo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // 开启状态数据的checkpoint机制（快照的周期，快照的模式）
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        // 开启快照后，就需要指定快照数据的持久化存储位置
       /* env.getCheckpointConfig().setCheckpointStorage(new URI("hdfs://doit01:8020/checkpoint/"));*/
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint/");


        // 开启  task级别故障自动 failover
        // env.setRestartStrategy(RestartStrategies.noRestart()); // 默认是，不会自动failover；一个task故障了，整个job就失败了
        // 使用的重启策略是： 固定重启上限和重启时间间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000));


        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        // 需要使用map算子来达到一个效果：
        // 没来一条数据（字符串），输出 该条字符串拼接此前到达过的所有字符串
        source.map(new StateMapFunction()).print();

        // 提交一个job
        env.execute();

    }

}

/**
 * 要使用operator state，需要让用户自己的Function类去实现 CheckpointedFunction
 * 然后在其中的 方法initializeState 中，去拿到operator state 存储器
 */
class StateMapFunction implements  MapFunction<String,String> , CheckpointedFunction{

    ListState<String> listState;

    /**
     * 正常的MapFunction的处理逻辑方法
     * @param value The input value.
     * @return
     * @throws Exception
     */
    @Override
    public String map(String value) throws Exception {

        /**
         * 故意埋一个异常，来测试 task级别自动容错效果
         */
        if(value.equals("x") && RandomUtils.nextInt(1,15)% 4 == 0)
            throw new Exception("哈哈哈哈,出错了");

        // 将本条数据，插入到状态存储器中
        listState.add(value);



        // 然后拼接历史以来的字符串
        Iterable<String> strings = listState.get();
        StringBuilder sb = new StringBuilder();
        for (String string : strings) {
            sb.append(string);
        }

        return sb.toString();
    }

    /**
     * 系统对状态数据做快照（持久化）时会调用的方法，用户利用这个方法，在持久化前，对状态数据做一些操控
     * @param context the context for drawing a snapshot of the operator
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // System.out.println("checkpoint 触发了，checkpointId : " +context.getCheckpointId());
    }

    /**
     * 算子任务在启动之初，会调用下面的方法，来为用户进行状态数据初始化
     * @param context the context for initializing the operator
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        // 从方法提供的context中拿到一个算子状态存储器
        OperatorStateStore operatorStateStore = context.getOperatorStateStore();

        // 算子状态存储器，只提供List数据结构来为用户存储数据
        ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<>("strings", String.class); // 定义一个状态存储结构描述器

        // getListState方法，在task失败后，task自动重启时，会帮用户自动加载最近一次的快照状态数据
        // 如果是job重启，则不会自动加载此前的快照状态数据
        listState = operatorStateStore.getListState(stateDescriptor);  // 在状态存储器上调用get方法，得到具体结构的状态管理器


        /**
         * unionListState 和普通 ListState的区别：
         * unionListState的快照存储数据，在系统重启后，list数据的重分配模式为： 广播模式； 在每个subtask上都拥有一份完整的数据
         * ListState的快照存储数据，在系统重启后，list数据的重分配模式为： round-robin； 轮询平均分配
         */
        //ListState<String> unionListState = operatorStateStore.getUnionListState(stateDescriptor);



    }
}
