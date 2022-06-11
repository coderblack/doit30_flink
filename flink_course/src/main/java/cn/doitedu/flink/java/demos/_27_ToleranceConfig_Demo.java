package cn.doitedu.flink.java.demos;


import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/5/8
 * @Desc: flink容错机制相关参数配置示例
 *    checkpoint 相关配置
 *    restartStrategy 相关配置
 **/
public class _27_ToleranceConfig_Demo {

    public static void main(String[] args) throws Exception {

        /**
         * 在idea中做测试时，指定从某个保存点来恢复状态
         */
        Configuration conf = new Configuration();
        //conf.setString("execution.savepoint.path", "file:///D:/checkpoint/7ecbd4f9106957c42109bcde/chk-544");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        /* *
         * checkpoint相关配置
         */
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);  // 传入两个最基本ck参数：间隔时长，ck模式
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://doit01:8020/ckpt");
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofMinutes(10000)); // 设置ck对齐的超时时长
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);  // 设置ck算法模式
        checkpointConfig.setCheckpointInterval(2000); // ck的间隔时长
        //checkpointConfig.setCheckpointIdOfIgnoredInFlightData(5); // 用于非对齐算法模式下，在job恢复时让各个算子自动抛弃掉ck-5中飞行数据
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);  // job cancel调时，保留最后一次ck数据
        checkpointConfig.setForceUnalignedCheckpoints(false);  // 是否强制使用  非对齐的checkpoint模式
        checkpointConfig.setMaxConcurrentCheckpoints(5); // 允许在系统中同时存在的飞行中（未完成的）的ck数
        checkpointConfig.setMinPauseBetweenCheckpoints(2000); //  设置两次ck之间的最小时间间隔，用于防止checkpoint过多地占用算子的处理时间
        checkpointConfig.setCheckpointTimeout(3000); // 一个算子在一次checkpoint执行过程中的总耗费时长超时上限
        checkpointConfig.setTolerableCheckpointFailureNumber(10); // 允许的checkpoint失败最大次数





        /* *
         * task失败自动重启策略配置
         */
        RestartStrategies.RestartStrategyConfiguration restartStrategy = null;

        // 固定、延迟重启（参数 1： 故障重启最大次数；参数2： 两次重启之间的延迟间隔）
        restartStrategy = RestartStrategies.fixedDelayRestart(5, 2000);

        // 默认的故障重启策略：不重启（只要有task失败，整个job就失败）
        restartStrategy = RestartStrategies.noRestart();


        /* *
         *  本策略：故障越频繁，两次重启间的惩罚间隔就越长
         *
         *  initialBackoff 重启间隔惩罚时长的初始值 ： 1s
         *  maxBackoff 重启间隔最大惩罚时长 : 60s
         *  backoffMultiplier 重启间隔时长的惩罚倍数: 2（ 每多故障一次，重启延迟惩罚就在 上一次的惩罚时长上 * 倍数）
         *  resetBackoffThreshold 重置惩罚时长的平稳运行时长阈值（平稳运行达到这个阈值后，如果再故障，则故障重启延迟时间重置为了初始值：1s）
         *  jitterFactor 取一个随机数来加在重启时间点上，让每次重启的时间点呈现一定随机性
         *     job1: 9.51   9.53+2*0.1    9.57   ......
         *     job2: 9.51   9.53+2*0.15   9.57   ......
         *     job3: 9.51   9.53+2*0.8    9.57   ......
         */
        restartStrategy = RestartStrategies.exponentialDelayRestart(Time.seconds(1),Time.seconds(60),2.0,Time.hours(1),1.0);

        /* *
         *  failureRate : 在指定时长内的最大失败次数
         *  failureInterval 指定的衡量时长
         *  delayInterval 两次重启之间的时间间隔
         */
        restartStrategy = RestartStrategies.failureRateRestart(5,Time.hours(1),Time.seconds(5));

        /* *
         *  本策略就是退回到配置文件所配置的策略
         *  常用于自定义 RestartStrategy
         *  用户自定义了重启策略类，常常配置在 flink-conf.yaml 文件中
         */
        restartStrategy = RestartStrategies.fallBackRestart();


        // 设置指定的重启策略
        env.setRestartStrategy(restartStrategy);


        // 数据处理
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);


        // 提交执行
        env.execute();
    }


}
