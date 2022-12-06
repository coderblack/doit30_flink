package tmp;

import com.alibaba.fastjson.JSON;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.error.MarkedYAMLException;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.runtime.operators.window.CountWindow;
import org.apache.flink.table.runtime.operators.window.assigners.CountSlidingWindowAssigner;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.SerializableSupplier;
import tmp.pojos.MysqlUser;
import tmp.pojos.UserSlotGame;

import javax.sql.XADataSource;
import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class FlinkKafkaDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建一个kafkasource，读取kafka数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("47.242.160.190:9092,47.242.250.174:9092,47.242.36.238:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("Test01")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setTopics("user_slot_game_log")
                .build();

        // 设置checkpoint保存点
        env.setStateBackend(new FsStateBackend("hdfs://***:8020/flink/checkpoints/dominoUser02"));
        //每5秒启动一个检查点
        env.enableCheckpointing(5000);
        //检查点保存模式
        //env.getCheckpointConfig.getCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        //设置最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);

        //设置超时时长
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        //可恢复
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //重试机制
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,org.apache.flink.api.common.time.Time.of(30, TimeUnit.SECONDS)));


        //水印策略  按照log_date 作为事件时间
        WatermarkStrategy<UserSlotGame> strategy = WatermarkStrategy.<UserSlotGame>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<UserSlotGame>() {

                    @Override
                    public long extractTimestamp(UserSlotGame ele, long l) {

                        try {
                            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(ele.getLOG_DATE()).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        return l;
                    }
                })
                .withIdleness(Duration.ofMillis(1000));




        // 添加source,把数据包装成对象,分配水印  stream1 打印没有问题
        SingleOutputStreamOperator<UserSlotGame> stream1 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kfk")
                .map(new MapFunction<String, UserSlotGame>() {
                    @Override
                    public UserSlotGame map(String s) throws Exception {
                        return JSON.parseObject(s, UserSlotGame.class);
                    }
                })
                .assignTimestampsAndWatermarks(strategy);

        //分组 想按三个字段分组怎么做？
        //KeyedStream<UserSlotGame, Long> keyedStream = stream1.keyBy(UserSlotGame -> UserSlotGame.getGAME_ID());
        KeyedStream<UserSlotGame, Tuple3<Long, Long, Long>> keyedStream = stream1.keyBy(g -> Tuple3.of(g.getGAME_ID(),g.getSERVER_ID() ,g.getUSER_ID()), TypeInformation.of(new TypeHint<Tuple3<Long, Long, Long>>() {
        }));


        // 计数窗口 20 条计算一次
        /*keyedStream.countWindow(20)
                .process(new ProcessWindowFunction<UserSlotGame, Object, Long, GlobalWindow>() {
                    @Override
                    public void process(Long aLong, Context context, Iterable<UserSlotGame> iterable, Collector<Object> collector) throws Exception {

                    }
                });*/

        /**
         *
         * TODO
         * TODO 需求中的逻辑，完全可以用aggregate来做——增量聚合方式，更省内存
         * TODO 所以，这里给你用  aggregate来实现一下
         *
         */
        keyedStream.countWindow(20)
                .aggregate(new AggregateFunction<UserSlotGame, MysqlUser, MysqlUser>() {
                    @Override
                    public MysqlUser createAccumulator() {
                        return new MysqlUser();
                    }

                    @Override
                    public MysqlUser add(UserSlotGame value, MysqlUser accumulator) {
                        aggregate(value, accumulator);
                        return accumulator;
                    }

                    @Override
                    public MysqlUser getResult(MysqlUser accumulator) {
                        return accumulator;
                    }

                    @Override
                    public MysqlUser merge(MysqlUser a, MysqlUser b) {
                        // TODO 如果这段代码要与离线计算通用，则把这里写一下： a 和 b 中的字段按聚合逻辑求最大或者求和即可
                        a.setLog_date(a.getLog_date().compareTo(b.getLog_date())>0?a.getLog_date():b.getLog_date());
                        a.setBonus_game_sin_money(a.getBonus_game_sin_money()+b.getBonus_game_sin_money());
                        // TODO .......

                        return a;
                    }
                });




        //一分钟计算一次
        /**
         * TODO 当然，也可以用process来做
         * TODO 本质是一样的
         */
        keyedStream.window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new ProcessWindowFunction<UserSlotGame, MysqlUser, Tuple3<Long, Long, Long>, TimeWindow>() {
                    ValueState<MysqlUser> accumulator;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        accumulator = getRuntimeContext().getState(new ValueStateDescriptor<MysqlUser>("agg", MysqlUser.class));
                    }

                    @Override
                    public void process(Tuple3<Long, Long, Long> aLong, Context context, Iterable<UserSlotGame> iterable, Collector<MysqlUser> collector) throws Exception {
                        MysqlUser value = accumulator.value();
                        for (UserSlotGame userSlotGame : iterable) {
                            aggregate(userSlotGame,value);
                        }

                        collector.collect(value);
                    }
                });


        //开窗计算，把sql计算结果转成MysqlUser对象


       // 写到mysql
        SinkFunction<MysqlUser> JDBCSink = JdbcSink.exactlyOnceSink(
                "insert into Test01(log_date,game_id,server_id,game_user,game_rate,total_bet_money" +
                        ",total_win_money,total_game_num,main_game_win_money,free_game_sin_money,bonus_game_sin_money,win_user" +
                        ",lose_user,bet_game_num) values (?,?,?,?,?,?,?,?,?,?,?,?)",
                (PreparedStatement preparedStatement, MysqlUser mu) -> {
                    preparedStatement.setString(1, mu.getLog_date());
                    preparedStatement.setLong(2, mu.getGame_id());
                    preparedStatement.setLong(3, mu.getServer_id());
                    preparedStatement.setLong(4, mu.getGame_user());
                    preparedStatement.setLong(5, mu.getGame_rate());
                    preparedStatement.setLong(6, mu.getTotal_bet_money());
                    preparedStatement.setLong(7, mu.getTotal_win_money());
                    preparedStatement.setLong(8, mu.getTotal_game_num());
                    preparedStatement.setLong(9, mu.getMain_game_win_money());
                    preparedStatement.setLong(10, mu.getFree_game_sin_money());
                    preparedStatement.setLong(11, mu.getBonus_game_sin_money());
                    preparedStatement.setLong(12, mu.getWin_user());
                    preparedStatement.setLong(13, mu.getLose_user());
                    preparedStatement.setLong(14, mu.getBet_game_num());
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(1)  // TODO 需要提高插入mysql的效率，比如需要20条一批插入mysql，把这个batchSize参数调大即可
                        .build(),
                JdbcExactlyOnceOptions.builder()
                        .withTransactionPerConnection(true) // mysql不支持同一个连接上存在并行的多个事务，必须把该参数设置为true
                        .build(),
                (SerializableSupplier<XADataSource>) () -> {
                    // XADataSource就是jdbc连接，不过它是支持分布式事务的连接
                    // 而且它的构造方法，不同的数据库构造方法不同
                    MysqlXADataSource xaDataSource = new MysqlXADataSource();
                    xaDataSource.setUrl("am-j6c14v9j8w9i334ha167320o.ads.aliyuncs.com/domino_ads");
                    xaDataSource.setUser("gcroot");
                    xaDataSource.setPassword("5iFBNyK2PXhKdPnv");
                    return xaDataSource;
                }
        );


        // 把构造好的sink添加到流中  streamOperator 必须是一个datastream，才能调用addSink方法
        //streamOperator.addSink(JDBCSink);


        env.execute();

    }

    /**
     * TODO 累加等聚合逻辑
     * @param value
     * @param accumulator
     *
     * GAME_ID,SERVER_ID,USER_ID
     * max(LOG_DATE) as LOG_DATE
     * sum(BET_MONEY) as total_bet_money
     * sum(WIN_MONEY) as total_win_money
     * sum(BUFF0) as total_game_num
     * sum (case when GAME_TYPE = 0 then BUFF0 else 0 end)  as main_game_win_money
     * sum (case when GAME_TYPE = 1 then BUFF0 else 0 end)  as free_game_sin_money
     * sum (case when GAME_TYPE = 2 then BUFF0 else 0 end)  as bonus_game_sin_money
     * sum (case when BET_MONEY > 0 then BUFF0 else 0 end)  as bet_game_num
     *
     */
    private static void aggregate(UserSlotGame value, MysqlUser accumulator) {
        // GAME_ID
        accumulator.setGame_id(value.getGAME_ID());
        // SERVER_ID
        accumulator.setServer_id(value.getSERVER_ID());
        // USER_ID
        accumulator.setGame_user(value.getUSER_ID());

        // max(LOG_DATE) as LOG_DATE
        if(value.getLOG_DATE().compareTo(accumulator.getLog_date())>0) accumulator.setLog_date(value.getLOG_DATE());

        // sum(BET_MONEY) as total_bet_money
        accumulator.setTotal_bet_money(accumulator.getTotal_bet_money() + value.getBET_MONEY());

        // sum(WIN_MONEY) as total_win_money
        accumulator.setTotal_win_money(accumulator.getTotal_win_money() + value.getWIN_MONEY());

        // sum(BUFF0) as total_game_num
        accumulator.setTotal_game_num(accumulator.getTotal_game_num() + value.getBUFF0());

        // sum (case when GAME_TYPE = 0 then BUFF0 else 0 end)  as main_game_win_money
        accumulator.setMain_game_win_money(accumulator.getMain_game_win_money() + value.getGAME_TYPE() == 0 ? value.getBUFF0() : 0);

        // sum (case when GAME_TYPE = 1 then BUFF0 else 0 end)  as free_game_sin_money
        accumulator.setFree_game_sin_money(accumulator.getFree_game_sin_money() + value.getGAME_TYPE() == 1 ? value.getBUFF0() : 0);

        // sum (case when GAME_TYPE = 2 then BUFF0 else 0 end)  as bonus_game_sin_money
        accumulator.setBonus_game_sin_money(accumulator.getBonus_game_sin_money() + value.getGAME_TYPE() == 2 ? value.getBUFF0() : 0);

        // sum (case when BET_MONEY > 0 then BUFF0 else 0 end)  as bet_game_num
        accumulator.setBet_game_num(accumulator.getBet_game_num() + value.getBET_MONEY() > 0  ? value.getBUFF0() : 0);
    }
}

class SqlHolder3 {
    public static String getSql3(int idx) throws IOException{
        String filestr = FileUtils.readFileToString(new File("sql/sqls3.sql"), "utf-8");
        return filestr.split("\\~")[idx-1];
    }
}
