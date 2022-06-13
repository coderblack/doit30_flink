package cn.doitedu.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo13_FileSystemConnectorTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        EnvironmentSettings environmentSettings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, environmentSettings);


        // 建表 fs_table 来映射 mysql中的flinktest.stu
        tenv.executeSql(
                "CREATE TABLE fs_table (\n" +
                        "  user_id STRING,\n" +
                        "  order_amount DOUBLE,\n" +
                        "  dt STRING,\n" +
                        "  `hour` STRING\n" +
                        ") PARTITIONED BY (dt, `hour`) WITH (\n" +
                        "  'connector'='filesystem',\n" +
                        "  'path'='file:///d:/filetable/',\n" +
                        "  'format'='json',\n" +
                        "  'sink.partition-commit.delay'='1 h',\n" +
                        "  'sink.partition-commit.policy.kind'='success-file',\n" +
                        "  'sink.rolling-policy.file-size' = '8M',\n" +
                        "  'sink.rolling-policy.rollover-interval'='30 min',\n" +
                        "  'sink.rolling-policy.check-interval'='10 second'\n" +
                        ")"
        );


        // u01,88.8,2022-06-13,14
        SingleOutputStreamOperator<Tuple4<String, Double, String, String>> stream = env
                .socketTextStream("doitedu", 9999)
                .map(s -> {
                    String[] split = s.split(",");
                    return Tuple4.of(split[0], Double.parseDouble(split[1]), split[2], split[3]);
                }).returns(new TypeHint<Tuple4<String, Double, String, String>>() {
                });

        tenv.createTemporaryView("orders",stream);

        tenv.executeSql("insert into fs_table select * from orders");



        env.execute();
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean1 {
        public int id;
        public String gender;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean2 {
        public int id;
        public String name;
    }
}
