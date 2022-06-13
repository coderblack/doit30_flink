package cn.doitedu.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo12_JdbcConnectorTest2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        EnvironmentSettings environmentSettings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, environmentSettings);


        // 建表来映射 mysql中的 flinktest.stu
        tenv.executeSql(
                "create table flink_stu(\n" +
                        "   id  int  primary key, \n" +
                        "   gender string, \n" +
                        "   name string  \n" +
                        ") with (\n" +
                        "  'connector' = 'jdbc',\n" +
                        "  'url' = 'jdbc:mysql://doitedu:3306/flinktest',\n" +
                        "  'table-name' = 'stu2',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = 'root' \n" +
                        ")"
        );


        // 1,male
        SingleOutputStreamOperator<Bean1> bean1 = env
                .socketTextStream("doitedu", 9998)
                .map(s -> {
                    String[] arr = s.split(",");
                    return new Bean1(Integer.parseInt(arr[0]), arr[1]);
                });
        // 1,zs
        SingleOutputStreamOperator<Bean2> bean2 = env.socketTextStream("doitedu", 9999).map(s -> {
            String[] arr = s.split(",");
            return new Bean2(Integer.parseInt(arr[0]), arr[1]);
        });


        // 流转表
        tenv.createTemporaryView("bean1", bean1);
        tenv.createTemporaryView("bean2", bean2);

        tenv.executeSql("insert into flink_stu " +
                "select  bean1.id,bean1.gender,bean2.name from bean1 left join bean2 on bean1.id=bean2.id");


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
