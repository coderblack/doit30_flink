package cn.doitedu.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * 利用mysql的事务机制，来实现kafka consumer数据传输过程 端到端 的 exactly - once
 * <p>
 * 准备工作：
 * 1. 创建topic
 * [root@doit01 ~]# kafka-topics.sh --create --topic user-info --partitions 3 --replication-factor 2 --zookeeper doit01:2181
 * <p>
 * 2. 创建mysql表
 * CREATE TABLE `stu_info` (
 * `id` int(11) NOT NULL,
 * `name` varchar(255) DEFAULT NULL,
 * `age` int(11) DEFAULT NULL,
 * `gender` varchar(255) DEFAULT NULL,
 * PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 * <p>
 * 3.创建消费位移记录表
 * CREATE TABLE `t_offsets` (
 * `topic_partition` varchar(255) NOT NULL,
 * `offset` bigint(20) DEFAULT NULL,
 * PRIMARY KEY (`topic_partition`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 */
public class Consumer实现ExactlyOnce手段1 {

    public static void main(String[] args) throws SQLException {


        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "doit01:9092,doit02:9092,doit03:9092");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "d30-2");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //  关闭自动位移提交机制


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 创建一个jdbc连接
        Connection conn = DriverManager.getConnection("jdbc:mysql://doit01:3306/abc", "root", "ABC123.abc123");
        // 关闭jdbc的自动事务提交
        conn.setAutoCommit(false);

        // 定义一个业务数据插入语句
        PreparedStatement pstData = conn.prepareStatement("insert into stu_info values ( ? , ? , ? , ?)");

        // 定义一个偏移量更新语句
        PreparedStatement pstOffset = conn.prepareStatement("insert into t_offsets values(? , ?) on DUPLICATE KEY UPDATE offset= ?");

        //
        PreparedStatement pstQueryOffset = conn.prepareStatement("select offset  from t_offsets where topic_partition = ?");


        // 订阅主题
        // TODO 需要把消费起始位置，初始化成上一次运行所记录的消费位移
        // TODO 而且，还要考虑一个问题： 消费组再均衡时会发生什么
        consumer.subscribe(Arrays.asList("user-info"), new ConsumerRebalanceListener() {

            // 被剥夺了分区消费权后调用下面的方法
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {


            }

            // 被分配了新的分区消费权后调用的方法
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

                try {
                    for (TopicPartition topicPartition : partitions) {
                        // 去查询mysql中的t_offsets表，得到自己拥有消费权的分区的消费位移记录
                        pstQueryOffset.setString(1, topicPartition.topic() + ":" + topicPartition.partition());
                        ResultSet resultSet = pstQueryOffset.executeQuery();
                        resultSet.next();
                        long offset = resultSet.getLong("offset");

                        System.out.println("发生了再均衡，被分配了分区消费权，并查询到了目标分区之前提交的偏移量： " + topicPartition + ", " + offset);

                        // 将消费起始位置初始化为 数据库中查询到的偏移量
                        consumer.seek(topicPartition, offset);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });


        boolean run = true;
        while (run) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            // 遍历拉到的这一批数据
            for (ConsumerRecord<String, String> record : records) {

                try {
                    String data = record.value();
                    // 解析原始数据 :   1,zs,18,male
                    String[] fields = data.split(",");

                    // 替换插入语句中的占位符
                    pstData.setInt(1, Integer.parseInt(fields[0]));
                    pstData.setString(2, fields[1]);
                    pstData.setInt(3, Integer.parseInt(fields[2]));
                    pstData.setString(4, fields[3]);

                    // 执行业务数据插入语句
                    pstData.execute();

                    // 替换偏移量更新语句中的占位符
                    pstOffset.setString(1, record.topic() + ":" + record.partition());  // 主题:分区
                    pstOffset.setLong(2, record.offset() + 1);  // 消费位移
                    pstOffset.setLong(3, record.offset() + 1);  // 消费位移

                    // 人为埋一个异常 ，来测试  事务控制是否生效
                    /*if (fields[0].equals("4")) throw new Exception("哈哈哈，抛给你看");*/


                    // 执行偏移量更新语句
                    pstOffset.execute();

                    // 提交jdbc事务
                    conn.commit();
                } catch (Exception e) {
                    e.printStackTrace();
                    conn.rollback();  // 事务回滚
                }
            }
        }

        pstData.close();
        conn.close();
        consumer.close();


    }
}
