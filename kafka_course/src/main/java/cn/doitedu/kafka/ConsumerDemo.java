package cn.doitedu.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class ConsumerDemo {
    public static void main(String[] args) {

        // 构造一个properties来存放消费者客户端参数
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"doit01:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // kafka的消费者，默认是从所属组之前所记录的偏移量开始消费，如果找不到之前记录的便宜量，则从如下参数配置的策略来确定消费起始偏移量
        // 可以选择：earliest(自动重置到每个分区的最前一条消息）/latest（自动重置到每个分区的最新一条消息/none（没有重置策略）
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        // 设置消费者所属的组id
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"d30-1");

        // 设置自动提交最新的消费位移
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true"); // 默认就是开启的

        // 自动提交最新消费位移的时间间隔
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"5000"); // 默认值就是5000ms

        // 构造一个消费者客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅主题（可以是多个）
        consumer.subscribe(Collections.singletonList("abcx"));

        // 显式指定消费起始偏移量
        /*TopicPartition abcxP0 = new TopicPartition("abcx", 0);
        TopicPartition abcxP1 = new TopicPartition("abcx", 1);
        consumer.seek(abcxP0,10);
        consumer.seek(abcxP1,15);*/


        // 循环往复拉取数据
        boolean condition = true;
        while(condition){

            // 客户端去拉取数据的时候，如果服务端没有数据响应，会保持连接等待服务端响应
            // poll中传入的超时时长参数，是指等待的最大时长
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));

            // 对数据进行业务逻辑处理
            // Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
            // 直接用for循环来迭代本次取到的一批数据
            for (ConsumerRecord<String, String> record : records) {
                // ConsumerRecord中，不光有用户的业务数据，还有kafka塞入的元数据
                String key = record.key();
                String value = record.value();

                // 本条数据所属的topic
                String topic = record.topic();
                // 本条数据所属的分区
                int partition = record.partition();

                // 本条数据的offset
                long offset = record.offset();

                // 当前这条数据所在分区的leader的朝代纪年
                Optional<Integer> leaderEpoch = record.leaderEpoch();

                // 在kafka的数据底层存储中，不光有用户的业务数据，还有大量元数据
                // timestamp就是其中之一： 记录本条数据的时间戳
                // 但是时间戳有两种类型： 本条数据的创建时间（生产者）; 本条数据的追加时间（broker写入log文件的时间）
                // log.message.timestamp.type ==> topic的参数，控制timestamp元数据记录的时间戳的类型
                TimestampType timestampType = record.timestampType();
                long timestamp = record.timestamp();

                // 数据头；
                // 数据头是生产者在写入数据时附加进去的（相当于用户自己自定义的元数据）
                Headers headers = record.headers();

                System.out.println(String.format("数据key: %s , 数据value：%s ,数据所属的topic: %s ,数据所属的partition： %d, 数据的offset: %d , 数据所属leader的纪元： %s , 数据的时间戳类型: %s , 数据的时间戳： %d ",
                        key,value,topic,partition,offset,leaderEpoch.get(),timestampType.name,timestamp
                        ));

            }
        }

        // 关闭客户端
        consumer.close();
    }
}
