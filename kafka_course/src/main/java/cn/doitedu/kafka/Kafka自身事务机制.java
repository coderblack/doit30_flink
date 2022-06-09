package cn.doitedu.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

/**
 * 从kafka的topic-a中读数据，处理（把读到的数据转大写），处理结果写回kafka的topic-b
 * 利用kafka自身的事务机制，来实现 端到端的eos语义
 * 核心点： 让 消费端的偏移量记录更新  和   生产端的数据落地  ，绑定在一个事务中
 */
public class Kafka自身事务机制 {

    public static void main(String[] args) {

        Properties props = new Properties();
        /**
         * 消费者参数
         */
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "doit01:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "trans-001");
        // 关闭消费者的消费位移自动提交机制
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        /**
         * 生产者参数
         */
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "doit01:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "x001");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "4");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        // 初始化事务
        producer.initTransactions();
        // 创建一个自己记录最大消费位移的hashmap
        HashMap<TopicPartition, OffsetAndMetadata> offsetsMap = new HashMap<>();

        consumer.subscribe(Arrays.asList("topic-a"));

        // 开始消费数据，做业务处理
        boolean flag = true;
        while (flag) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

            // 如果要对本次拉取的所有数据的处理绑定在一个事务中，则在此处开启事务
            producer.beginTransaction();

            try {
                // 从拉取到的数据中，获得本批数据包含哪些分区
                Set<TopicPartition> topicPartitionSet = records.partitions();

                // 遍历每一个分区
                for (TopicPartition topicPartition : topicPartitionSet) {

                    // 从拉取到的数据中取到本分区的所有数据
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        // 业务处理逻辑
                        String result = record.value().toUpperCase();
                        // 把处理的结果写出去
                        ProducerRecord<String, String> resultRecord = new ProducerRecord<>("topic-b", result);
                        producer.send(resultRecord);

                        // 记录本分区本条消息的offset 到  offsetMap中
                        long offset = record.offset();
                        offsetsMap.put(topicPartition, new OffsetAndMetadata(offset + 1));
                    }
                }

                // 提交消费位移
                consumer.commitSync();  // 它会自动计算出本批拉取到的数据中的每个分区的最大消息offset，来得到每个分区要提交的消费位移
                // consumer.commitSync(offsetsMap);  // 或者按照自己想要的各分区消费位移值来提交

                // 提交事务
                producer.commitTransaction();
            } catch (Exception e) {
                // 如果上面那一批数据处理过程中任意时刻发生了异常，则放弃本次事务
                // 下游就可以通过设置 isolation_level=read_committed 来避开本次产生的“脏”数据
                producer.abortTransaction();
            }
        }

        consumer.close();
        producer.close();

    }
}
