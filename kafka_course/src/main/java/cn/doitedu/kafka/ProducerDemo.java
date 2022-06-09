package cn.doitedu.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * kafka生产者api代码示例
 */
public class ProducerDemo {

    public static void main(String[] args) throws InterruptedException {


        // 泛型 K: 要发送的数据中的key
        // 泛型 V: 要发送的数据中的value
        // 隐含之意： kafka中的 message，是 Key-value结构的 （可以没有key）
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "doit01:9092,doit02:9092");

        // 因为kafka底层的存储是没有类型维护机制的，用户所发的所有数据类型，都必须变成 序列化后的byte[]
        // 所以，kafka的producer需要一个针对用户要发送的数据类型的序列化工具类
        // 且这个序列化工具类，需要实现kafka所提供的序列工具接口： org.apache.kafka.common.serialization.Serializer
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        /**
         * 代码中进行客户端参数配置的另一种写法
         */
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "doit01:9092,doit02:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // 消息发送应答级别
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG,MyPartitioner.class.getName()); // 指定自定义的分区器


        // 构造一个生产者客户端
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        // 检查是否发送成功的消费者命令：
        //    kafka-console-consumer.sh  --bootstrap-server doit01:9092 --topic abcx
        //    kafka-console-consumer.sh  --bootstrap-server doit01:9092 --topic abcy
        for(int i=0;i<100;i++){
            // 将业务数据封装成客户端所能发送的封装格式
            // 0->abc0
            // 1->abc1

            // TODO 请修改此处逻辑： i奇数的业务数据，发到 abcx  ;    i为偶数的业务数据，发到 abcy
            ProducerRecord<String, String> message = new ProducerRecord<>("abcx", "user_id:"+i, "doit_edu_" + i);

            // 调用客户端去发送
            // 数据的发送动作在producer的底层是异步线程去异步发送的
            producer.send(message);

            Thread.sleep(100);
        }


        // 关闭客户端
        // producer.flush();
        producer.close();


    }
}
