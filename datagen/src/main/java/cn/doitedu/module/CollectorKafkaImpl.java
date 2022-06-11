package cn.doitedu.module;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class CollectorKafkaImpl implements Collector {

    private KafkaProducer<Integer, String> kafkaProducer;
    private String topicName;

    int messageSeq = 0;

    public CollectorKafkaImpl(String topicName){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "doitedu:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        // 构造一个kafka生产者客户端
        this.kafkaProducer = new KafkaProducer<>(props);

        this.topicName = topicName;
    }
    @Override
    public void collect(String logdata) {
        this.messageSeq ++;

        ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName, this.messageSeq, logdata);
        kafkaProducer.send(record);

        kafkaProducer.flush();

    }
}
