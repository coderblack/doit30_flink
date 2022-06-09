package cn.doitedu.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * 手动指定消费起始偏移量位置
 */
public class ConsumerDemo2 {

    public static void main(String[] args) throws IOException {

        Properties props = new Properties();
        // 从配置文件中加载写好的参数
        props.load(ConsumerDemo2.class.getClassLoader().getResourceAsStream("consumer.properties"));
        // 手动再set一些参数进去
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"d30-2");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

/*
        // subscribe订阅 会参与消费组的自动再均衡机制才能真正获得自己要消费的topic及其分区的
        consumer.subscribe(Collections.singletonList("ddd"));
        // 这里无意义地去拉一次数据，主要就是为了确保 分区分配动作已完成
        consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        // 然后再定位到指定的偏移量，开始正式消费
        consumer.seek(new TopicPartition("ddd",0),2);
*/


        // 既然要自己指定一个确定的起始消费位置，那通常隐含之意是不需要去参与消费组自动再均衡机制
        // 那么，就不要使用 subscribe 来订阅主题
        consumer.assign(Arrays.asList(new TopicPartition("ddd",0)));
        consumer.seek(new TopicPartition("ddd",0),4);


        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("数据key: %s , 数据value：%s ,数据所属的topic: %s ,数据所属的partition： %d, 数据的offset: %d , 数据所属leader的纪元： %s , 数据的时间戳类型: %s , 数据的时间戳： %d ",
                        record.key(),record.value(),record.topic(),record.partition(),record.offset(),record.leaderEpoch().get(),record.timestampType().name,record.timestamp()
                ));
            }
        }


    }
}
