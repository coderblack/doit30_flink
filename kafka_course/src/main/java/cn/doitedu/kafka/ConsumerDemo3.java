package cn.doitedu.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * 消费组再均衡观察
 */
public class ConsumerDemo3 {

    public static void main(String[] args) throws IOException {

        Properties props = new Properties();
        // 从配置文件中加载写好的参数
        props.load(ConsumerDemo3.class.getClassLoader().getResourceAsStream("consumer.properties"));
        // 手动再set一些参数进去
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"d30-2");
        // 指定消费者再均衡策略
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,RangeAssignor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);


        // reb-1 主题： 3个分区
        // reb-2 主题： 2个分区
        consumer.subscribe(Arrays.asList("reb-1", "reb-2"), new ConsumerRebalanceListener() {
            // 再均衡过程中，消费者会被取消先前所分配的主题，分区
            // 取消了之后，consumer底层就会调用下面的方法
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("我被取消了如下主题，分区：" + partitions);

            }

            // 再均衡过程中，消费者会被重新分配到新的主题，分区
            // 分配好了新的主题，分区后，consumer底层调用下面的方法
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("我又被分配了如下主题，分区：" + partitions);
            }
        });

        while(true){
            consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        }

    }
}
