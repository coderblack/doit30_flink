package cn.doitedu.kafka;

import com.alibaba.fastjson.JSON;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.roaringbitmap.RoaringBitmap;

import java.time.Duration;
import java.util.*;

public class Kafka编程练习_消费者_判重 {

    public static void main(String[] args) {

        // 启动数据消费线程
        new Thread(new ConsumeRunnableBloomFilter()).start();
    }


    /**
     * 消费拉取数据的线程runnable
     */
    static class ConsumeRunnableBloomFilter implements Runnable {

        BloomFilter<Long> bloomFilter;

        KafkaConsumer<String, String> consumer;

        public ConsumeRunnableBloomFilter() {
            bloomFilter = BloomFilter.create(Funnels.longFunnel(), 1000000000, 0.01);

            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "doit01:9092");
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "event-01");

            consumer = new KafkaConsumer<>(props);

        }

        @Override
        public void run() {
            consumer.subscribe(Arrays.asList("doit30-events"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                for (ConsumerRecord<String, String> record : records) {
                    String eventJson = record.value();
                    // 解析json, 拿到 guid
                    try {
                        UserEvent userEvent = JSON.parseObject(eventJson, UserEvent.class);

                        // 去布隆过滤器中判断一下，本次出现guid，是否曾经已经记录过
                        boolean mightContain = bloomFilter.mightContain(userEvent.getGuid());

                        // 如果本次出现的guid已存在，则将flag设置为0
                        if(mightContain){
                            userEvent.setFlag(0);
                        }
                        // 如果本次出现的guid不存在，则将flag设置为1，并将本次出现的guid映射到布隆过滤
                        else{
                            userEvent.setFlag(1);
                            // 向布隆过滤器中映射新的元素
                            bloomFilter.put(userEvent.getGuid());
                        }

                        // 输出结果
                        System.out.println(JSON.toJSONString(userEvent));

                    } catch (Exception e) {
                        System.out.println("出异常了： " + eventJson);
                    }
                }
            }
        }
    }
}