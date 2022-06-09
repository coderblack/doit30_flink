package cn.doitedu.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class AdminClientDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"doit01:9092,doit02:9092");

        // 管理客户端
        AdminClient adminClient = KafkaAdminClient.create(props);

        // 创建一个topic
        /*NewTopic zzuzz = new NewTopic("zzuzz", 3, (short) 2);
        adminClient.createTopics(Arrays.asList(zzuzz));*/

        // 查看一个topic的详细信息
        DescribeTopicsResult topicDescriptions = adminClient.describeTopics(Arrays.asList("zzuzz"));

        KafkaFuture<Map<String, TopicDescription>> descriptions = topicDescriptions.all();
        Map<String, TopicDescription> infos = descriptions.get();
        Set<Map.Entry<String, TopicDescription>> entries = infos.entrySet();
        for (Map.Entry<String, TopicDescription> entry : entries) {
            String topicName = entry.getKey();
            TopicDescription td = entry.getValue();
            List<TopicPartitionInfo> partitions = td.partitions();
            for (TopicPartitionInfo partition : partitions) {
                int partitionIndex = partition.partition();
                List<Node> replicas = partition.replicas();
                List<Node> isr = partition.isr();
                Node leader = partition.leader();
                System.out.println(topicName+ "\t" +partitionIndex + "\t" + replicas + "\t" + isr + "\t" + leader);
            }
        }


        adminClient.close();

    }
}
