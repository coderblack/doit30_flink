package cn.doitedu;

import cn.doitedu.module.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-27
 * @desc 行为日志生成模拟器（自动连续生成）
 * <p>
 * {
 * "account": "Vz54E9Ya",
 * "appId": "cn.doitedu.app1",
 * "appVersion": "3.4",
 * "carrier": "中国移动",
 * "deviceId": "WEISLD0235S0934OL",
 * "deviceType": "MI-6",
 * "ip": "24.93.136.175",
 * "latitude": 42.09287620431088,
 * "longitude": 79.42106825764643,
 * "netType": "WIFI",
 * "osName": "android",
 * "osVersion": "6.5",
 * "releaseChannel": "豌豆荚",
 * "resolution": "1024*768",
 * "sessionId": "SE18329583458",
 * "timeStamp": 1594534406220
 * "eventId": "productView",
 * "properties": {
 * "pageId": "646",
 * "productId": "157",
 * "refType": "4",
 * "refUrl": "805",
 * "title": "爱得堡 男靴中高帮马丁靴秋冬雪地靴 H1878 复古黄 40码",
 * "url": "https://item.jd.com/36506691363.html",
 * "utm_campain": "4",
 * "utm_loctype": "1",
 * "utm_source": "10"
 * }
 * }
 * <p>
 * <p>
 * kafka中要先创建好topic
 * [root@hdp01 kafka_2.11-2.0.0]# bin/kafka-topics.sh --create --topic yinew_applog --partitions 2 --replication-factor 1 --zookeeper hdp01:2181,hdp02:2181,hdp03:2181
 * <p>
 * 创建完后，检查一下是否创建成功：
 * [root@hdp01 kafka_2.11-2.0.0]# bin/kafka-topics.sh --list --zookeeper hdp01:2181
 */
public class ActionLogAutoGen {
    public static void main(String[] args) throws Exception {

        // 加载历史用户
        // String filePath = "data/users/hisu-1654943006977.txt";
        // HashMap<String, LogBean> hisUsers = UserUtils.loadHisUsers(filePath);

        // 添加新用户
        HashMap<String, LogBean> hisUsers = new HashMap<>();
        UserUtils.addNewUsers(hisUsers, 1000, true);

        UserUtils.saveUsers(hisUsers);

        // 转成带状态用户数据
        List<LogBeanWrapper> wrapperedUsers = UserUtils.userToWrapper(hisUsers);

        System.out.println("日活用户总数：" + wrapperedUsers.size() + "-------");

        // 多线程并行生成日志
        // CollectorConsoleImpl collector = new CollectorConsoleImpl();
        CollectorKafkaImpl collector = new CollectorKafkaImpl("doit-events");
        genBatchToConsole(wrapperedUsers, 3,collector);


    }

    private static void genBatchToConsole(List<LogBeanWrapper> wrapperedUsers, int threads , Collector collector) {
        int partSize = wrapperedUsers.size() / threads;

        ArrayList<List<LogBeanWrapper>> partList = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            List<LogBeanWrapper> userPart = new ArrayList<>();

            for (int j = i * partSize; j < (i != threads - 1 ? (i + 1) * partSize : wrapperedUsers.size()); j++) {
                userPart.add(wrapperedUsers.get(j));
            }
            new Thread(new LogRunnable(userPart,collector,10)).start();
        }
    }

}
