package cn.doitedu.flink.java.demos;

import com.alibaba.fastjson.JSON;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Optional;

/**
 *
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/26
 * @Desc:
 *     将数据流写入redis，利用RedisSink算子
 *
 **/
public class _12_RedisSinkOperator_Demo1 {


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);


        // 开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        // 构造好一个数据流
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        // eventLog数据插入redis，你想用什么结构来存储？
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("doit01").build();

        RedisSink<EventLog> redisSink = new RedisSink<>(config, new StringInsertMapper());

        streamSource.addSink(redisSink);

        env.execute();

    }


    static class StringInsertMapper implements RedisMapper<EventLog>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);
        }

        /**
         *  如果选择的是没有内部key的redis数据结构，则此方法返回的就是大 key
         *  如果选择的是有内部key的redis数据结构（hset），则此方法返回的是hset内部的小key，二把上面Description中传入的值作为大key
         * @param data
         * @return
         */
        @Override
        public String getKeyFromData(EventLog data) {
            return data.getGuid()+"-"+data.getSessionId()+"-"+data.getTimeStamp();   // 这里就是string数据的大key
        }

        @Override
        public String getValueFromData(EventLog data) {
            return JSON.toJSONString(data);   // 这里就是string数据的value
        }
    }


    /**
     * HASH结构数据插入
     */
    static class HsetInsertMapper implements RedisMapper<EventLog>{
        // 可以根据具体数据， 选择额外key（就是hash这种结构，它有额外key（大key）
        @Override
        public Optional<String> getAdditionalKey(EventLog data) {
            return RedisMapper.super.getAdditionalKey(data);
        }

        // 可以根据具体数据，设置不同的TTL（time to live，数据的存活时长）
        @Override
        public Optional<Integer> getAdditionalTTL(EventLog data) {
            return RedisMapper.super.getAdditionalTTL(data);
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"event-logs");
        }

        /**
         *  如果选择的是没有内部key的redis数据结构，则此方法返回的就是大 key
         *  如果选择的是有内部key的redis数据结构（hset），则此方法返回的是hset内部的小key，二把上面Description中传入的值作为大key
         * @param data
         * @return
         */
        @Override
        public String getKeyFromData(EventLog data) {
            return data.getGuid()+"-"+data.getSessionId()+"-"+data.getTimeStamp();  // 这里就是hset中的field（小key）
        }

        @Override
        public String getValueFromData(EventLog data) {
            return data.getEventId();   // 这里就是hset中的value
        }


    }






}
