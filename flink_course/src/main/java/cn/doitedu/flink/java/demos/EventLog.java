package cn.doitedu.flink.java.demos;

import lombok.*;
import org.apache.flink.streaming.connectors.redis.RedisSink;

import java.util.Map;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public   class EventLog{
    private long guid;
    private String sessionId;
    private String eventId;
    private long timeStamp;
    private Map<String,String> eventInfo;
}


