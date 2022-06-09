package cn.doitedu.flinksql.fuxi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventBean {

    private long guid;
    private String sessionId;
    private String eventId;
    private long eventTs;
    private Map<String,String> properties;
}
