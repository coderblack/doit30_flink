package cn.doitedu.flink.exercise;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventCount {
    private int id;
    private String eventId;
    private int cnt;

}
