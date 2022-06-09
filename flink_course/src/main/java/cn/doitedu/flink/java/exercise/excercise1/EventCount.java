package cn.doitedu.flink.java.exercise.excercise1;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventCount {
    private int id;
    private String eventId;
    private int cnt;

}
