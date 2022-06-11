package cn.doitedu.flink.exercise;

import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventUserInfo {

    private int id;
    private String eventId;
    private int cnt;
    private String gender;
    private String city;

}
