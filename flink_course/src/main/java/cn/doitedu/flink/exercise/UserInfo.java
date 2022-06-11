package cn.doitedu.flink.exercise;

import lombok.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserInfo {
    private int id;
    private String gender;
    private String city;
}
