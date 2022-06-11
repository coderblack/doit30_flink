package cn.doitedu.flink.java.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;

/**
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/23
 * @Desc: 各类transformation算子的api演示
 **/
public class _07_Transformation_Demos {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // {"uid":1,"name":"zs","friends":[{"fid":2,"name":"aa"},{"fid":3,"name":"bb"}]}
        /*DataStreamSource<String> streamSource = env.fromElements(
                "{\"uid\":1,\"name\":\"zs\",\"friends\":[{\"fid\":2,\"name\":\"aa\"},{\"fid\":3,\"name\":\"bb\"}]}",
                "{\"uid\":2,\"name\":\"ls\",\"friends\":[{\"fid\":1,\"name\":\"cc\"},{\"fid\":2,\"name\":\"aa\"}]}",
                "{\"uid\":3,\"name\":\"ww\",\"friends\":[{\"fid\":2,\"name\":\"aa\"}]}",
                "{\"uid\":4,\"name\":\"zl\",\"friends\":[{\"fid\":3,\"name\":\"bb\"}]}",
                "{\"uid\":5,\"name\":\"tq\",\"friends\":[{\"fid\":2,\"name\":\"aa\"},{\"fid\":3,\"name\":\"bb\"}]}"
        );*/


        DataStreamSource<String> streamSource = env.readTextFile("flink_course/data/transformation_input/");


        /**
         * map算子的演示
         */
        // 把每条json数据，转成javabean数据
        SingleOutputStreamOperator<UserInfo> beanStream = streamSource.map(json -> JSON.parseObject(json, UserInfo.class));
        /*beanStream.print();*/


        /**
         * filter算子的演示
         *   请过滤掉好友超过3位的用户数据
         */
        SingleOutputStreamOperator<UserInfo> filtered = beanStream.filter(bean -> bean.getFriends().size() <= 3);
        /*filtered.print();*/


        /**
         * flatmap算子的演示
         *  把每个用户的好友信息，全部提取出来（带上用户自身的信息），并且压平，放入结果流中
         *  {"uid":1,"name":"zs","gender":"male","friends":[{"fid":2,"name":"aa"},{"fid":3,"name":"bb"}]}
         *  =>
         *  {"uid":1,"name":"zs","gender":"male","fid":2,"fname":"aa"}
         *  {"uid":1,"name":"zs","gender":"male","fid":3,"fname":"bb"}
         */
        SingleOutputStreamOperator<UserFriendInfo> flatten = filtered.flatMap(new FlatMapFunction<UserInfo, UserFriendInfo>() {
            @Override
            public void flatMap(UserInfo value, Collector<UserFriendInfo> out) throws Exception {
                // 把friends列表提取出来，一个一个地返回
                List<FriendInfo> friends = value.getFriends();
                /* friends.forEach(x->out.collect(new UserFriendInfo(value.getUid(), value.getName(), value.getGender(),x.getFid(),x.getName() )));*/
                for (FriendInfo x : friends) {
                    out.collect(new UserFriendInfo(value.getUid(), value.getName(), value.getGender(), x.getFid(), x.getName()));
                }
            }
        });
        /* flatten.print();*/


        /**
         * keyBy算子的演示
         * 对上一步的结果，按用户性别分组
         *
         * 滚动聚合算子（只能在 KeyedStream 流上调用）：  sum算子 、 min算子 、 minBy算子 、 max算子、  maxBy算子、 reduce算子的演示
         * 并统计：
         *    各性别用户的好友总数
         *
         *    各性别中，用户好友数量最大值
         *    各性别中，用户好友数量最小值
         *
         *    求各性别中，好友数量最大的那个人
         *    求各性别中，好友数量最小的那个人
         *
         */
        // 各性别用户的好友总数
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = flatten.map(bean -> Tuple2.of(bean.getGender(), 1)).returns(new TypeHint<Tuple2<String, Integer>>() {
                })
                .keyBy(tp -> tp.f0);
        
        keyedStream
                .sum(1);
        /*genderFriendCount.print();*/

        // 各性别中，用户好友数量最大值

        /**
         * max / maxBy  都是滚动聚合：  算子不会把收到的所有数据全部攒起来；而是只在状态中记录上一次的聚合值，然后当新数据到达的时候，会根据逻辑去更新 状态中记录的聚合值，并输出最新状态数据
         * max / maxBy  区别： 更新状态的逻辑！  max只更新要求最大值的字段；  而 maxBy 会更新整条数据；
         * 同理，min和minBy也如此
         */
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> tuple4Stream = beanStream.map(bean -> Tuple4.of(bean.getUid(), bean.getName(), bean.getGender(), bean.getFriends().size())).returns(new TypeHint<Tuple4<Integer, String, String, Integer>>() {
        });


        tuple4Stream
                .keyBy(tp -> tp.f2)
                /*.max(3); */              // 各性别中，用户好友数量最大值
                .maxBy(3);  // 求各性别中，好友数量最大的那个人

        /*genderUserFriendsMaxCount.print();*/


        /**
         * reduce 算子 使用演示
         * 需求： 求各性别中，好友数量最大的那个人，而且如果前后两个人的好友数量相同，则输出的结果中，也需要将uid/name等信息更新成后面一条数据的值
         *
         */
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> reduceResult = tuple4Stream.keyBy(tp -> tp.f2)
                .reduce(new ReduceFunction<Tuple4<Integer, String, String, Integer>>() {
                    /**
                     *
                     * @param value1  是此前的聚合结果
                     * @param value2  是本次的新数据
                     * @return 更新后的聚合结果
                     * @throws Exception
                     */
                    @Override
                    public Tuple4<Integer, String, String, Integer> reduce(Tuple4<Integer, String, String, Integer> value1, Tuple4<Integer, String, String, Integer> value2) throws Exception {

                        if (value1 == null || value2.f3 >= value1.f3) {
                            return value2;
                        } else {
                            return value1;
                        }
                    }
                });
        /*reduceResult.print();*/


        /**
         * 用reduce，来实现sum算子的功能
         * 求：  对上面的  4元组数据  1,ua,male,2  ，求各性别的好友数总和
         * TODO
         */
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> reduceSum = tuple4Stream.keyBy(tp -> tp.f2)
                .reduce(new ReduceFunction<Tuple4<Integer, String, String, Integer>>() {
                    @Override
                    public Tuple4<Integer, String, String, Integer> reduce(Tuple4<Integer, String, String, Integer> value1, Tuple4<Integer, String, String, Integer> value2) throws Exception {
                        value2.setField(value2.f3 + (value1 == null ? 0 : value1.f3), 3);
                        return value2;
                    }
                });
        reduceSum.print();


        env.execute();

    }
}

@Data
class UserInfo implements Serializable {
    private int uid;
    private String name;
    private String gender;
    private List<FriendInfo> friends;
}

@Data
class FriendInfo implements Serializable {
    private int fid;
    private String name;
}

@Data
@AllArgsConstructor
class UserFriendInfo implements Serializable {
    private int uid;
    private String name;
    private String gender;
    private int fid;
    private String fname;

}