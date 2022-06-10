package cn.doitedu.flinksql.demos;


/**
 *
 * 基本： kafka中有如下数据：
 * {"id":1,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
 *
 * 高级：kafka中有如下数据：
 * {"id":1,"name":{"formal":"zs","nick":"tiedan"},"age":18,"gender":"male"}
 *
 *
 * 现在需要用flinkSql来对上述数据进行查询统计：
 *   截止到当前,每个昵称,都有多少个用户
 *   截止到当前,每个性别,年龄最大值
 *
 */
public class Demo6_Exercise {

}
