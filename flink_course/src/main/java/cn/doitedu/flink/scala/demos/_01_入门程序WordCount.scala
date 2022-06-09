package cn.doitedu.flink.scala.demos

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object _01_入门程序WordCount {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceStream = env.socketTextStream("doit01", 9999)

    // sourceStream.flatMap(s=>s.split("\\s+")).map(w=>(w,1))

    sourceStream
      .flatMap(s => {
        s.split("\\s+").map(w => (w, 1))
      })
      .keyBy(tp => tp._1)
      .sum(1)
      .print("我爱你")

    env.execute("我的job"); // 提交job

  }

}
