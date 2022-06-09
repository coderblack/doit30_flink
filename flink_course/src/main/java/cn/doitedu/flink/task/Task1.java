package cn.doitedu.flink.task;

public class Task1 implements Runnable{

    @Override
    public void run() {

        // 从上游接收数据
        //String data = receive();

        Mapper1 mapper1 = new Mapper1();
       //String res = mapper1.map(data);

        // 把结果发往下游
        // channel.send(res);
    }

}
