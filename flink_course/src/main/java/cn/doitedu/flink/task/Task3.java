package cn.doitedu.flink.task;

public class Task3 implements Runnable{
    @Override
    public void run() {
        Mapper1 mapper1 = new Mapper1();
        Mapper2 mapper2 = new Mapper2();


        String res1 = mapper1.map("aaaa");
        String res2 = mapper2.map(res1);
    }
}
