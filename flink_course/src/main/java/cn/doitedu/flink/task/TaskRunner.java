package cn.doitedu.flink.task;

public class TaskRunner {

    public static void main(String[] args) {

        // Task1的6个并行实例，每一个并行实例在flink中叫什么 subTask
        new Thread(new Task1()).start();
        new Thread(new Task1()).start();
        new Thread(new Task1()).start();
        new Thread(new Task1()).start();
        new Thread(new Task1()).start();
        new Thread(new Task1()).start();


        // Task2的6个并行实例，每一个并行实例在flink中叫什么 subTask
        new Thread(new Task2()).start();
        new Thread(new Task2()).start();
        new Thread(new Task2()).start();
        new Thread(new Task2()).start();
        new Thread(new Task2()).start();
        new Thread(new Task2()).start();


        // Task3的6个并行实例，每一个并行实例在flink中叫什么 subTask
        new Thread(new Task3()).start();
        new Thread(new Task3()).start();
        new Thread(new Task3()).start();
        new Thread(new Task3()).start();
        new Thread(new Task3()).start();
        new Thread(new Task3()).start();

    }
}
