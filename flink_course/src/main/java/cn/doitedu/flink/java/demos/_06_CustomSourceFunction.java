package cn.doitedu.flink.java.demos;


import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;

/**
 *
 * @Author: deep as the sea
 * @Site: www.51doit.com
 * @QQ: 657270652
 * @Date: 2022/4/23
 * @Desc: 自定义source算子
 *
 * 自定义source
 *    可以实现   SourceFunction  或者 RichSourceFunction , 这两者都是非并行的source算子
 *    也可实现   ParallelSourceFunction  或者 RichParallelSourceFunction , 这两者都是可并行的source算子
 *
 * -- 带 Rich的，都拥有 open() ,close() ,getRuntimeContext() 方法
 * -- 带 Parallel的，都可多实例并行执行
 **/
public class _06_CustomSourceFunction {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<EventLog> dataStreamSource = env.addSource(new MySourceFunction());
        // DataStreamSource<EventLog> dataStreamSource = env.addSource(new MyParallelSourceFunction()).setParallelism(2);
        // DataStreamSource<EventLog> dataStreamSource = env.addSource(new MyRichSourceFunction());
        // DataStreamSource<EventLog> dataStreamSource = env.addSource(new MyRichParallelSourceFunction()).setParallelism(2);
        dataStreamSource.map(JSON::toJSONString).print();

        env.execute();
    }
}


class MySourceFunction implements SourceFunction<EventLog>{
    volatile boolean flag = true;

    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {

        EventLog eventLog = new EventLog();
        String[] events = {"appLaunch","pageLoad","adShow","adClick","itemShare","itemCollect","putBack","wakeUp","appClose"};
        HashMap<String, String> eventInfoMap = new HashMap<>();

        while(flag){

            eventLog.setGuid(RandomUtils.nextLong(1,1000));
            eventLog.setSessionId(RandomStringUtils.randomAlphabetic(12).toUpperCase());
            eventLog.setTimeStamp(System.currentTimeMillis());
            eventLog.setEventId(events[RandomUtils.nextInt(0,events.length)]);

            eventInfoMap.put(RandomStringUtils.randomAlphabetic(1),RandomStringUtils.randomAlphabetic(2));
            eventLog.setEventInfo(eventInfoMap);

            ctx.collect(eventLog);

            eventInfoMap.clear();

            Thread.sleep(RandomUtils.nextInt(200,1500));
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}

class MyParallelSourceFunction implements ParallelSourceFunction<EventLog>{

    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}


class MyRichSourceFunction extends RichSourceFunction<EventLog>{

    volatile boolean flag = true;
    /**
     * source组件初始化
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {

        RuntimeContext runtimeContext = getRuntimeContext();
        // 可以从运行时上下文中，取到本算子所属的 task 的task名
        String taskName = runtimeContext.getTaskName();
        // 可以从运行时上下文中，取到本算子所属的 subTask 的subTaskId
        int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();


    }

    /**
     * source组件生成数据的过程（核心工作逻辑）
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {
        EventLog eventLog = new EventLog();
        String[] events = {"appLaunch","pageLoad","adShow","adClick","itemShare","itemCollect","putBack","wakeUp","appClose"};
        HashMap<String, String> eventInfoMap = new HashMap<>();

        while(flag){

            eventLog.setGuid(RandomUtils.nextLong(1,1000));
            eventLog.setSessionId(RandomStringUtils.randomAlphabetic(12).toUpperCase());
            eventLog.setTimeStamp(System.currentTimeMillis());
            eventLog.setEventId(events[RandomUtils.nextInt(0,events.length)]);

            eventInfoMap.put(RandomStringUtils.randomAlphabetic(1),RandomStringUtils.randomAlphabetic(2));
            eventLog.setEventInfo(eventInfoMap);

            ctx.collect(eventLog);

            eventInfoMap.clear();

            Thread.sleep(RandomUtils.nextInt(500,1500));
        }
    }


    /**
     * job取消调用的方法
     */
    @Override
    public void cancel() {
        flag = false;
    }

    /**
     * 组件关闭调用的方法
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        System.out.println("组件被关闭了.....");
    }
}

class MyRichParallelSourceFunction extends RichParallelSourceFunction<EventLog>{

    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}




