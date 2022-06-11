package cn.doitedu.module;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;

public class LogRunnable implements Runnable {
    List<LogBeanWrapper> userPart;
    Collector collector;
    int slowFactor;

    /**
     *
     * @param userPart
     * @param collector
     * @param slowFactor  速度因子，事件间隔时长=RandomUtils.nextInt(50+10*slowFactor,100*slowFactor)
     */
    public LogRunnable(List<LogBeanWrapper> userPart,Collector collector,int slowFactor) {
        this.userPart = userPart;
        this.collector = collector;
        this.slowFactor = slowFactor;
    }

    @Override
    public void run() {

        try {
            long off = 0;
            while(true) {
                LogBeanWrapper logBeanWrapper = userPart.get(RandomUtils.nextInt(0, userPart.size()));

                int eventCnt = RandomUtils.nextInt(5, 50);
                for (int i = 0; i < eventCnt; i++) {
                    if(logBeanWrapper.getSessionMax()>2) break;

                    if (logBeanWrapper.isExists() ) {   // 关闭后的启动状态
                        logBeanWrapper.getLogBean().setEventId("launch");
                        logBeanWrapper.getLogBean().setProperties(new HashMap<String, String>());

                        // 设置会话id
                        String sessionId = RandomStringUtils.randomAlphabetic(10).toUpperCase();
                        logBeanWrapper.getLogBean().setSessionId(sessionId);
                        logBeanWrapper.getLogBean().setTimeStamp(System.currentTimeMillis());
                        collector.collect(JSON.toJSONString(logBeanWrapper.getLogBean()));

                        // 设置状态
                        logBeanWrapper.setSessionId(sessionId);
                        logBeanWrapper.setLastTime(logBeanWrapper.getLogBean().getTimeStamp());
                        logBeanWrapper.setExists(false);
                        logBeanWrapper.setPushback(false);


                        Thread.sleep(10*slowFactor);

                        // 生成页面加载事件
                        logBeanWrapper.getLogBean().setEventId("pageLoad");
                        HashMap<String, String> properties = new HashMap<String, String>();
                        properties.put("pageId", "index");
                        logBeanWrapper.getLogBean().setProperties(properties);
                        logBeanWrapper.getLogBean().setTimeStamp(System.currentTimeMillis());
                        collector.collect(JSON.toJSONString(logBeanWrapper.getLogBean()));

                        // 设置状态
                        logBeanWrapper.setSessionMax(logBeanWrapper.getSessionMax()+1);


                    } else if (logBeanWrapper.isPushback()) {  // 放入后台状态
                        HashMap<String, String> properties = new HashMap<String, String>();
                        logBeanWrapper.getLogBean().setEventId("wakeup");

                        properties.put("pageId", logBeanWrapper.getLogBean().getProperties().get("pageId"));

                        logBeanWrapper.getLogBean().setProperties(properties);
                        logBeanWrapper.getLogBean().setTimeStamp(System.currentTimeMillis());

                        collector.collect(JSON.toJSONString(logBeanWrapper.getLogBean()));

                        // 设置状态
                        logBeanWrapper.setExists(false);
                        logBeanWrapper.setPushback(false);

                    } else {  // 其他使用状态
                        logBeanWrapper.setExists(false);  // 设置状态
                        logBeanWrapper.setPushback(false);

                        setRandomEvent(logBeanWrapper,collector);  // 内部特殊事件还会覆盖状态
                    }


                    Thread.sleep(RandomUtils.nextInt(50+10*slowFactor,100*slowFactor));
                }

                off++;
                if(off> userPart.size() *3*3){
                    System.out.printf("全部用户用完,线程 (%s) 退出 \n",Thread.currentThread().getId());
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void setRandomEvent(LogBeanWrapper wrapperBean,Collector collector) throws InterruptedException {
        int rand = RandomUtils.nextInt(1, 20);

        if (rand % 6 == 0) {  // 放入后台
            HashMap<String, String> props = new HashMap<>();
            props.put("pageId",wrapperBean.getLogBean().getProperties().get("pageId"));
            wrapperBean.getLogBean().setEventId("pushback");
            wrapperBean.getLogBean().setProperties(props);
            wrapperBean.getLogBean().setTimeStamp(System.currentTimeMillis());
            collector.collect(JSON.toJSONString(wrapperBean.getLogBean()));

            // 设置状态为 后台
            wrapperBean.setExists(false);
            wrapperBean.setPushback(true);

        }
        else if(rand % 7 == 0){  // 关闭 app
            HashMap<String, String> props = new HashMap<>();
            props.put("pageId",wrapperBean.getLogBean().getProperties().get("pageId"));
            wrapperBean.getLogBean().setEventId("appClose");
            wrapperBean.getLogBean().setProperties(props);
            wrapperBean.getLogBean().setTimeStamp(System.currentTimeMillis());
            collector.collect(JSON.toJSONString(wrapperBean.getLogBean()));

            // 设置状态为退出
            wrapperBean.setExists(true);
            wrapperBean.setPushback(false);

        }
        else if(!wrapperBean.isPushback() && !wrapperBean.isExists() && rand % 3 ==0){ // 切换页面的事件
            int pCnt = RandomUtils.nextInt(1, 5); // 属性个数

            wrapperBean.getLogBean().setEventId("click-"+RandomStringUtils.randomAlphabetic(3).toLowerCase()+"-"+pCnt);
            HashMap<String, String> properties = new HashMap<String, String>();
            for (int i = 0; i < pCnt; i++) {
                properties.put("p" + RandomUtils.nextInt(1, 11), "v" + RandomUtils.nextInt(1, 3));
            }
            properties.put("pageId", wrapperBean.getLogBean().getProperties().get("pageId"));
            wrapperBean.getLogBean().setProperties(properties);
            wrapperBean.getLogBean().setTimeStamp(System.currentTimeMillis());
            collector.collect(JSON.toJSONString(wrapperBean.getLogBean()));

            // 再发出一个页面加载事件
            Thread.sleep(10*slowFactor);

            // 获取新页面
            String pageId = "page" + StringUtils.leftPad(RandomUtils.nextInt(1, 3)+"",3,"0");
            wrapperBean.getLogBean().setEventId("pageload");
            HashMap<String, String> props = new HashMap<String, String>();
            props.put("pageId", pageId);

            wrapperBean.getLogBean().setProperties(props);
            wrapperBean.getLogBean().setTimeStamp(System.currentTimeMillis());

            collector.collect(JSON.toJSONString(wrapperBean.getLogBean()));

            // 设置状态

        }else{
            setRandomEvent(wrapperBean.getLogBean());
            wrapperBean.getLogBean().setTimeStamp(System.currentTimeMillis());
            collector.collect(JSON.toJSONString(wrapperBean.getLogBean()));

            // 设置状态
        }
    }

    public void setRandomEvent(LogBean logBean){
        int pCnt = RandomUtils.nextInt(1, 5); // 属性个数
        logBean.setEventId("e_"+RandomStringUtils.randomAlphabetic(2).toLowerCase()+"_"+pCnt);

        HashMap<String, String> properties = new HashMap<String, String>();
        for (int i = 0; i < pCnt; i++) {
            properties.put("p" + RandomUtils.nextInt(10, 20), "v" + RandomUtils.nextInt(1, 10));
        }
        properties.put("pageId", logBean.getProperties().get("pageId"));
        logBean.setProperties(properties);
    }
}
