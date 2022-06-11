package cn.doitedu.module;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class UserUtils {

    public static String[] carriers = {"中国移动","中国联通","中国电信","小米移动","华为移动"};
    public static String[] androidDeviceTypes = {"mi5","mi6","mi7","mi8","mi10","ry6","ry7","ry8","ry9","oppo6","oppo7","oppo8","oppo9"};
    public static String[] iosDeviceTypes = {"iphone6","iphone7","iphone8","iphone9","iphone10","iphone11"};
    public static String[] channels = {"小米应用","华为应用","豌豆荚","360应用"};
    public static String[] osVerions = {"7.0","8.0","9.0","10.0"};
    public static String[] netTypes = {"5G","4G","WIFI"};
    public static String[] resolutions = {"1024*768","1280*768","2048*1366","1366*768"};
    public static String[] appVersions = {"2.1","2.5","2.6","2.8","3.0","3.2"};

    public static HashMap<String,LogBean> loadHisUsers(String filePath) throws Exception {
        HashMap<String,LogBean> users = new HashMap<>();
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line =  null;
        while((line=br.readLine())!=null){
            LogBean logBean = JSON.parseObject(line, LogBean.class);
            users.put(logBean.getDeviceId()+logBean.getAccount(),logBean);
        }
        br.close();
        System.out.println("历史用户数据加载完成!");
        return users;
    }

    public static String addNewUsers(HashMap<String,LogBean> users,int cnt,boolean save) throws IOException {

        for (int i = 0; i < cnt; i++) {
            LogBean logBean = new LogBean();
            // 生成的账号形如： 004078
            String account = RandomStringUtils.randomAlphabetic(5).toLowerCase();
            logBean.setAccount(account);
            logBean.setAppId("cn.doitedu.yinew");
            logBean.setAppVersion(appVersions[RandomUtils.nextInt(0,appVersions.length)]);
            logBean.setCarrier(carriers[RandomUtils.nextInt(0,carriers.length)]);
            // deviceid直接用account
            logBean.setDeviceId(RandomStringUtils.randomAlphabetic(4).toLowerCase()+"-"+RandomStringUtils.randomNumeric(4));
            logBean.setIp(RandomUtils.nextInt(10, 12) + "." + RandomUtils.nextInt(20, 22) + "." + RandomUtils.nextInt(100, 102) + "." + RandomUtils.nextInt(60, 66));
            logBean.setLatitude(RandomUtils.nextDouble(10.0, 52.0));
            logBean.setLongitude(RandomUtils.nextDouble(120.0, 160.0));
            if (RandomUtils.nextInt(1, 10) % 4 == 0) {
                logBean.setDeviceType(androidDeviceTypes[RandomUtils.nextInt(0, androidDeviceTypes.length)]);
                logBean.setOsName("android");
                logBean.setOsVersion(osVerions[RandomUtils.nextInt(0, osVerions.length)]);
                logBean.setReleaseChannel(channels[RandomUtils.nextInt(0, channels.length)]);
            } else {
                logBean.setDeviceType(iosDeviceTypes[RandomUtils.nextInt(0, iosDeviceTypes.length)]);
                logBean.setOsName("ios");
                logBean.setOsVersion(osVerions[RandomUtils.nextInt(0, osVerions.length)]);
                logBean.setReleaseChannel("apple-store");
            }
            logBean.setNetType(netTypes[RandomUtils.nextInt(0, netTypes.length)]);
            logBean.setResolution(resolutions[RandomUtils.nextInt(0, resolutions.length)]);

            users.put(logBean.getDeviceId()+logBean.getAccount(),logBean);
        }

        if(save) {
            String s = saveUsers(users);
            return s;
        }
        return null;
    }

    public static String saveUsers(HashMap<String,LogBean> users) throws IOException {
        String filePath = "data/users/hisu-" + System.currentTimeMillis() + ".txt";
        BufferedWriter bw = new BufferedWriter(new FileWriter(filePath));
        for (LogBean value : users.values()) {
            String s = JSON.toJSONString(value);
            bw.write(s);
            bw.newLine();
        }
        bw.flush();
        bw.close();
        System.out.println("用户数据保存完毕!");

        return filePath;
    }

    public static List<LogBeanWrapper> userToWrapper(HashMap<String,LogBean> users){

        List<LogBeanWrapper> wrapper = new ArrayList<>();

        for (String s : users.keySet()) {
            wrapper.add(new LogBeanWrapper(users.get(s),RandomStringUtils.randomAlphabetic(10).toLowerCase(),System.currentTimeMillis()));
        }

        return wrapper;
    }



}
