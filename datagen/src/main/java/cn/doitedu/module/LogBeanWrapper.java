package cn.doitedu.module;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LogBeanWrapper {
    private LogBean logBean;
    private String sessionId;
    private long lastTime;

    private boolean isExists = true;
    private boolean isPushback = false;

    //private String currPage;

    private int sessionMax = 0;

    public LogBeanWrapper(LogBean logBean,String sessionId,long lastTime){
        this.logBean = logBean;
        this.sessionId = sessionId;
        this.lastTime = lastTime;

    }



}
