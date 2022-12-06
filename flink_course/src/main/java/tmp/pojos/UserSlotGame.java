package tmp.pojos;


import java.io.Serializable;

public class UserSlotGame implements Serializable {

    public String    GAME_NUM;
    public Long      USER_ID;
    public Long      AGENT_ID;
    public Long      GAME_ID;
    public Long      SERVER_ID;
    public Long      GAME_TYPE;
    public Long      CONTROL_TYPE;
    public Long      BET_MONEY;
    public Long      WIN_MONEY;
    public Long      BEGIN_MONEY;
    public Long      END_MONEY;
    public Long      BEGIN_CREDIT;
    public Long      END_CREDIT;
    public String    MATCH_UI_INFO;
    public String    MATCH_RESULT;
    public Long      BUFF0;
    public Long      BUFF1;
    public Long      BUFF2;
    public Long      BUFF3;
    public Long      BUFF4;
    public Long      BUFF5;
    public String    LOG_DATE;
    public Double    NOW_EXP;

    public UserSlotGame() {
    }

    public UserSlotGame(String GAME_NUM, Long USER_ID, Long AGENT_ID, Long GAME_ID, Long SERVER_ID, Long GAME_TYPE, Long CONTROL_TYPE, Long BET_MONEY, Long WIN_MONEY, Long BEGIN_MONEY, Long END_MONEY, Long BEGIN_CREDIT, Long END_CREDIT,
                        String MATCH_UI_INFO, String MATCH_RESULT, Long BUFF0, Long BUFF1, Long BUFF2, Long BUFF3, Long BUFF4, Long BUFF5, String LOG_DATE, Double NOW_EXP) {
        this.GAME_NUM = GAME_NUM;
        this.USER_ID = USER_ID;
        this.AGENT_ID = AGENT_ID;
        this.GAME_ID = GAME_ID;
        this.SERVER_ID = SERVER_ID;
        this.GAME_TYPE = GAME_TYPE;
        this.CONTROL_TYPE = CONTROL_TYPE;
        this.BET_MONEY = BET_MONEY;
        this.WIN_MONEY = WIN_MONEY;
        this.BEGIN_MONEY = BEGIN_MONEY;
        this.END_MONEY = END_MONEY;
        this.BEGIN_CREDIT = BEGIN_CREDIT;
        this.END_CREDIT = END_CREDIT;
        this.MATCH_UI_INFO = MATCH_UI_INFO;
        this.MATCH_RESULT = MATCH_RESULT;
        this.BUFF0 = BUFF0;
        this.BUFF1 = BUFF1;
        this.BUFF2 = BUFF2;
        this.BUFF3 = BUFF3;
        this.BUFF4 = BUFF4;
        this.BUFF5 = BUFF5;
        this.LOG_DATE = LOG_DATE;
        this.NOW_EXP = NOW_EXP;
    }

    public String getGAME_NUM() {
        return GAME_NUM;
    }

    public Long getUSER_ID() {
        return USER_ID;
    }

    public Long getAGENT_ID() {
        return AGENT_ID;
    }

    public Long getGAME_ID() {
        return GAME_ID;
    }

    public Long getSERVER_ID() {
        return SERVER_ID;
    }

    public Long getGAME_TYPE() {
        return GAME_TYPE;
    }

    public Long getCONTROL_TYPE() {
        return CONTROL_TYPE;
    }

    public Long getBET_MONEY() {
        return BET_MONEY;
    }

    public Long getWIN_MONEY() {
        return WIN_MONEY;
    }

    public Long getBEGIN_MONEY() {
        return BEGIN_MONEY;
    }

    public Long getEND_MONEY() {
        return END_MONEY;
    }

    public Long getBEGIN_CREDIT() {
        return BEGIN_CREDIT;
    }

    public Long getEND_CREDIT() {
        return END_CREDIT;
    }

    public String getMATCH_UI_INFO() {
        return MATCH_UI_INFO;
    }

    public String getMATCH_RESULT() {
        return MATCH_RESULT;
    }

    public Long getBUFF0() {
        return BUFF0;
    }

    public Long getBUFF1() {
        return BUFF1;
    }

    public Long getBUFF2() {
        return BUFF2;
    }

    public Long getBUFF3() {
        return BUFF3;
    }

    public Long getBUFF4() {
        return BUFF4;
    }

    public Long getBUFF5() {
        return BUFF5;
    }

    public String getLOG_DATE() {
        return LOG_DATE;
    }

    public Double getNOW_EXP() {
        return NOW_EXP;
    }

    public void setGAME_NUM(String GAME_NUM) {
        this.GAME_NUM = GAME_NUM;
    }

    public void setUSER_ID(Long USER_ID) {
        this.USER_ID = USER_ID;
    }

    public void setAGENT_ID(Long AGENT_ID) {
        this.AGENT_ID = AGENT_ID;
    }

    public void setGAME_ID(Long GAME_ID) {
        this.GAME_ID = GAME_ID;
    }

    public void setSERVER_ID(Long SERVER_ID) {
        this.SERVER_ID = SERVER_ID;
    }

    public void setGAME_TYPE(Long GAME_TYPE) {
        this.GAME_TYPE = GAME_TYPE;
    }

    public void setCONTROL_TYPE(Long CONTROL_TYPE) {
        this.CONTROL_TYPE = CONTROL_TYPE;
    }

    public void setBET_MONEY(Long BET_MONEY) {
        this.BET_MONEY = BET_MONEY;
    }

    public void setWIN_MONEY(Long WIN_MONEY) {
        this.WIN_MONEY = WIN_MONEY;
    }

    public void setBEGIN_MONEY(Long BEGIN_MONEY) {
        this.BEGIN_MONEY = BEGIN_MONEY;
    }

    public void setEND_MONEY(Long END_MONEY) {
        this.END_MONEY = END_MONEY;
    }

    public void setBEGIN_CREDIT(Long BEGIN_CREDIT) {
        this.BEGIN_CREDIT = BEGIN_CREDIT;
    }

    public void setEND_CREDIT(Long END_CREDIT) {
        this.END_CREDIT = END_CREDIT;
    }

    public void setMATCH_UI_INFO(String MATCH_UI_INFO) {
        this.MATCH_UI_INFO = MATCH_UI_INFO;
    }

    public void setMATCH_RESULT(String MATCH_RESULT) {
        this.MATCH_RESULT = MATCH_RESULT;
    }

    public void setBUFF0(Long BUFF0) {
        this.BUFF0 = BUFF0;
    }

    public void setBUFF1(Long BUFF1) {
        this.BUFF1 = BUFF1;
    }

    public void setBUFF2(Long BUFF2) {
        this.BUFF2 = BUFF2;
    }

    public void setBUFF3(Long BUFF3) {
        this.BUFF3 = BUFF3;
    }

    public void setBUFF4(Long BUFF4) {
        this.BUFF4 = BUFF4;
    }

    public void setBUFF5(Long BUFF5) {
        this.BUFF5 = BUFF5;
    }

    public void setLOG_DATE(String LOG_DATE) {
        this.LOG_DATE = LOG_DATE;
    }

    public void setNOW_EXP(Double NOW_EXP) {
        this.NOW_EXP = NOW_EXP;
    }

    @Override
    public String toString() {
        return "UserSlotGame{" +
                "GAME_NUM='" + GAME_NUM + '\'' +
                ", USER_ID=" + USER_ID +
                ", AGENT_ID=" + AGENT_ID +
                ", GAME_ID=" + GAME_ID +
                ", SERVER_ID=" + SERVER_ID +
                ", GAME_TYPE=" + GAME_TYPE +
                ", CONTROL_TYPE=" + CONTROL_TYPE +
                ", BET_MONEY=" + BET_MONEY +
                ", WIN_MONEY=" + WIN_MONEY +
                ", BEGIN_MONEY=" + BEGIN_MONEY +
                ", END_MONEY=" + END_MONEY +
                ", BEGIN_CREDIT=" + BEGIN_CREDIT +
                ", END_CREDIT=" + END_CREDIT +
                ", MATCH_UI_INFO='" + MATCH_UI_INFO + '\'' +
                ", MATCH_RESULT='" + MATCH_RESULT + '\'' +
                ", BUFF0=" + BUFF0 +
                ", BUFF1=" + BUFF1 +
                ", BUFF2=" + BUFF2 +
                ", BUFF3=" + BUFF3 +
                ", BUFF4=" + BUFF4 +
                ", BUFF5=" + BUFF5 +
                ", LOG_DATE='" + LOG_DATE + '\'' +
                ", NOW_EXP=" + NOW_EXP +
                '}';
    }
}
