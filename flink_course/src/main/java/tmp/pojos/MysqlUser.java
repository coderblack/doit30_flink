package tmp.pojos;

import java.io.Serializable;

public class MysqlUser implements Serializable {

    public String log_date;
    public long game_id;
    public long server_id;
    public long game_user;
    public long game_rate;
    public long total_bet_money;
    public long total_win_money;
    public long total_game_num;
    public long main_game_win_money;
    public long free_game_sin_money;
    public long bonus_game_sin_money;
    public long win_user;
    public long lose_user;
    public long bet_game_num;

    public MysqlUser() {
    }

    public MysqlUser(String log_date, long game_id, long server_id, long game_user, long game_rate, long total_bet_money, long total_win_money, long total_game_num,
                     long main_game_win_money, long free_game_sin_money, long bonus_game_sin_money, long win_user, long lose_user, long bet_game_num) {
        this.log_date = log_date;
        this.game_id = game_id;
        this.server_id = server_id;
        this.game_user = game_user;
        this.game_rate = game_rate;
        this.total_bet_money = total_bet_money;
        this.total_win_money = total_win_money;
        this.total_game_num = total_game_num;
        this.main_game_win_money = main_game_win_money;
        this.free_game_sin_money = free_game_sin_money;
        this.bonus_game_sin_money = bonus_game_sin_money;
        this.win_user = win_user;
        this.lose_user = lose_user;
        this.bet_game_num = bet_game_num;
    }

    public String getLog_date() {
        return log_date;
    }

    public long getGame_id() {
        return game_id;
    }

    public long getServer_id() {
        return server_id;
    }

    public long getGame_user() {
        return game_user;
    }

    public long getGame_rate() {
        return game_rate;
    }

    public long getTotal_bet_money() {
        return total_bet_money;
    }

    public long getTotal_win_money() {
        return total_win_money;
    }

    public long getTotal_game_num() {
        return total_game_num;
    }

    public long getMain_game_win_money() {
        return main_game_win_money;
    }

    public long getFree_game_sin_money() {
        return free_game_sin_money;
    }

    public long getBonus_game_sin_money() {
        return bonus_game_sin_money;
    }

    public long getWin_user() {
        return win_user;
    }

    public long getLose_user() {
        return lose_user;
    }

    public long getBet_game_num() {
        return bet_game_num;
    }

    public void setLog_date(String log_date) {
        this.log_date = log_date;
    }

    public void setGame_id(long game_id) {
        this.game_id = game_id;
    }

    public void setServer_id(long server_id) {
        this.server_id = server_id;
    }

    public void setGame_user(long game_user) {
        this.game_user = game_user;
    }

    public void setGame_rate(long game_rate) {
        this.game_rate = game_rate;
    }

    public void setTotal_bet_money(long total_bet_money) {
        this.total_bet_money = total_bet_money;
    }

    public void setTotal_win_money(long total_win_money) {
        this.total_win_money = total_win_money;
    }

    public void setTotal_game_num(long total_game_num) {
        this.total_game_num = total_game_num;
    }

    public void setMain_game_win_money(long main_game_win_money) {
        this.main_game_win_money = main_game_win_money;
    }

    public void setFree_game_sin_money(long free_game_sin_money) {
        this.free_game_sin_money = free_game_sin_money;
    }

    public void setBonus_game_sin_money(long bonus_game_sin_money) {
        this.bonus_game_sin_money = bonus_game_sin_money;
    }

    public void setWin_user(long win_user) {
        this.win_user = win_user;
    }

    public void setLose_user(long lose_user) {
        this.lose_user = lose_user;
    }

    public void setBet_game_num(long bet_game_num) {
        this.bet_game_num = bet_game_num;
    }

    @Override
    public String toString() {
        return "MysqlUser{" +
                "log_date='" + log_date + '\'' +
                ", game_id=" + game_id +
                ", server_id=" + server_id +
                ", game_user=" + game_user +
                ", game_rate=" + game_rate +
                ", total_bet_money=" + total_bet_money +
                ", total_win_money=" + total_win_money +
                ", total_game_num=" + total_game_num +
                ", main_game_win_money=" + main_game_win_money +
                ", free_game_sin_money=" + free_game_sin_money +
                ", bonus_game_sin_money=" + bonus_game_sin_money +
                ", win_user=" + win_user +
                ", lose_user=" + lose_user +
                ", bet_game_num=" + bet_game_num +
                '}';
    }
}
