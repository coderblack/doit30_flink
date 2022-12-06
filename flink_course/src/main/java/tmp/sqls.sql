--sql1
CREATE TABLE user_slot_game_log
(
`GAME_NUM`          String,
`USER_ID`           bigint,
`AGENT_ID`          bigint,
`GAME_ID`           bigint,
`SERVER_ID`         bigint,
`GAME_TYPE`         bigint,
`CONTROL_TYPE`      bigint,
`BET_MONEY`         bigint,
`WIN_MONEY`         bigint,
`BEGIN_MONEY`       bigint,
`END_MONEY`         bigint,
`BEGIN_CREDIT`      bigint,
`END_CREDIT`        bigint,
`MATCH_UI_INFO`     String,
`MATCH_RESULT`      String,
`BUFF0`             bigint,
`BUFF1`             bigint,
`BUFF2`             bigint,
`BUFF3`             bigint,
`BUFF4`             bigint,
`BUFF5`             bigint,
`LOG_DATE`          timestamp(3),
`NOW_EXP`           double,
  -- 声明 user_action_time 是事件时间属性，并且用 延迟 # 秒的策略来生成 watermark
WATERMARK FOR LOG_DATE AS LOG_DATE - INTERVAL '5' SECOND

-- primary key(`LOG_DATE`,`GAME_ID`,`SERVER_ID`) NOT ENFORCED
) WITH (
      'connector' = 'kafka',
      'topic' = 'user_slot_game_log',
      'properties.bootstrap.servers' = '47.242.160.190:9092,47.242.250.174:9092,47.242.36.238:9092',
      'properties.group.id' = 'flink_02',
      'scan.startup.mode' = 'latest-offset',
      -- csv json ...
      'format' = 'json'
      )

~
--sql2

Create Table `Test01` (
 `log_date` timestamp(3) COMMENT '日期',
 `game_id` bigint COMMENT '游戏ID',
 `server_id` bigint COMMENT '服务器ID',
 `game_user` bigint COMMENT '游戏人数',
 `game_rate` bigint COMMENT '游戏几率',
 `total_bet_money` bigint COMMENT '总玩分',
 `total_win_money` bigint COMMENT '总赢分',
 `total_game_num` bigint COMMENT '总局数',
 `main_game_win_money` bigint COMMENT '主游戏赢分',
 `free_game_sin_money` bigint COMMENT '免费游戏赢分',
 `bonus_game_sin_money` bigint COMMENT 'BONUS游戏赢分',
 `win_user` bigint COMMENT '赢分玩家人数',
 `lose_user` bigint COMMENT '输分玩家人数',
 `bet_game_num` bigint COMMENT '消耗玩分局数',
primary key (`log_date`,`game_id`,`server_id`) NOT ENFORCED
)WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://am-j6c14v9j8w9i334ha167320o.ads.aliyuncs.com:3306/domino_ads',
  'table-name' = 'slot_game_report_rt' ,
  'username' = 'gcroot' ,
  'password' = '5iFBNyK2PXhKdPnv'
)

~
--sql3
insert into Test01
(
    log_date,
	game_id,
	server_id,
	game_user,
	game_rate,
	total_bet_money,
	total_win_money,
	total_game_num,
	main_game_win_money,
	free_game_sin_money,
	bonus_game_sin_money,
	win_user,
	lose_user,
	bet_game_num
)
select
LOG_DATE
,GAME_ID
,SERVER_ID
,count(distinct USER_ID)  as game_user  -- 游戏人数
,sum(total_win_money) / sum(total_bet_money) as game_rate  --游戏几率
,sum(total_bet_money) as total_bet_money   -- 总玩分
,sum(total_win_money) as total_win_money  --总赢分
,sum(total_game_num)  as total_game_num          --总局数
,sum(main_game_win_money) as main_game_win_money
,sum(free_game_sin_money) as free_game_sin_money
,sum(bonus_game_sin_money) as bonus_game_sin_money
,count(distinct case when total_bet_money - total_win_money > 0 then USER_ID end) as win_user
,count(distinct case when total_bet_money - total_win_money < 0 then USER_ID end) as lose_user
,sum(bet_game_num) as bet_game_num
from (
  SELECT
  GAME_ID,SERVER_ID,USER_ID
  ,max(LOG_DATE) as LOG_DATE
  ,sum(BET_MONEY) as total_bet_money      -- 总玩分
  ,sum(WIN_MONEY) as total_win_money      -- 总赢分
  ,sum(BUFF0) as total_game_num           --总局数
  ,sum (case when GAME_TYPE = 0 then BUFF0 else 0 end)  as main_game_win_money      -- 主游戏赢分
  ,sum (case when GAME_TYPE = 1 then BUFF0 else 0 end)  as free_game_sin_money        --免费游戏赢分
  ,sum (case when GAME_TYPE = 2 then BUFF0 else 0 end)  as bonus_game_sin_money       -- bonus游戏赢分
  ,sum (case when BET_MONEY > 0 then BUFF0 else 0 end)  as bet_game_num       --消耗玩分局数
  from user_slot_game_log t
--  where LOG_DATE >= cast(current_date as String)
  group by GAME_ID,SERVER_ID,USER_ID,TUMBLE(LOG_DATE, INTERVAL '60' second)
)
group by LOG_DATE,GAME_ID,SERVER_ID


~

select
LOG_DATE
,GAME_ID
,SERVER_ID
,count(distinct USER_ID)  as game_user  -- 游戏人数
,sum(total_win_money) / sum(total_bet_money) as game_rate  --游戏几率
,sum(total_bet_money) as total_bet_money   -- 总玩分
,sum(total_win_money) as total_win_money  --总赢分
,sum(total_game_num)  as total_game_num          --总局数
,sum(main_game_win_money) as main_game_win_money
,sum(free_game_sin_money) as free_game_sin_money
,sum(bonus_game_sin_money) as bonus_game_sin_money
,count(distinct case when total_bet_money - total_win_money > 0 then USER_ID end) as win_user
,count(distinct case when total_bet_money - total_win_money < 0 then USER_ID end) as lose_user
,sum(bet_game_num) as bet_game_num
from (
  SELECT
  GAME_ID,SERVER_ID,USER_ID
  ,max(LOG_DATE) as LOG_DATE
  ,sum(BET_MONEY) as total_bet_money      -- 总玩分
  ,sum(WIN_MONEY) as total_win_money      -- 总赢分
  ,sum(BUFF0) as total_game_num           --总局数
  ,sum (case when GAME_TYPE = 0 then BUFF0 else 0 end)  as main_game_win_money      -- 主游戏赢分
  ,sum (case when GAME_TYPE = 1 then BUFF0 else 0 end)  as free_game_sin_money        --免费游戏赢分
  ,sum (case when GAME_TYPE = 2 then BUFF0 else 0 end)  as bonus_game_sin_money       -- bonus游戏赢分
  ,sum (case when BET_MONEY > 0 then BUFF0 else 0 end)  as bet_game_num       --消耗玩分局数
  from user_slot_game_log t
--  where LOG_DATE >= cast(current_date as String)
  group by GAME_ID,SERVER_ID,USER_ID,TUMBLE(LOG_DATE, INTERVAL '5' second)
)
group by LOG_DATE,GAME_ID,SERVER_ID





