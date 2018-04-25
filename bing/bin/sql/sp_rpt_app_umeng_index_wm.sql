
--好豆BI周报结果表4-友盟应用指标周报
--*本处数据来源为友盟

--创建临时表
create table if not exists bing.tmp_app_umeng_index_wm
(
  statis_info        string comment '统计周期信息',
  rcp_total          int comment '菜谱应用累计装机量',
  rcp_android_total  int comment '安卓菜谱累计装机量',
  rcp_iphone_total   int comment 'iPhone菜谱累计装机量',
  rcp_ipad_total     int comment 'iPad菜谱累计装机量',
  rcp_incr           int comment '菜谱应用新增装机量',
  rcp_android_incr   int comment '安卓菜谱新增装机量',
  rcp_iphone_incr    int comment 'iPhone菜谱新增装机量',
  rcp_ipad_incr      int comment 'iPad菜谱新增装机量',
  rcp_dayact         int comment '菜谱应用平均日活',
  rcp_android_dayact int comment '安卓菜谱平均日活',
  rcp_iphone_dayact  int comment 'iPhone菜谱平均日活',
  rcp_ipad_dayact    int comment 'iPad菜谱平均日活',
  rcp_android_perdur int comment '安卓菜谱单次时长',
  rcp_iphone_perdur  int comment 'iPhone菜谱单次时长',
  rcp_ipad_perdur    int comment 'iPad菜谱单次时长',
  qnc_total          int comment '去哪吃应用累计装机量',
  qnc_android_total  int comment '安卓去哪吃累计装机量',
  qnc_iphone_total   int comment 'iPhone去哪吃累计装机量',
  qnc_incr           int comment '去哪吃应用新增装机量',
  qnc_android_incr   int comment '安卓去哪吃新增装机量',
  qnc_iphone_incr    int comment 'iPhone去哪吃新增装机量',
  qnc_dayact         int comment '去哪吃应用平均日活',
  qnc_android_dayact int comment '安卓去哪吃平均日活',
  qnc_iphone_dayact  int comment 'iPhone去哪吃平均日活',
  qnc_android_perdur int comment '安卓去哪吃单次时长',
  qnc_iphone_perdur  int comment 'iPhone去哪吃单次时长'
) comment '好豆BI周报临时表4-友盟应用指标'
  partitioned by (statis_week string)
  row format delimited fields terminated by '\001' stored as textfile
;

--本期累计装机量
insert overwrite table bing.tmp_app_umeng_index_wm partition (statis_week='${statis_week}')
select '' as statis_info,
sum(case when appid in ('2','4','6') then accumulations end) as rcp_total,
sum(case when appid='2' then accumulations end) as rcp_android_total,
sum(case when appid='4' then accumulations end) as rcp_iphone_total,
sum(case when appid='6' then accumulations end) as rcp_ipad_total,
sum(case when appid in ('2','4','6') then accumulations end) as rcp_incr,
sum(case when appid='2' then accumulations end) as rcp_android_incr,
sum(case when appid='4' then accumulations end) as rcp_iphone_incr,
sum(case when appid='6' then accumulations end) as rcp_ipad_incr,
0 as rcp_dayact, 0 as rcp_android_dayact, 0 as rcp_iphone_dayact, 0 as rcp_ipad_dayact,
0 as rcp_android_perdur, 0 as rcp_iphone_perdur, 0 as rcp_ipad_perdur,
sum(case when appid in ('1','3') then accumulations end) as qnc_total, 
sum(case when appid='3' then accumulations end) as qnc_android_total, 
sum(case when appid='1' then accumulations end) as qnc_iphone_total,
sum(case when appid in ('1','3') then accumulations end) as qnc_incr, 
sum(case when appid='3' then accumulations end) as qnc_android_incr, 
sum(case when appid='1' then accumulations end) as qnc_iphone_incr,
0 as qnc_dayact, 0 as qnc_android_dayact, 0 as qnc_iphone_dayact,
0 as qnc_android_perdur, 0 as qnc_iphone_perdur
from bing.ods_umeng_index_details
where statis_date='${statisweek_lastday}'
and appid in ('1','2','3','4','6')
;

--减去上期的累计装机量，得到本期新增装机量
insert into table bing.tmp_app_umeng_index_wm partition (statis_week='${statis_week}')
select '' as statis_info,
0 as rcp_total, 0 as rcp_android_total, 0 as rcp_iphone_total, 0 as rcp_ipad_total,
-1*sum(case when appid in ('2','4','6') then accumulations end) as rcp_incr,
-1*sum(case when appid='2' then accumulations end) as rcp_android_incr,
-1*sum(case when appid='4' then accumulations end) as rcp_iphone_incr,
-1*sum(case when appid='6' then accumulations end) as rcp_ipad_incr,
0 as rcp_dayact, 0 as rcp_android_dayact, 0 as rcp_iphone_dayact, 0 as rcp_ipad_dayact,
0 as rcp_android_perdur, 0 as rcp_iphone_perdur, 0 as rcp_ipad_perdur,
0 as qnc_total, 0 as qnc_android_total, 0 as qnc_iphone_total,
-1*sum(case when appid in ('1','3') then accumulations end) as qnc_incr,
-1*sum(case when appid='3' then accumulations end) as qnc_android_incr,
-1*sum(case when appid='1' then accumulations end) as qnc_iphone_incr,
0 as qnc_dayact, 0 as qnc_android_dayact, 0 as qnc_iphone_dayact,
0 as qnc_android_perdur, 0 as qnc_iphone_perdur
from bing.ods_umeng_index_details
where statis_date='${preday7_date}'
and appid in ('1','2','3','4','6')
;

--本周平均日活
insert into table bing.tmp_app_umeng_index_wm partition (statis_week='${statis_week}')
select '' as statis_info,
0 as rcp_total, 0 as rcp_android_total, 0 as rcp_iphone_total, 0 as rcp_ipad_total,
0 as rcp_incr, 0 as rcp_android_incr, 0 as rcp_iphone_incr, 0 as rcp_ipad_incr,
floor(avg(rcp_dayact)) as rcp_dayact,
floor(avg(rcp_android_dayact)) as rcp_android_dayact,
floor(avg(rcp_iphone_dayact)) as rcp_iphone_dayact,
floor(avg(rcp_ipad_dayact)) as rcp_ipad_dayact,
floor(avg(rcp_android_perdur)) as rcp_android_perdur,
floor(avg(rcp_iphone_perdur)) as rcp_iphone_perdur,
floor(avg(rcp_ipad_perdur)) as rcp_ipad_perdur,
0 as qnc_total, 0 as qnc_android_total, 0 as qnc_iphone_total,
0 as qnc_incr, 0 as qnc_android_incr, 0 as qnc_iphone_incr, 
floor(avg(qnc_dayact)) as qnc_dayact,
floor(avg(qnc_android_dayact)) as qnc_android_dayact,
floor(avg(qnc_iphone_dayact)) as qnc_iphone_dayact,
floor(avg(qnc_android_perdur)) as qnc_android_perdur,
floor(avg(qnc_iphone_perdur)) as qnc_iphone_perdur
from
(select statis_date, 
sum(case when appid in ('2','4','6') then active_data end) as rcp_dayact,
sum(case when appid='2' then active_data end) as rcp_android_dayact,
sum(case when appid='4' then active_data end) as rcp_iphone_dayact,
sum(case when appid='6' then active_data end) as rcp_ipad_dayact,
sum(case when appid='2' then duration_data end) as rcp_android_perdur,
sum(case when appid='4' then duration_data end) as rcp_iphone_perdur,
sum(case when appid='6' then duration_data end) as rcp_ipad_perdur,
sum(case when appid in ('1','3') then active_data end) as qnc_dayact,
sum(case when appid='3' then active_data end) as qnc_android_dayact,
sum(case when appid='1' then active_data end) as qnc_iphone_dayact,
sum(case when appid='3' then duration_data end) as qnc_android_perdur,
sum(case when appid='1' then duration_data end) as qnc_iphone_perdur
from bing.ods_umeng_index_details
where statis_date between '${statisweek_firstday}' and '${statisweek_lastday}'
and appid in ('1','2','3','4','6')
group by statis_date
) t
;

--汇总入结果表
insert overwrite table bing.rpt_app_umeng_index_wm partition (statis_week='${statis_week}')
select
'${statisweek_firstday} ~ ${statisweek_lastday}' as statis_info,
sum(t.rcp_total         ) as rcp_total         ,
sum(t.rcp_android_total ) as rcp_android_total ,
sum(t.rcp_iphone_total  ) as rcp_iphone_total  ,
sum(t.rcp_ipad_total    ) as rcp_ipad_total    ,
sum(t.rcp_incr          ) as rcp_incr          ,
sum(t.rcp_android_incr  ) as rcp_android_incr  ,
sum(t.rcp_iphone_incr   ) as rcp_iphone_incr   ,
sum(t.rcp_ipad_incr     ) as rcp_ipad_incr     ,
sum(t.rcp_dayact        ) as rcp_dayact        ,
sum(t.rcp_android_dayact) as rcp_android_dayact,
sum(t.rcp_iphone_dayact ) as rcp_iphone_dayact ,
sum(t.rcp_ipad_dayact   ) as rcp_ipad_dayact   ,
sum(t.rcp_android_perdur) as rcp_android_perdur,
sum(t.rcp_iphone_perdur ) as rcp_iphone_perdur ,
sum(t.rcp_ipad_perdur   ) as rcp_ipad_perdur   ,
sum(t.qnc_total         ) as qnc_total         ,
sum(t.qnc_android_total ) as qnc_android_total ,
sum(t.qnc_iphone_total  ) as qnc_iphone_total  ,
sum(t.qnc_incr          ) as qnc_incr          ,
sum(t.qnc_android_incr  ) as qnc_android_incr  ,
sum(t.qnc_iphone_incr   ) as qnc_iphone_incr   ,
sum(t.qnc_dayact        ) as qnc_dayact        ,
sum(t.qnc_android_dayact) as qnc_android_dayact,
sum(t.qnc_iphone_dayact ) as qnc_iphone_dayact ,
sum(t.qnc_android_perdur) as qnc_android_perdur,
sum(t.qnc_iphone_perdur ) as qnc_iphone_perdur 
from bing.tmp_app_umeng_index_wm t
where t.statis_week='${statis_week}'
;
