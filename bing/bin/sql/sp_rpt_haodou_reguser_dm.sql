
set hive.auto.convert.join=false;

--好豆用户注册日表

--创建临时表
create table if not exists bing.tmp_haodou_reguser_dm
(
  userid   string,
  fromway  int
) comment '好豆用户注册日表临时表'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

--保留历史数据
insert overwrite table bing.rpt_haodou_reguser_dm
select *
from bing.rpt_haodou_reguser_dm
where statis_date!='${statis_date}'
;

--生成当天注册用户临时表
insert overwrite table bing.tmp_haodou_reguser_dm partition (statis_date='${statis_date}')
select userid, fromway
from haodou_passport_${curdate}.`User`
where regtime between '${statis_date} 00:00:00' and '${statis_date} 23:59:59'
and status = 1 
;

--生成结果数据
insert into table bing.rpt_haodou_reguser_dm
select
'${statis_date}' as statis_date,
count(u.userid) as usernum, 
count(case when u.FromWay=0 and cc.userid is null then u.userid end) as email_usernum,
count(case when u.FromWay=1 and cc.userid is null then u.userid end) as phone_usernum,
count(cc.userid) as 3p_usernum,
count(case when cc.siteid=1 then cc.userid end) as 163_usernum,
count(case when cc.siteid=2 then cc.userid end) as tqq_usernum,
count(case when cc.siteid=3 then cc.userid end) as weibo_usernum,
count(case when cc.siteid=4 then cc.userid end) as douban_usernum,
count(case when cc.siteid=5 then cc.userid end) as sohu_usernum,
count(case when cc.siteid=6 then cc.userid end) as qzone_usernum,
count(case when cc.siteid=7 then cc.userid end) as taobao_usernum,
count(case when cc.siteid=8 then cc.userid end) as weixin_usernum,
count(case when u.FromWay not in (0,1) and cc.userid is null then u.userid end) as 3p_nobind
from 
(select *
from bing.tmp_haodou_reguser_dm
where statis_date='${statis_date}'
) u left outer join
(select userid, siteid
from haodou_connect_recipe_${curdate}.Connect
) cc
on (u.userid=cc.userid)
;
