
set mapreduce.map.memory.mb=8192;
set mapreduce.reduce.memory.mb=8192;

--目前作为测试，使用临时表存放结果数据
create table if not exists bing.tmp_app_dayact_dm
(
  app_id       string comment '好豆应用ID',
  act_devnum   int comment '有效活跃设备数',
  req_devnum   int comment '请求设备数'
) comment '活跃设备数日表'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;

insert overwrite table bing.tmp_app_dayact_dm partition (statis_date='${statis_date}')
select
  app_id,
  count(distinct case when not find_in_set(function_id,'notice.getpullnotice,mobiledevice.initandroiddevice,recipe.getfindrecipe,ad.getad_imocha,/Msg/Pull/getNotice')>0 then device_id end) as act_devnum,
  count(distinct device_id) as req_devnum
from bing.ods_app_requestlog_dm 
where statis_date='${statis_date}'
group by app_id
;

select statis_date,
sum(case when app_id in ('2','4','6') then act_devnum end) as cp_act_devnum,
sum(case when app_id in ('2','4','6') then req_devnum end) as cp_req_devnum,
sum(case when app_id='2' then act_devnum end) as cp_android_act_devnum,
sum(case when app_id='2' then req_devnum end) as cp_android_req_devnum,
sum(case when app_id='4' then act_devnum end) as cp_iphone_act_devnum,
sum(case when app_id='4' then req_devnum end) as cp_iphone_req_devnum,
sum(case when app_id='6' then act_devnum end) as cp_ipad_act_devnum,
sum(case when app_id='6' then req_devnum end) as cp_ipad_req_devnum,
sum(case when app_id in ('3','1') then act_devnum end) as qnc_act_devnum,
sum(case when app_id in ('3','1') then req_devnum end) as qnc_req_devnum,
sum(case when app_id='3' then act_devnum end) as qnc_android_act_devnum,
sum(case when app_id='3' then req_devnum end) as qnc_android_req_devnum,
sum(case when app_id='1' then act_devnum end) as qnc_iphone_act_devnum,
sum(case when app_id='1' then req_devnum end) as qnc_iphone_req_devnum
from bing.tmp_app_dayact_dm
where statis_date='2014-11-19'
group by statis_date
;
