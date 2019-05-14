
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;

add jar hdfs://hdcluster/udf/bing.jar;
create temporary function getweekday as 'com.haodou.hive.bing.getWeekday';
create temporary function getweekofyear as 'com.haodou.hive.bing.getWeekOfYear';

--直接Group By动态分区写入表存在bug，写入分区与预期不一致，故先按非动态分区方式写入，再从直接Select导入目标分区表
--不分版本留存
insert overwrite table bing.rpt_app_remain_wm_np partition (exec_date='${statis_date}')
select 
t.app_id, '*' as version_id,
count(distinct t.device_id) as new_devnum,
count(distinct case when t.remain_day between  7 and 13 then t.device_id end) as remain_devnum1,
count(distinct case when t.remain_day between 14 and 20 then t.device_id end) as remain_devnum2,
count(distinct case when t.remain_day between 21 and 27 then t.device_id end) as remain_devnum3,
count(distinct case when t.remain_day between 28 and 34 then t.device_id end) as remain_devnum4,
count(distinct case when t.remain_day between 35 and 41 then t.device_id end) as remain_devnum5,
count(distinct case when t.remain_day between 42 and 48 then t.device_id end) as remain_devnum6,
count(distinct case when t.remain_day between 49 and 55 then t.device_id end) as remain_devnum7,
t.statis_monday as statis_week
from (
select /*+ mapjoin(ds)*/
ds.statis_date, ds.statis_monday, ds.app_id, ds.version_id, ds.device_id,
datediff(dm.active_date,ds.statis_monday) as remain_day
from
(select app_id, first_version as version_id, device_id, 
to_date(first_day) as statis_date,
date_sub(to_date(first_day), getweekday(to_date(first_day))-1) as statis_monday
from bing.dw_app_device_ds
where first_day between '${preday60_date} 00:00:00' and '${preday7_date} 23:59:59'
and isactive=1
) ds
inner join
(select app_id, device_id, to_date(statis_date) as active_date
from bing.dw_app_device_dm
where statis_date between '${preday60_date}' and '${statis_date}'
and eff_reqcnt>0
) dm on (ds.app_id=dm.app_id and ds.device_id=dm.device_id and ds.statis_monday>='${preday60_date}')
) t
group by t.statis_monday, t.app_id
;

--分版本留存
insert into table bing.rpt_app_remain_wm_np partition (exec_date='${statis_date}')
select 
t.app_id, t.version_id,
count(distinct t.device_id) as new_devnum,
count(distinct case when t.remain_day between  7 and 13 then t.device_id end) as remain_devnum1,
count(distinct case when t.remain_day between 14 and 20 then t.device_id end) as remain_devnum2,
count(distinct case when t.remain_day between 21 and 27 then t.device_id end) as remain_devnum3,
count(distinct case when t.remain_day between 28 and 34 then t.device_id end) as remain_devnum4,
count(distinct case when t.remain_day between 35 and 41 then t.device_id end) as remain_devnum5,
count(distinct case when t.remain_day between 42 and 48 then t.device_id end) as remain_devnum6,
count(distinct case when t.remain_day between 49 and 55 then t.device_id end) as remain_devnum7,
t.statis_monday as statis_week
from (
select /*+ mapjoin(ds)*/
ds.statis_date, ds.statis_monday, ds.app_id, ds.version_id, ds.device_id,
datediff(dm.active_date,ds.statis_monday) as remain_day
from
(select app_id, first_version as version_id, device_id, 
to_date(first_day) as statis_date,
date_sub(to_date(first_day), getweekday(to_date(first_day))-1) as statis_monday
from bing.dw_app_device_ds
where first_day between '${preday60_date} 00:00:00' and '${preday7_date} 23:59:59'
and isactive=1
) ds
inner join
(select app_id, device_id, to_date(statis_date) as active_date
from bing.dw_app_device_dm
where statis_date between '${preday60_date}' and '${statis_date}'
and eff_reqcnt>0
) dm on (ds.app_id=dm.app_id and ds.device_id=dm.device_id and ds.statis_monday>='${preday60_date}')
) t
group by t.statis_monday, t.app_id, t.version_id
;

--动态分区写入结果表
insert overwrite table bing.rpt_app_remain_wm partition (statis_week)
select 
app_id         ,
version_id     ,
new_devnum     ,
remain_devnum1 ,
remain_devnum2 ,
remain_devnum3 ,
remain_devnum4 ,
remain_devnum5 ,
remain_devnum6 ,
remain_devnum7 ,
statis_week    
from bing.rpt_app_remain_wm_np
where exec_date='${statis_date}'
;
