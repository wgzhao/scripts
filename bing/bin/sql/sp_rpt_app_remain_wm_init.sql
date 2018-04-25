
set hive.auto.convert.join=false;

add jar hdfs://hdcluster/udf/bing.jar;
create temporary function getweekday as 'com.haodou.hive.bing.getWeekday';

--不分版本留存
insert overwrite table bing.rpt_app_remain_wm partition (statis_week='${statisweek_firstday}')
select 
t.app_id, '*' as version_id,
count(distinct t.device_id) as new_devnum,
count(distinct case when t.remain_day between  7 and 13 then t.device_id end) as remain_devnum1,
count(distinct case when t.remain_day between 14 and 20 then t.device_id end) as remain_devnum2,
count(distinct case when t.remain_day between 21 and 27 then t.device_id end) as remain_devnum3,
count(distinct case when t.remain_day between 28 and 34 then t.device_id end) as remain_devnum4,
count(distinct case when t.remain_day between 35 and 41 then t.device_id end) as remain_devnum5,
count(distinct case when t.remain_day between 42 and 48 then t.device_id end) as remain_devnum6,
count(distinct case when t.remain_day between 49 and 55 then t.device_id end) as remain_devnum7
from (
select /*+ mapjoin(ds)*/
ds.app_id, ds.version_id, ds.device_id,
datediff(dm.active_date,to_date('${statisweek_firstday}')) as remain_day
from
(select app_id, first_version as version_id, device_id
from bing.dw_app_device_ds
where first_day between '${statisweek_firstday} 00:00:00' and '${statisweek_lastday} 23:59:59'
and isactive=1
) ds
inner join
(select app_id, device_id, to_date(statis_date) as active_date
from bing.dw_app_device_dm
where statis_date between '${statisweek_firstday}' and '${nextday60_date}'
and eff_reqcnt>0
) dm on (ds.app_id=dm.app_id and ds.device_id=dm.device_id)
) t
group by t.app_id
;

--分版本留存
insert into table bing.rpt_app_remain_wm partition (statis_week='${statisweek_firstday}')
select 
t.app_id, t.version_id,
count(distinct t.device_id) as new_devnum,
count(distinct case when t.remain_day between  7 and 13 then t.device_id end) as remain_devnum1,
count(distinct case when t.remain_day between 14 and 20 then t.device_id end) as remain_devnum2,
count(distinct case when t.remain_day between 21 and 27 then t.device_id end) as remain_devnum3,
count(distinct case when t.remain_day between 28 and 34 then t.device_id end) as remain_devnum4,
count(distinct case when t.remain_day between 35 and 41 then t.device_id end) as remain_devnum5,
count(distinct case when t.remain_day between 42 and 48 then t.device_id end) as remain_devnum6,
count(distinct case when t.remain_day between 49 and 55 then t.device_id end) as remain_devnum7
from (
select /*+ mapjoin(ds)*/
ds.app_id, ds.version_id, ds.device_id,
datediff(dm.active_date,to_date('${statisweek_firstday}')) as remain_day
from
(select app_id, first_version as version_id, device_id
from bing.dw_app_device_ds
where first_day between '${statisweek_firstday} 00:00:00' and '${statisweek_lastday} 23:59:59'
and isactive=1
) ds
inner join
(select app_id, device_id, to_date(statis_date) as active_date
from bing.dw_app_device_dm
where statis_date between '${statisweek_firstday}' and '${nextday60_date}'
and eff_reqcnt>0
) dm on (ds.app_id=dm.app_id and ds.device_id=dm.device_id)
) t
group by t.app_id, t.version_id
;
