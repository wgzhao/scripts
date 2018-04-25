
set hive.auto.convert.join=false;

--不分版本留存
insert overwrite table bing.rpt_app_remain_dm partition (statis_date='${statis_date}')
select 
app_id, '*' as version_id, count(distinct device_id) as new_devnum,
count(distinct case when remain_day=1 then device_id end) as remain_devnum1,
count(distinct case when remain_day=2 then device_id end) as remain_devnum2,
count(distinct case when remain_day=3 then device_id end) as remain_devnum3,
count(distinct case when remain_day=4 then device_id end) as remain_devnum4,
count(distinct case when remain_day=5 then device_id end) as remain_devnum5,
count(distinct case when remain_day=6 then device_id end) as remain_devnum6,
count(distinct case when remain_day=7 then device_id end) as remain_devnum7,
count(distinct case when remain_day=14 then device_id end) as remain_devnum14,
count(distinct case when remain_day=30 then device_id end) as remain_devnum30
from (
select /*+ mapjoin(ds)*/
ds.app_id, ds.device_id, ds.version_id, dm.active_date, 
datediff(dm.active_date,to_date('${statis_date}')) as remain_day
from
(select app_id, first_version as version_id, device_id
from bing.dw_app_device_ds
where first_day between '${statis_date} 00:00:00' and '${statis_date} 23:59:59'
) ds
inner join
(select app_id, device_id, to_date(statis_date) as active_date
from bing.dw_app_device_dm
where statis_date between '${statis_date}' and '${nextday30_date}'
and eff_reqcnt>0
) dm on (ds.app_id=dm.app_id and ds.device_id=dm.device_id)
) t
group by app_id
;

--分版本留存
insert into table bing.rpt_app_remain_dm partition (statis_date='${statis_date}')
select 
app_id, version_id, count(distinct device_id) as new_devnum,
count(distinct case when remain_day=1 then device_id end) as remain_devnum1,
count(distinct case when remain_day=2 then device_id end) as remain_devnum2,
count(distinct case when remain_day=3 then device_id end) as remain_devnum3,
count(distinct case when remain_day=4 then device_id end) as remain_devnum4,
count(distinct case when remain_day=5 then device_id end) as remain_devnum5,
count(distinct case when remain_day=6 then device_id end) as remain_devnum6,
count(distinct case when remain_day=7 then device_id end) as remain_devnum7,
count(distinct case when remain_day=14 then device_id end) as remain_devnum14,
count(distinct case when remain_day=30 then device_id end) as remain_devnum30
from (
select /*+ mapjoin(ds)*/
ds.app_id, ds.device_id, ds.version_id, dm.active_date, 
datediff(dm.active_date,to_date('${statis_date}')) as remain_day
from
(select app_id, first_version as version_id, device_id
from bing.dw_app_device_ds
where first_day between '${statis_date} 00:00:00' and '${statis_date} 23:59:59'
) ds
inner join
(select app_id, device_id, to_date(statis_date) as active_date
from bing.dw_app_device_dm
where statis_date between '${statis_date}' and '${nextday30_date}'
and eff_reqcnt>0
) dm on (ds.app_id=dm.app_id and ds.device_id=dm.device_id)
) t
group by app_id, version_id
;
