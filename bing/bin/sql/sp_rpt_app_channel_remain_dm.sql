
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;

--直接Group By动态分区写入表存在bug，写入分区与预期不一致，故先按非动态分区方式写入，再从直接Select导入目标分区表
--渠道不分版本留存
insert overwrite table bing.rpt_app_channel_remain_dm_np partition (exec_date='${statis_date}')
select
app_id, channel_id, '*' as version_id, count(distinct device_id) as new_devnum,
count(distinct case when remain_day=1 then device_id end) as remain_devnum1,
count(distinct case when remain_day=2 then device_id end) as remain_devnum2,
count(distinct case when remain_day=3 then device_id end) as remain_devnum3,
count(distinct case when remain_day=4 then device_id end) as remain_devnum4,
count(distinct case when remain_day=5 then device_id end) as remain_devnum5,
count(distinct case when remain_day=6 then device_id end) as remain_devnum6,
count(distinct case when remain_day=7 then device_id end) as remain_devnum7,
count(distinct case when remain_day=14 then device_id end) as remain_devnum14,
count(distinct case when remain_day=30 then device_id end) as remain_devnum30,
statis_date
from (
select /*+ mapjoin(ds)*/
ds.statis_date, ds.app_id, ds.device_id, ds.channel_id, ds.version_id, dm.active_date, 
datediff(dm.active_date,ds.statis_date) as remain_day
from
(select app_id, first_channel as channel_id, first_version as version_id, device_id, to_date(first_day) as statis_date
from bing.dw_app_device_ds
where first_day between '${preday30_date} 00:00:00' and '${preday_date} 23:59:59'
and isactive=1
) ds
inner join
(select app_id, device_id, to_date(statis_date) as active_date
from bing.dw_app_device_dm
where statis_date between '${preday30_date}' and '${statis_date}'
and eff_reqcnt>0
) dm on (ds.app_id=dm.app_id and ds.device_id=dm.device_id)
) t
group by statis_date, app_id, channel_id
;

--渠道分版本留存
insert into table bing.rpt_app_channel_remain_dm_np partition (exec_date='${statis_date}')
select
app_id, channel_id, version_id, count(distinct device_id) as new_devnum,
count(distinct case when remain_day=1 then device_id end) as remain_devnum1,
count(distinct case when remain_day=2 then device_id end) as remain_devnum2,
count(distinct case when remain_day=3 then device_id end) as remain_devnum3,
count(distinct case when remain_day=4 then device_id end) as remain_devnum4,
count(distinct case when remain_day=5 then device_id end) as remain_devnum5,
count(distinct case when remain_day=6 then device_id end) as remain_devnum6,
count(distinct case when remain_day=7 then device_id end) as remain_devnum7,
count(distinct case when remain_day=14 then device_id end) as remain_devnum14,
count(distinct case when remain_day=30 then device_id end) as remain_devnum30,
statis_date
from (
select /*+ mapjoin(ds)*/
ds.statis_date, ds.app_id, ds.device_id, ds.channel_id, ds.version_id, dm.active_date, 
datediff(dm.active_date,ds.statis_date) as remain_day
from
(select app_id, first_channel as channel_id, first_version as version_id, device_id, to_date(first_day) as statis_date
from bing.dw_app_device_ds
where first_day between '${preday30_date} 00:00:00' and '${preday_date} 23:59:59'
and isactive=1
) ds
inner join
(select app_id, device_id, to_date(statis_date) as active_date
from bing.dw_app_device_dm
where statis_date between '${preday30_date}' and '${statis_date}'
and eff_reqcnt>0
) dm on (ds.app_id=dm.app_id and ds.device_id=dm.device_id)
) t
group by statis_date, app_id, channel_id, version_id
;

--动态分区写入结果表
insert overwrite table bing.rpt_app_channel_remain_dm partition (statis_date)
select
app_id         ,
channel_id     ,
version_id     ,
new_devnum     ,
remain_devnum1 ,
remain_devnum2 ,
remain_devnum3 ,
remain_devnum4 ,
remain_devnum5 ,
remain_devnum6 ,
remain_devnum7 ,
remain_devnum14,
remain_devnum30,
statis_date    
from bing.rpt_app_channel_remain_dm_np
where exec_date='${statis_date}'
;
