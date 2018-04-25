
insert overwrite table bing.rpt_app_index_dm partition (statis_date='${statis_date}')
select app_id, '*' as version_id, '日新增用户数' as index_name, count(device_id) as index_value
from bing.dw_app_device_ds
where first_day between '${statis_date} 00:00:00' and '${statis_date} 23:59:59'
and app_id in ('1','2','3','4','5','6')
group by app_id
;

insert into table bing.rpt_app_index_dm partition (statis_date='${statis_date}')
select app_id, first_version as version_id, '日新增用户数' as index_name, count(device_id) as index_value
from bing.dw_app_device_ds
where first_day between '${statis_date} 00:00:00' and '${statis_date} 23:59:59'
and app_id in ('1','2','3','4','5','6')
group by app_id, first_version
;

insert into table bing.rpt_app_index_dm partition (statis_date='${statis_date}')
select app_id, '*' as version_id, '日活跃用户数' as index_name, count(distinct device_id) as index_value
from bing.dw_app_device_dm
where statis_date = '${statis_date}' and app_id in ('1','2','3','4','5','6')
and eff_reqcnt>0
group by app_id
;

insert into table bing.rpt_app_index_dm partition (statis_date='${statis_date}')
select app_id, first_version as version_id, '日活跃用户数' as index_name, count(distinct device_id) as index_value
from bing.dw_app_device_dm
where statis_date = '${statis_date}' and app_id in ('1','2','3','4','5','6')
and eff_reqcnt>0
group by app_id, first_version
;

insert into table bing.rpt_app_index_dm partition (statis_date='${statis_date}')
select app_id, '*' as version_id, '日请求用户数' as index_name, count(distinct device_id) as index_value
from bing.dw_app_device_dm
where statis_date = '${statis_date}' and app_id in ('1','2','3','4','5','6')
group by app_id
;

insert into table bing.rpt_app_index_dm partition (statis_date='${statis_date}')
select app_id, first_version as version_id, '日请求用户数' as index_name, count(distinct device_id) as index_value
from bing.dw_app_device_dm
where statis_date = '${statis_date}' and app_id in ('1','2','3','4','5','6')
group by app_id, first_version
;

insert into table bing.rpt_app_index_dm partition (statis_date='${statis_date}')
select app_id, '*' as version_id, '日平均单次使用时长' as index_name, int(avg(duration)) as index_value
from bing.dw_app_device_duration_dm
where statis_date='${statis_date}' and app_id in ('1','2','3','4','5','6')
group by app_id
;

insert into table bing.rpt_app_index_dm partition (statis_date='${statis_date}')
select app_id, version_id, '日平均单次使用时长' as index_name, int(avg(duration)) as index_value
from bing.dw_app_device_duration_dm
where statis_date='${statis_date}' and app_id in ('1','2','3','4','5','6')
group by app_id, version_id
;

insert into table bing.rpt_app_index_dm partition (statis_date='${statis_date}')
select app_id, '*' as version_id, '日启动次数' as index_name, count(1) as index_value
from bing.dw_app_device_duration_dm
where statis_date='${statis_date}' and app_id in ('1','2','3','4','5','6')
group by app_id
;

insert into table bing.rpt_app_index_dm partition (statis_date='${statis_date}')
select app_id, version_id, '日启动次数' as index_name, count(1) as index_value
from bing.dw_app_device_duration_dm
where statis_date='${statis_date}' and app_id in ('1','2','3','4','5','6')
group by app_id, version_id
;

insert into table bing.rpt_app_index_dm partition (statis_date='${statis_date}')
select app_id, '*' as version_id, '累计用户' as index_name, count(device_id) as index_value
from bing.dw_app_device_ds
where first_day < '${statis_date} 23:59:59'
and app_id in ('1','2','3','4','5','6')
group by app_id
;

insert into table bing.rpt_app_index_dm partition (statis_date='${statis_date}')
select app_id, first_version as version_id, '累计用户' as index_name, count(device_id) as index_value
from bing.dw_app_device_ds
where first_day < '${statis_date} 23:59:59'
and app_id in ('1','2','3','4','5','6')
group by app_id, first_version
;

insert into table bing.rpt_app_index_dm partition (statis_date='${statis_date}')
select app_id, '*' as version_id, '过去7天活跃用户' as index_name, count(distinct device_id) as index_value
from bing.dw_app_device_dm
where statis_date between '${preday7_date}' and '${statis_date}'
and app_id in ('1','2','3','4','5','6')
and eff_reqcnt>0
group by app_id
;

insert into table bing.rpt_app_index_dm partition (statis_date='${statis_date}')
select app_id, first_version as version_id, '过去7天活跃用户' as index_name, count(distinct device_id) as index_value
from bing.dw_app_device_dm
where statis_date between '${preday7_date}' and '${statis_date}'
and app_id in ('1','2','3','4','5','6')
group by app_id, first_version
;

insert into table bing.rpt_app_index_dm partition (statis_date='${statis_date}')
select app_id, '*' as version_id, '过去30天活跃用户' as index_name, count(distinct device_id) as index_value
from bing.dw_app_device_dm
where statis_date between '${preday30_date}' and '${statis_date}'
and app_id in ('1','2','3','4','5','6')
group by app_id
;

insert into table bing.rpt_app_index_dm partition (statis_date='${statis_date}')
select app_id, first_version as version_id, '过去30天活跃用户' as index_name, count(distinct device_id) as index_value
from bing.dw_app_device_dm
where statis_date between '${preday30_date}' and '${statis_date}'
and app_id in ('1','2','3','4','5','6')
group by app_id, first_version
;
