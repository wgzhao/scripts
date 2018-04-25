
insert overwrite table bing.rpt_app_daydur_dist_dm partition (statis_date='${statis_date}')
select app_id, '*' as version_id, duration as duration_id,
sum(duration) as total_dur,
count(1) as total_cnt,
count(distinct device_id) as total_devnum
from bing.dw_app_device_duration_dm
where statis_date='${statis_date}'
group by app_id, duration
;

insert into table bing.rpt_app_daydur_dist_dm partition (statis_date='${statis_date}')
select app_id, version_id, duration as duration_id,
sum(duration) as total_dur,
count(1) as total_cnt,
count(distinct device_id) as total_devnum
from bing.dw_app_device_duration_dm
where statis_date='${statis_date}'
group by app_id, version_id, duration
;
