
insert overwrite table bing.rpt_app_channel_daydur_dm partition (statis_date='${statis_date}')
select app_id, channel_id, version_id,
sum(duration) as total_dur,
count(1) as total_cnt,
count(distinct device_id) as total_devnum,
avg(duration) as avg_dur,
coalesce(sum(case when duration between 4 and 10000 then duration end),0) as eff_totaldur,
count(case when duration between 4 and 10000 then 1 end) as eff_totalcnt,
count(distinct case when duration between 4 and 10000 then device_id end) as eff_devnum,
coalesce(avg(case when duration between 4 and 10000 then duration end),0) as eff_avgdur 
from bing.dw_app_device_duration_dm
where statis_date='${statis_date}' and duration>=0
group by app_id, channel_id, version_id
;
