
insert overwrite table bing.dw_app_device_duration_dm partition (statis_date='${statis_date}')
select app_id, device_id, channel_id, version_id, session_id,
min(request_time) as request_time,
count(1) as request_cnt,
unix_timestamp(max(request_time))-unix_timestamp(min(request_time)) as duration
from bing.dw_app_requestlog_session_dm
where statis_date='${statis_date}'
and app_id in ('1','2','3','4','6')
group by app_id, device_id, channel_id, version_id, session_id
;
