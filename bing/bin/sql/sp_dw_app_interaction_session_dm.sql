
set mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec;
set mapreduce.output.fileoutputformat.compress=true;
set hive.exec.compress.output=true;

insert overwrite table bing.dw_app_interaction_session_dm partition (statis_date='${statis_date}')
select /*+ mapjoin(kf)*/
ll.request_time,
ll.device_id,
ll.channel_id,
ll.userip,
ll.app_id,
ll.version_id,
ll.userid,
ll.function_id,
ll.parameter_info,
ll.parameter_desc,
ll.uuid,
ll.session_id,
row_number() over(partition by ll.app_id, ll.device_id, ll.channel_id, ll.session_id order by ll.session_seq asc) as seq 
from bing.dw_app_requestlog_session_dm ll
left semi join 
(select app_id, function_id
from bing.dw_app_function
where value_factor>0
) kf on (ll.app_id=kf.app_id and ll.function_id=kf.function_id)
where ll.statis_date='${statis_date}' and device_id!=''
distribute by ll.app_id, ll.device_id
;
