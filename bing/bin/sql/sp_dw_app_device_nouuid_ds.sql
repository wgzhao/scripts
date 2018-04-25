
set mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec;
set mapreduce.output.fileoutputformat.compress=true;
set hive.exec.compress.output=true;
set hive.groupby.skewindata=true;
set names='utf8';

insert overwrite table bing.dw_app_device_nouuid_ds
select '2' as app_id, device_id,
min(case when sn=1 then first_day end),  
min(case when sn=1 then first_channel end),
min(case when sn=1 then first_version end),
min(case when sn=1 then first_userip end),
min(case when sn=1 then first_userid end),
max(case when dsn=1 then last_day end),
max(case when dsn=1 then last_channel end),
max(case when dsn=1 then last_version end),
max(case when dsn=1 then last_userip end),
max(case when dsn=1 then last_userid end), 
max(isactive)
from
(select 
dev_imei as device_id,
first_day,
first_channel,
first_version,
first_userip,
first_userid,
last_day,
last_channel,
last_version,
last_userip,
last_userid,
isactive,
row_number() over(partition by dev_imei order by first_day asc) as sn,
row_number() over(partition by dev_imei order by last_day desc) as dsn
from bing.dw_app_device_ds
where app_id='2'
) dd
group by device_id
;
