
set mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec;
set mapreduce.output.fileoutputformat.compress=true;
set hive.exec.compress.output=true;
set hive.groupby.skewindata=true;
set names='utf8';

--
insert overwrite table bing.dw_app_device_ds
select dm.app_id, dm.device_id,
min(case when dm.sn=1 then dm.first_time end) as first_day,  
min(case when dm.sn=1 then dm.channel_id end) as first_channel,
min(case when dm.sn=1 then dm.first_version end) as first_version,
min(case when dm.sn=1 then dm.first_userip end) as first_userip,
min(case when dm.sn=1 then dm.first_userid end) as first_userid,
max(case when dm.dsn=1 then dm.first_time end) as last_day,
max(case when dm.dsn=1 then dm.channel_id end) as last_channel,
max(case when dm.dsn=1 then dm.first_version end) as last_version,
max(case when dm.dsn=1 then dm.first_userip end) as last_userip,
max(case when dm.dsn=1 then dm.first_userid end) as last_userid, 
case when dm.app_id in ('2','3') and size(split(dm.device_id,'\\|'))>0 then split(dm.device_id,'\\|')[0] else '' end as dev_imei,
case when dm.app_id in ('2','3') and size(split(dm.device_id,'\\|'))>1 then split(dm.device_id,'\\|')[1] 
when dm.app_id in ('1','4','6') and size(split(dm.device_id,'\\|'))>3 then split(dm.device_id,'\\|')[3]
else '' end as dev_uuid,
case when dm.app_id in ('1','4','6') and size(split(dm.device_id,'\\|'))>0 then split(dm.device_id,'\\|')[0] else '' end as mac_md5,
case when dm.app_id in ('1','4','6') and size(split(dm.device_id,'\\|'))>1 then split(dm.device_id,'\\|')[1] else '' end as mac_idfa,
case when dm.app_id in ('1','4','6') and size(split(dm.device_id,'\\|'))>2 then split(dm.device_id,'\\|')[2] else '' end as mac_idfv,
'' as virtual,       
0 as isvirtual,     
null as uninst_date,
max(isactive) as isactive
from (select d.app_id, d.device_id, 
cast(d.first_time as string) as first_time, d.channel_id, d.first_version, d.first_userip, d.first_userid,
case when d.eff_reqcnt>0 then 1 else 0 end as isactive,
row_number() over(partition by d.app_id, d.device_id order by d.first_time asc) as sn,
row_number() over(partition by d.app_id, d.device_id order by d.first_time desc) as dsn
from bing.dw_app_device_dm d
where d.statis_date between '2013-08-01' and '${statis_date}'
distribute by app_id, device_id
) dm
group by dm.app_id, dm.device_id
distribute by app_id, device_id
;
