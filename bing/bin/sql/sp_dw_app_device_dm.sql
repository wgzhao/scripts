
set mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec;
set mapreduce.output.fileoutputformat.compress=true;
set hive.exec.compress.output=true;
set names='utf8';

--抽取渠道虚假活跃IP
insert overwrite table bing.dw_app_fake_ip partition (statis_date='${statis_date}')
select app_id, channel_id, userip, count(distinct device_id) as dev_num, count(1) as req_cnt
from bing.ods_app_requestlog_dm
where statis_date='${statis_date}'
group by app_id, channel_id, userip
having count(distinct device_id)>=12
;

--
insert overwrite table bing.dw_app_device_dm partition (statis_date='${statis_date}')
select app_id, device_id, channel_id,
min(case when sn=1 then request_time end) as first_time,
min(case when sn=1 then version_id end) as first_version,
min(case when sn=1 then userip end) as first_userip,
min(userid) as first_userid,
case when app_id in ('2','3') and size(split(device_id,'\\|'))>0 then split(device_id,'\\|')[0] else '' end as dev_imei,
case when app_id in ('2','3') and size(split(device_id,'\\|'))>1 then split(device_id,'\\|')[1] 
when app_id in ('1','4','6') and size(split(device_id,'\\|'))>3 then split(device_id,'\\|')[3]
else '' end as dev_uuid,
case when app_id in ('1','4','6') and size(split(device_id,'\\|'))>0 then split(device_id,'\\|')[0] else '' end as mac_md5,
case when app_id in ('1','4','6') and size(split(device_id,'\\|'))>1 then split(device_id,'\\|')[1] else '' end as mac_idfa,
case when app_id in ('1','4','6') and size(split(device_id,'\\|'))>2 then split(device_id,'\\|')[2] else '' end as mac_idfv,
count(1) as req_cnt,
sum(eff_req) as eff_reqcnt,
0 as isvirtual,
0 as isfake
from 
(select app_id, regexp_replace(device_id,'\\|\\|$','') as device_id, channel_id, request_time, version_id, userip, userid, uuid,
case when find_in_set(function_id,'ad.getad_imocha,common.gettime,mobiledevice.initandroiddevice,mobiledevice.initiphonedevice,notice.getpullnotice,recipe.getfindrecipe,fix.getimagehostdnslist')>0 then 0 else 1 end as eff_req,
row_number() over(partition by app_id, device_id, channel_id order by request_time asc) as sn
from bing.ods_app_requestlog_dm
where statis_date='${statis_date}'
and app_id in ('1','2','3','4','6') and device_id!=''
) t0
group by app_id, device_id, channel_id
;

--抽取渠道虚假uuid
insert overwrite table bing.dw_app_fake_uuid partition (statis_date='${statis_date}')
select app_id, channel_id, dev_uuid, count(distinct device_id) as dev_num, sum(req_cnt) as req_cnt
from bing.dw_app_device_dm
where statis_date='${statis_date}' 
and app_id='2' and dev_uuid!=''
group by app_id, channel_id, dev_uuid
having count(distinct device_id)>2
;
