
set mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec;
set mapreduce.output.fileoutputformat.compress=true;
set hive.exec.compress.output=true;
set names='utf8';

--
insert overwrite table bing.dw_app_device_ds
select
nvl(ds.app_id,ns.app_id) as app_id,
nvl(ds.device_id,ns.device_id) as device_id,
case when ns.device_id is null then ds.first_day     when ds.device_id is null then ns.first_day     when ds.first_day<=ns.first_day then ds.first_day     else ns.first_day     end as first_day    ,
case when ns.device_id is null then ds.first_channel when ds.device_id is null then ns.first_channel when ds.first_day<=ns.first_day then ds.first_channel else ns.first_channel end as first_channel, 
case when ns.device_id is null then ds.first_version when ds.device_id is null then ns.first_version when ds.first_day<=ns.first_day then ds.first_version else ns.first_version end as first_version, 
case when ns.device_id is null then ds.first_userip  when ds.device_id is null then ns.first_userip  when ds.first_day<=ns.first_day then ds.first_userip  else ns.first_userip  end as first_userip , 
case when ns.device_id is null then ds.first_userid  when ds.device_id is null then ns.first_userid  when ds.first_day<=ns.first_day then ds.first_userid  else ns.first_userid  end as first_userid ,
case when ns.device_id is null then ds.last_day      when ds.device_id is null then ns.last_day      when ds.last_day >=ns.last_day  then ds.last_day      else ns.last_day      end as last_day     , 
case when ns.device_id is null then ds.last_channel  when ds.device_id is null then ns.last_channel  when ds.last_day >=ns.last_day  then ds.last_channel  else ns.last_channel  end as last_channel , 
case when ns.device_id is null then ds.last_version  when ds.device_id is null then ns.last_version  when ds.last_day >=ns.last_day  then ds.last_version  else ns.last_version  end as last_version , 
case when ns.device_id is null then ds.last_userip   when ds.device_id is null then ns.last_userip   when ds.last_day >=ns.last_day  then ds.last_userip   else ns.last_userip   end as last_userip  , 
case when ns.device_id is null then ds.last_userid   when ds.device_id is null then ns.last_userid   when ds.last_day >=ns.last_day  then ds.last_userid   else ns.last_userid   end as last_userid  ,
case when nvl(ds.app_id,ns.app_id) in ('2','3') and size(split(nvl(ds.device_id,ns.device_id),'\\|'))>0 then split(nvl(ds.device_id,ns.device_id),'\\|')[0] else '' end as dev_imei,
case when nvl(ds.app_id,ns.app_id) in ('2','3') and size(split(nvl(ds.device_id,ns.device_id),'\\|'))>1 then split(nvl(ds.device_id,ns.device_id),'\\|')[1] 
when nvl(ds.app_id,ns.app_id) in ('1','4','6') and size(split(nvl(ds.device_id,ns.device_id),'\\|'))>3 then split(nvl(ds.device_id,ns.device_id),'\\|')[3]
else '' end as dev_uuid,
case when nvl(ds.app_id,ns.app_id) in ('1','4','6') and size(split(nvl(ds.device_id,ns.device_id),'\\|'))>0 then split(nvl(ds.device_id,ns.device_id),'\\|')[0] else '' end as mac_md5,
case when nvl(ds.app_id,ns.app_id) in ('1','4','6') and size(split(nvl(ds.device_id,ns.device_id),'\\|'))>1 then split(nvl(ds.device_id,ns.device_id),'\\|')[1] else '' end as mac_idfa,
case when nvl(ds.app_id,ns.app_id) in ('1','4','6') and size(split(nvl(ds.device_id,ns.device_id),'\\|'))>2 then split(nvl(ds.device_id,ns.device_id),'\\|')[2] else '' end as mac_idfv,
'' as virtual,
0 as isvirtual,
null as uninst_date,
case when coalesce(ds.isactive,0)=1 or coalesce(ns.isactive,0)=1 then 1 else 0 end as isactive
from
(select app_id, device_id,
cast(first_day as string) as first_day, first_channel, first_version, first_userip, first_userid,
cast(last_day as string) as last_day, last_channel, last_version, last_userip, last_userid, isactive
from bing.dw_app_device_ds
distribute by app_id, device_id
) ds full join
(select dm.app_id, dm.device_id,
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
max(dm.isactive) as isactive
from (select d.app_id, d.device_id, 
cast(d.first_time as string) as first_time, d.channel_id, d.first_version, d.first_userip, d.first_userid, 
case when d.eff_reqcnt>0 then 1 else 0 end as isactive,
row_number() over(partition by app_id, device_id order by d.first_time asc) as sn,
row_number() over(partition by app_id, device_id order by d.first_time desc) as dsn
from bing.dw_app_device_dm d
where d.statis_date='${statis_date}'
distribute by d.app_id, d.device_id
) dm
group by dm.app_id, dm.device_id
) ns on (ds.app_id=ns.app_id and ds.device_id=ns.device_id)
;
