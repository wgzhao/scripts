
set names='utf8';

create table if not exists bing.tmp_app_channel_index_dm
(
  app_id          string comment '好豆应用ID',
  channel_id      string comment '渠道ID。格式：渠道编码+版本。',
  version_id      string comment '版本ID',
  new_devnum      int comment '新增设备激活量',
  fake_devnum     int comment '虚假新增设备数',
  remain_basenum  int comment '留存设备基数（昨日新增设备激活量）',
  remain_devnum   int comment '留存设备数',
  remain_rate     float comment '次日留存率',
  act_devnum      int comment '活跃设备数',
  avg_dur         int comment '单次使用时长'
) comment '安卓菜谱渠道监控指标'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

insert overwrite table bing.tmp_app_channel_index_dm partition (statis_date='${statis_date}')
select /*+ mapjoin(ip)*/
'2' as app_id, ds.channel_id, ds.version_id,
count(distinct case when ip.userip is null then ds.device_id end) as new_devnum,
count(distinct case when ip.userip is not null then ds.device_id end) as fake_devnum,
0 as remain_basenum,
0 as remain_devnum,
0 as remain_rate,
0 as act_devnum,
0 as avg_dur
from
(select first_channel as channel_id, first_version as version_id, first_userip as userip, device_id
from bing.dw_app_device_nouuid_ds
where first_day between '${statis_date} 00:00:00' and '${statis_date} 23:59:59'
and app_id='2' and isactive=1
) ds left outer join
(select channel_id, userip
from bing.dw_app_fake_ip
where statis_date='${statis_date}'
and app_id='2' and dev_num>=12
) ip on (ds.channel_id=ip.channel_id and ds.userip=ip.userip)
group by ds.channel_id, ds.version_id
;

insert into table bing.tmp_app_channel_index_dm partition (statis_date='${statis_date}')
select '2' as app_id, nd.channel_id, nd.version_id,
0 as new_devnum,
0 as fake_devnum,
count(distinct nd.device_id) as remain_basenum,
count(distinct dm.device_id) as remain_devnum,
round(100.0*count(distinct dm.device_id)/count(distinct nd.device_id),2) as remain_rate,
0 as act_devnum,
0 as avg_dur
from 
(select /*+ mapjoin(ip)*/
ds.channel_id, ds.version_id, ds.device_id
from
(select first_channel as channel_id, first_version as version_id, first_userip as userip, device_id
from bing.dw_app_device_nouuid_ds
where first_day between '${preday_date} 00:00:00' and '${preday_date} 23:59:59'
and app_id='2' and isactive=1
) ds left outer join
(select app_id, channel_id, userip
from bing.dw_app_fake_ip
where statis_date='${preday_date}'
and dev_num>=12
) ip on (ds.channel_id=ip.channel_id and ds.userip=ip.userip)
where ip.userip is null
) nd
left outer join
(select distinct dev_imei as device_id
from bing.dw_app_device_dm
where statis_date='${statis_date}'
and app_id='2' and eff_reqcnt>0
) dm on (nd.device_id=dm.device_id)
group by nd.channel_id, nd.version_id
;

insert into table bing.tmp_app_channel_index_dm partition (statis_date='${statis_date}')
select '2' as app_id, channel_id, first_version as version_id,
0 as new_devnum,
0 as fake_devnum,
0 as remain_basenum,
0 as remain_devnum,
0 as remain_rate,
count(distinct dev_imei) as act_devnum,
0 as avg_dur
from bing.dw_app_device_dm
where statis_date='${statis_date}'
and app_id='2' and eff_reqcnt>0
group by channel_id, first_version
;

insert into table bing.tmp_app_channel_index_dm partition (statis_date='${statis_date}')
select '2' as app_id, channel_id, version_id,
0 as new_devnum, 
0 as base_devnum,
0 as remain_basenum,
0 as remain_devnum,
0 as remain_rate,
0 as act_devnum,
avg_dur
from bing.rpt_app_channel_daydur_dm
where statis_date='${statis_date}'
and app_id='2'
;

insert overwrite table bing.rpt_app_channel_index_dm partition (statis_date='${statis_date}')
select app_id, channel_id, version_id,
sum(new_devnum),
sum(fake_devnum),
sum(remain_basenum),
sum(remain_devnum),
sum(remain_rate),
sum(act_devnum),
sum(avg_dur)
from bing.tmp_app_channel_index_dm
where statis_date='${statis_date}'
group by app_id, channel_id, version_id
;
