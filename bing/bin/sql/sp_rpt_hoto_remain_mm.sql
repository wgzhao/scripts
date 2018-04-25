
set hive.auto.convert.join=false;

insert overwrite table bing.rpt_hoto_remain_mm partition (statis_date='${firstday_date}')
select
coalesce(sum(newnum),0),
coalesce(sum(remain_num),0),
case when sum(newnum)>0 then 100.0*sum(remain_num)/sum(newnum) else 0 end,
coalesce(sum(case when app_id='2' then newnum end),0),
coalesce(sum(case when app_id='2' then remain_num end),0),
case when sum(case when app_id='2' then newnum end)>0 then 100.0*sum(case when app_id='2' then remain_num end)/sum(case when app_id='2' then newnum end) else 0 end,
coalesce(sum(case when app_id='4' then newnum end),0),
coalesce(sum(case when app_id='4' then remain_num end),0),
case when sum(case when app_id='4' then newnum end)>0 then 100.0*sum(case when app_id='4' then remain_num end)/sum(case when app_id='4' then newnum end) else 0 end,
coalesce(sum(case when app_id='6' then newnum end),0),
coalesce(sum(case when app_id='6' then remain_num end),0),
case when sum(case when app_id='6' then newnum end)>0 then 100.0*sum(case when app_id='6' then remain_num end)/sum(case when app_id='6' then newnum end) else 0 end
from 
(select /*+ mapjoin(ds,dm)*/
ds.app_id,
count(ds.device_id) as newnum,
count(dm.device_id) as remain_num
from 
(select app_id, device_id,
case when instr(first_channel,'_')>0 then substr(first_channel,1,instr(first_channel,'_')) else coalesce(first_channel,'') end as channel_id
from bing.dw_app_device_ds
where first_day between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
and app_id in ('2','4','6') and isactive=1
) ds
left outer join
(select distinct app_id, device_id
from bing.dw_app_device_dm
where statis_date between '${nextday_date}' and '${nextday30_date}'
and app_id in ('2','4','6') and eff_reqcnt>0
) dm on (ds.app_id=dm.app_id and ds.device_id=dm.device_id)
where (ds.app_id='2' and find_in_set(ds.channel_id,'360_,xiaomi_,wandoujia_,baidu_,91_,qq_,hiapk_,anzhi_,default_')>0)
or (ds.app_id='4' and find_in_set(ds.channel_id,'appstore,91,appstore_,apptore')>0)
or (ds.app_id='6' and ds.channel_id='appstore_')
group by ds.app_id
) t
;
