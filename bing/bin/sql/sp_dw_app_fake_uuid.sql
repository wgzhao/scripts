
set names='utf8';

--抽取渠道虚假uuid
insert overwrite table bing.dw_app_fake_uuid partition (statis_date='${statis_date}')
select app_id, channel_id, dev_uuid, count(distinct device_id) as dev_num, sum(req_cnt) as req_cnt
from bing.dw_app_device_dm
where statis_date='${statis_date}' 
and app_id='2' and dev_uuid!=''
group by app_id, channel_id, dev_uuid
having count(distinct device_id)>2
;
