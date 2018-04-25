
--好豆菜谱渠道IP TOP10报表
--此处目前限定为菜谱版本为5以上（用来减少统计数据条数，版本可随发布情况修改）
insert overwrite table bing.rpt_app_iptop10_ds
select
app_id,
first_channel as channel_id, 
first_userip as userip, 
devnum
from
(select app_id, first_channel, first_userip, count(device_id) as devnum,
row_number() over(partition by first_channel order by count(device_id) desc) as sn
from bing.dw_app_device_ds
where (app_id='2' and first_version>'5') or (app_id='4' and first_version>'v5')
group by app_id, first_channel, first_userip
) t
where sn<=10
;
