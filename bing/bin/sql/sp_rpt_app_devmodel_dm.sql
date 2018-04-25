
--好豆菜谱机型分布报表
--已知问题：在重跑时，可能存在统计开始时间之前的新增机器，在统计结束时间之后发生活跃，导致其最后活跃日期被更新而未被统计在内。
insert overwrite table bing.rpt_app_devmodel_dm partition (statis_date='${statis_date}')
select app_id, dev_brand, concat(dev_brand, ' ', dev_model) as model,
count(device_id) as devnum,
count(case when first_date>='${preday30_date}' then device_id end) as newnum
from bing.dw_app_devinfo_ds
where first_date between '${preday30_date}' and '${statis_date}'
or last_date between '${preday30_date}' and '${statis_date}'
group by app_id, dev_brand, dev_model
;
