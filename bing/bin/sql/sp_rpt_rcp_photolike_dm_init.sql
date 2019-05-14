
--成果照喜欢日指标（初始化）

insert overwrite table bing.rpt_rcp_photolike_dm
select
to_date(d.createtime) as statis_date,
count(d.id) as like_num,
count(distinct d.userid) as usr_num,
count(distinct d.itemid) as photo_num
from haodou_digg_${curdate}.digg d
where d.itemtype=1
group by to_date(d.createtime)
;
