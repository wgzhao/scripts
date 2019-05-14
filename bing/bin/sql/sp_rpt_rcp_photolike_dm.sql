
--成果照喜欢日指标

insert overwrite table bing.rpt_rcp_photolike_dm
select *
from bing.rpt_rcp_photolike_dm
where statis_date != '${statis_date}'
;

insert into table bing.rpt_rcp_photolike_dm
select
'${statis_date}',
count(d.id) as like_num,
count(distinct d.userid) as usr_num,
count(distinct d.itemid) as photo_num
from haodou_digg_${curdate}.Digg d
where d.createtime between '${statis_date} 00:00:00' and '${statis_date} 23:59:59'
and d.itemtype=1
;
