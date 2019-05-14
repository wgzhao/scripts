
--广场发帖
insert overwrite table bing.rpt_grp_topicpublish_wm partition (statis_week='${statisweek_firstday}')
select 
coalesce(cateid,'*') as group_id, 
count(1) as topic_num,
count(distinct userid) as user_num
from haodou_center_${curdate}.GroupTopic
where createtime between '${statisweek_firstday} 00:00:00' and '${statisweek_lastday} 23:59:59'
and status='1'
group by cateid with rollup
;
