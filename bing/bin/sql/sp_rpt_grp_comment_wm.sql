
--广场回复
insert overwrite table bing.rpt_grp_comment_wm partition (statis_week='${statisweek_firstday}')
select 
coalesce(ut.cateid,'*') as group_id, 
count(uc.commentid) as comment_num,
count(distinct uc.userid) as user_num
from 
(select commentid, userid, itemid as topicid
from haodou_comment_${curdate}.Comment
where createtime between '${statisweek_firstday} 00:00:00' and '${statisweek_lastday} 23:59:59'
and type = '6' and status = '1'
) uc inner join
haodou_center_${curdate}.GroupTopic ut on (uc.topicid=ut.topicid)
group by ut.cateid with rollup
;
