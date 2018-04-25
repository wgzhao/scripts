insert overwrite table bing.rpt_qnc_comment_dm partition(ptdate='${statisdate}')
select vv1.*, vv2.item_count
from(
select if(isnull(t1.type),t2.type,t1.type) type, 
t1.count, t1.reply_count, t1.comment_count, t2.comment_user_count, t2.reply_user_count
from(
select type,
sum(if(commentid!=0,1,0)) count,
sum(if(replyid!=0,1,0)) reply_count,
sum(if(replyid=0,1,0)) comment_count
from qnc_haodou_comment_${curdate}.comment
where to_date(createtime) = '${statis_date}'
and status = 1
group by type) t1
full outer join(
select if(t21.type is null, t22.type, t21.type) type, 
if(isnull(t21.comment_user_count), 0, t21.comment_user_count) comment_user_count,
if(isnull(t22.reply_user_count), 0, t22.reply_user_count) reply_user_count 
from(
select type, count(distinct userid) comment_user_count
from qnc_haodou_comment_${curdate}.comment
where to_date(createtime) = '${statis_date}'
and status = 1 and replyid = 0
group by type) t21
full outer join(
select type, count(distinct userid) reply_user_count
from qnc_haodou_comment_${curdate}.comment
where to_date(createtime) = '${statis_date}'
and status = 1 and replyid != 0
group by type) t22
on t21.type=t22.type
) t2 
on t1.type=t2.type) vv1
left join 
(select type, count(distinct itemid) item_count
from qnc_haodou_comment_${curdate}.comment
where to_date(createtime) = '${statis_date}'
and status = 1 group by type) vv2
on vv1.type = vv2.type;
