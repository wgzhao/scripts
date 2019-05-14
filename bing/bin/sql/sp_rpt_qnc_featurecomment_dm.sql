insert overwrite table bing.rpt_qnc_featurecomment_dm partition(ptdate='${statisdate}')
select t.replyid, count(1) count, count( distinct t.itemid) item_count, count(distinct t.userid) user_count
from
(select cmt.itemid, cmt.userid, cmt.replyid, fte.type
from
(select itemid, userid, if(replyid!=0,1,0) replyid
from qnc_haodou_comment_${curdate}.comment
where to_date(createtime) = '${statis_date}' and status=1 and type=12) cmt
left outer join
(select id, type
from qnc_haodou_mobile_${curdate}.qncfeature
where status=1 and type!=4) fte
on fte.id = cmt.itemid order by cmt.itemid
union all
select cmt.itemid, cmt.userid, cmt.replyid, fte.type
from
(select itemid, userid, if(replyid!=0,1,0) replyid
from qnc_haodou_comment_${curdate}.comment
where to_date(createtime) = '${statis_date}' and status=1 and type=11) cmt
join
(select itemid, type
from qnc_haodou_mobile_${curdate}.qncfeature
where status=1 and type=4) fte
on fte.itemid = cmt.itemid order by cmt.itemid) t
group by t.replyid;
