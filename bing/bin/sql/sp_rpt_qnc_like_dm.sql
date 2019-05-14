insert overwrite table bing.rpt_qnc_like_dm partition(ptdate='${statisdate}')
select * 
from(
select 1 type, count(1) count, count(distinct userid) user_count, count(distinct itemid) item_count from qnc_qunachi_favorite_${curdate}.favorite where to_date(createtime) = '${statis_date} and itemtype = 4'
union all
select 2 type,count(1) count, count(distinct userid) user_count, count(distinct topicid) item_count from qnc_haodou_pai_${curdate}.shoptopiclike where to_date(createtime) = '${statis_date}'
union all
select 3 type,count(1) count, count(distinct userid) user_count, count(distinct shareid) item_count from qnc_haodou_pai_${curdate}.pailike where to_date(createtime) = '${statis_date}') t;
