
--好豆BI周报结果表7-去哪吃探店贴

insert overwrite table bing.rpt_qnc_shoptopic_wm partition (statis_week='${statisweek_firstday}')
select 
coalesce(cityid,'*') as city_id,
count(topicid) as topic_num,
count(distinct userid) as user_num,
count(case when `type`=1 then topicid end) as shop_type_num,
count(case when `type`=2 then topicid end) as snack_type_num,
count(case when `mode`=1 then topicid end) as std_mode_num,
count(case when `mode`=2 then topicid end) as free_mode_num,
count(case when rate>=2  then topicid end) as star_num
from 
(select topicid, userid, coalesce(cityid,0) as cityid, `type`, `mode`, rate
from qnc_haodou_pai_${curdate}.ShopTopic
where updatetime between '${statisweek_firstday} 00:00:00' and '${statisweek_lastday} 23:59:59'
and display=1
) t
group by cityid with rollup
;
