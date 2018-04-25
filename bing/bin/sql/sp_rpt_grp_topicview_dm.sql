
set hive.auto.convert.join=false;

--广场内容浏览情况（分小组）
--应用端
insert overwrite table bing.rpt_grp_topicview_dm partition (statis_date='${statis_date}')
select 'app' as source_type, 
coalesce(tt.cateid,'*') as group_id,
count(*) as visit_cnt,
count(distinct vv.userip) as ip_num
from
(select parse_url(concat('http://',`host`,`path`),'QUERY','id') as topicid,
remote_addr as userip
from logs.m_haodou_com
where logdate='${statis_date}'
and method='GET' and status in (200,301)
and `path` like '/app/recipe/topic/view.php%'
and (http_x_requested_with='com.haodou.recipe' or http_user_agent like '%HAODOU_RECIPE_CLIENT%')
) vv left outer join
haodou_center_${curdate}.GroupTopic tt on (vv.topicid=tt.topicid)
group by tt.cateid with rollup
;
