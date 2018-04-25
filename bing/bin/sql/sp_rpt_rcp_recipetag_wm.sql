
set mapreduce.map.memory.mb=8192;
set mapreduce.reduce.memory.mb=8192;
set hive.auto.convert.join=false;

--好豆菜谱标签构成周报
insert overwrite table bing.rpt_rcp_recipetag_wm partition (statis_week='${statisweek_firstday}')
select 
ct.catenameid as tag_id,
count(ct.cateid) as recipe_num,
sum(rp.viewcount) as recipe_viewnum,
sum(rp.likecount) as recipe_likenum
from
(select cateid, recipeid, catenameid
from haodou_recipe_${curdate}.Cate) ct
inner join
(select recipeid, viewcount, likecount
from haodou_recipe_${curdate}.Recipe
where status=0
) rp on (ct.recipeid=rp.recipeid)
group by ct.catenameid
;
