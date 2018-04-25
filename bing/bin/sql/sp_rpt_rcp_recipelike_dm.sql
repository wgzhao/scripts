
--菜谱喜欢日指标

--更新最近7天
insert overwrite table bing.rpt_rcp_recipelike_dm
select *
from bing.rpt_rcp_recipelike_dm
where statis_date < '${preday7_date}'
;

insert into table bing.rpt_rcp_recipelike_dm
select
to_date(fv.createtime) as statis_date,
count(fv.FavoriteId) as like_num,
count(distinct fv.userid) as user_num,
count(distinct fv.itemid) as recipe_num
from haodou_favorite_${curdate}.Favorite fv
where fv.createtime between '${preday7_date} 00:00:00' and '${statis_date} 23:59:59'
and fv.itemtype=1
group by to_date(fv.createtime)
;
