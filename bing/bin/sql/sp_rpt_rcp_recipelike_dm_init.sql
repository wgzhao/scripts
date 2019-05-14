
--菜谱喜欢日指标（初始化）

insert overwrite table bing.rpt_rcp_recipelike_dm
select
to_date(fv.createtime) as statis_date,
count(fv.FavoriteId) as like_num,
count(distinct fv.userid) as user_num,
count(distinct fv.itemid) as recipe_num
from haodou_favorite_${curdate}.Favorite fv
where fv.itemtype=1
group by to_date(fv.createtime)
;
