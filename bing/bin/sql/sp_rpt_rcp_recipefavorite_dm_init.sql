
--菜谱收藏到专辑日指标（初始化）

insert overwrite table bing.rpt_rcp_recipefavorite_dm
select
to_date(ar.createtime) as statis_date,
count(ar.albumrecipeid) as favorite_num,
count(distinct ar.recipeid) as recipe_num,
count(distinct a.userid) as user_num,
count(distinct ar.albumid) as album_num
from haodou_recipe_albumrecipe_${curdate}.AlbumRecipe ar
left outer join haodou_recipe_${curdate}.Album a on (ar.albumid=a.albumid)
group by to_date(ar.createtime)
;
