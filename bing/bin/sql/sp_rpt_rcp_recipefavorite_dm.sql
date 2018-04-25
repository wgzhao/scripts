
--菜谱收藏到专辑日指标

--更新最近7天
insert overwrite table bing.rpt_rcp_recipefavorite_dm
select *
from bing.rpt_rcp_recipefavorite_dm
where statis_date < '${preday7_date}'
;

drop table if exists bing.tmp_rcp_recipefavorite_dm;
create table if not exists bing.tmp_rcp_recipefavorite_dm like haodou_recipe_albumrecipe_${curdate}.AlbumRecipe;

insert overwrite table bing.tmp_rcp_recipefavorite_dm
select *
from haodou_recipe_albumrecipe_${curdate}.AlbumRecipe
where createtime between '${preday7_date} 00:00:00' and '${statis_date} 23:59:59'
;

insert into table bing.rpt_rcp_recipefavorite_dm
select /*+ mapjoin(ar)*/
to_date(ar.createtime),
count(ar.albumrecipeid) as favorite_num,
count(distinct ar.recipeid) as recipe_num,
count(distinct a.userid) as user_num,
count(distinct ar.albumid) as album_num
from bing.tmp_rcp_recipefavorite_dm ar
inner join haodou_recipe_${curdate}.Album a on (ar.albumid=a.albumid)
group by to_date(ar.createtime)
;
