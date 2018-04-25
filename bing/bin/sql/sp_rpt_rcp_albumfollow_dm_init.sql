
--菜谱专辑关注日指标（初始化）

--更新最近7天
insert overwrite table bing.rpt_rcp_albumfollow_dm
select
to_date(af.createtime) as statis_date,
count(af.AlbumFollowId) as follow_num,
count(distinct af.userid) as user_num,
count(distinct af.AlbumId) as album_num
from haodou_recipe_albumfollow_${curdate}.AlbumFollow af
group by to_date(af.createtime)
;
