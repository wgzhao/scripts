
--$$ .config
--$$ script_file:
--$$ script_name: 菜谱专辑关注日指标
--$$ git: bing/bin/sql/
--$$ command: sqlexec.py
--$$
--$$ source: hive#haodou_recipe_albumfollow_${curdate}.AlbumFollow
--$$ result: hive#bing.rpt_rcp_albumfollow_dm

--更新最近7天
insert overwrite table bing.rpt_rcp_albumfollow_dm
select *
from bing.rpt_rcp_albumfollow_dm
where statis_date < '${preday7_date}'
;

insert into table bing.rpt_rcp_albumfollow_dm
select
to_date(af.createtime) as statis_date,
count(af.AlbumFollowId) as follow_num,
count(distinct af.userid) as user_num,
count(distinct af.AlbumId) as album_num
from haodou_recipe_albumfollow_${curdate}.AlbumFollow af
where af.createtime between '${preday7_date} 00:00:00' and '${statis_date} 23:59:59'
group by to_date(af.createtime)
;
