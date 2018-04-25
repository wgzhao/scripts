-- 视频菜谱日统计表

-- 创建临时表
create table if not exists bing.tmp_rpt_app_video_dm (
    app_id string  comment '应用ID',
    view_num int  comment '浏览数',
    dev_num int  comment '日活数',
    comment_num int  comment '评论数',
    fav_num int  comment '点赞数',
    collect_num int  comment '收藏数',
    shared_num int  comment '分享数',
    old_dev_num int  comment '昨日日活数'
)  comment '视频菜谱临时统计表--每日'
partitioned by (
    statis_date string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';


-- 获得视频菜谱当天的浏览数和日活数
insert overwrite table bing.tmp_rpt_app_video_dm partition(statis_date = '${statis_date}') 
select '*',sum(case when statis_date = '${statis_date}' then call_cnt end), 
max(case when statis_date = '${statis_date}' then dev_num end ),
0,0,0,0,
max(case when statis_date = '${preday_date}' then dev_num end)
from bing.rpt_app_functiondetail_dm 
where function_id in ('info.getvideoindexdata','info.getvideolist','info.getvideourl','mhaodou.novice') 
and version_id ='*' 
and statis_date in ('${statis_date}','${preday_date}');

-- 通过m.haodou.com访问视频菜谱的浏览数
insert into table bing.tmp_rpt_app_video_dm partition(statis_date = '${statis_date}') 
select '*',count(*),0,0,0,0,0,0 from 
(select regexp_extract(path,'/recipe/([0-9]*)',1) as p from  logs.m_haodou_com 
where logdate='${statis_date}'
and regexp_extract(path,'/recipe/([0-9]*)',1) != '' 
) m left semi join 
(select recipeid from 
haodou_recipe_${curdate}.video
) v on  (m.p  = v.recipeid );

-- 访问新手课堂的浏览数 
-- http://m.haodou.com/app/recipe/act/novice.php
insert into table bing.tmp_rpt_app_video_dm partition(statis_date = '${statis_date}') 
select '*',count(`path`),0,0,0,0,0,0 from 
logs.m_haodou_com 
where logdate='${statis_date}'
and `path` = '/app/recipe/act/novice.php';

-- 获得新手课堂的评论数
insert into table bing.tmp_rpt_app_video_dm partition(statis_date  = '${statis_date}')
    select '*',0,0,count(commentid) as comment_num,0,0,0,0.0
    from 
    (select itemid , commentid
    from haodou_comment_${curdate}.Comment cm
    where cm.createtime between '${statis_date} 00:00:00' and '${statis_date} 23:59:59'
    and cm.status=1 and cm.type=0 
    ) c  left semi join 
    bing.ods_app_novice  n
 on (n.id = c.itemid);
-- 获得视频菜谱当天的评论数
insert into table bing.tmp_rpt_app_video_dm partition(statis_date = '${statis_date}') 
select '*',0,0,count(commentid) as comment_num,0,0,0,0.0
from 
(select itemid , commentid
from haodou_comment_${curdate}.Comment cm
where cm.createtime between '${statis_date} 00:00:00' and '${statis_date} 23:59:59'
and cm.status=1 and cm.type=0 
) c  left semi join (
select recipeid
from haodou_recipe_${curdate}.video
) v on (v.recipeid = c.itemid);

-- 获得视频菜谱的当天点赞(喜欢)数
insert into table bing.tmp_rpt_app_video_dm partition(statis_date = '${statis_date}') 
select '*',0,0,0,count(favoriteid) as favorite_num,0,0,0
from 
(select favoriteid,itemid as recipeid
    from haodou_favorite_${curdate}.favorite
where createtime between '${statis_date} 00:00:00' and '${statis_date} 23:59:59' and itemtype = 1 )
f left semi join
(
select recipeid
from haodou_recipe_${curdate}.video
) v on (f.recipeid = v.recipeid);
    
-- 获得视频菜谱当天的收藏数
insert into table bing.tmp_rpt_app_video_dm partition(statis_date = '${statis_date}') 
select '*',0,0,0,0,count(albumrecipeid),0,0
from 
(select recipeid, albumrecipeid
from haodou_recipe_albumrecipe_${curdate}.albumrecipe
where createtime between '${statis_date} 00:00:00' and '${statis_date} 23:59:59'
) ar left semi join
haodou_recipe_${curdate}.video v 
on (v.recipeid= ar.recipeid);

    
-- 统计
insert overwrite table bing.rpt_app_video_dm partition (statis_date='${statis_date}')
select '*',sum(view_num),sum(dev_num),sum(comment_num),sum(fav_num),sum(collect_num),sum(shared_num), case when sum(old_dev_num) = 0 then 100 else 100.0 * (sum(dev_num) - sum(old_dev_num)) / sum(old_dev_num)  end
from bing.tmp_rpt_app_video_dm
where statis_date = '${statis_date}';
