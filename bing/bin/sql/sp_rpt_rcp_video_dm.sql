
--菜谱视频日报

--创建视频日报临时表
create table if not exists bing.tmp_rcp_video_dm (
app_id      string comment '应用id',
dev_num     int comment '活跃设备数',
view_cnt    int comment '浏览次数',
play_cnt    int comment '播放次数',
comment_num int comment '评论数',
digg_num    int comment '点赞数',
fav_num     int comment '收藏数',
share_num   int comment '分享数',
old_dev_num int comment '昨日活跃设备数'
) comment '视频日报临时表'
row format delimited fields terminated by '\t' stored as textfile
;

--取昨天的分应用日活数
insert overwrite table bing.tmp_rcp_video_dm
select app_id, 0, 0, 0, 0, 0, 0, 0, dev_num
from bing.rpt_rcp_video_dm
where statis_date='${preday_date}' and app_id in ('2','4','0')
;

--分应用活跃、浏览、播放指标
insert into table bing.tmp_rcp_video_dm
select app_id, 
count(distinct device_id) as dev_num, 
sum(cnt) as view_cnt,
sum(case when function_id in ('info.getvideourl','video.getvideourl') then cnt else 0 end) as play_cnt,
0 as comment_num,
0 as digg_num,
0 as fav_num,
0 as share_num,
0 as old_dev_num
from bing.dw_rcp_video_log
where statis_date='${statis_date}'
group by app_id
;

--写入结果表
insert overwrite table bing.rpt_rcp_video_dm partition (statis_date='${statis_date}')
select coalesce(t.app_id,'*'),
sum(dev_num    ),
sum(view_cnt   ),
sum(play_cnt   ),
sum(comment_num),
sum(digg_num   ),
sum(fav_num    ),
sum(share_num  ),
sum(old_dev_num)
from bing.tmp_rcp_video_dm t
group by t.app_id with rollup
;
