
--菜谱视频访问日志
--注意：结果表被定义为lzo格式存储，故需要指定insert压缩格式。默认text。已在sqlexec.rc中指定

--应用端视频相关请求
--'video.index' 视频首页
--'video.searchvideo' 视频搜索
--'video.getrecipevideocatelist' 视频分类列表
--'video.gethotranklist' 视频排行榜
--'info.getinfo' 查看视频菜谱/新手课堂（与标准菜谱重复，需要通过菜谱id区别）
--'info.getvideourl' 播放视频菜谱/新手课堂
--'video.getvideoinfo' 查看趣味生活
--'video.getvideourl' 播放趣味生活

--应用端视频日志1
insert overwrite table bing.dw_rcp_video_log partition (statis_date='${statis_date}')
select appid, uuid, function_id, 
case when function_id='info.getvideourl' then coalesce(get_json_object(parameter_desc,'$.rid'),'')
when function_id='video.getvideoinfo' then coalesce(get_json_object(parameter_desc,'$.vid'),'')
when function_id='video.getvideourl' then coalesce(get_json_object(parameter_desc,'$.vid'),'')
else '' end as itemid, 1 as cnt
from logs.log_php_app_log l
where logdate='${statis_date}'
and function_id in ('video.index','video.searchvideo','video.getrecipevideocatelist','video.gethotranklist',
'info.getvideourl','video.getvideoinfo','video.getvideourl')
and appid in ('2','4')
;

--应用端视频日志2
--抽取查看视频菜谱/新手课堂部分
insert into table bing.dw_rcp_video_log partition (statis_date='${statis_date}')
select appid, uuid, 'info.getinfo' as function_id, recipeid, 1 as cnt
from (select appid, uuid, coalesce(get_json_object(parameter_desc,'$.rid'),'') as recipeid
from logs.log_php_app_log l
where logdate='${statis_date}'
and function_id='info.getinfo'
and appid in ('2','4')
) ll 
left semi join haodou_recipe_${curdate}.Video v on (ll.recipeid=v.recipeid)
;

--m端查看视频菜谱
--http://m.haodou.com/recipe/1015921?device=iphone&hash=62f72d4e45b0b0d295370d5e969af797&siteid=1004
--http://m.haodou.com/recipe/851904?from=singlemessage&isappinstalled=1
--http://m.haodou.com/app/weixin/share.php?do=ajax&act=share&callback=jQuery18309852228707168251_1451297279797&req_url=http%3A%2F%2Fm.haodou.com%2Frecipe%2F851904%3Ffrom%3Dsinglemessage%26isappinstalled%3D1&_=1451297282654
insert into table bing.dw_rcp_video_log partition (statis_date='${statis_date}')
select '0' as app_id, concat(remote_addr,http_user_agent) as visitor,
'mhaodoucom.recipe' as function_id, recipeid, 1 as cnt
from (select remote_addr, http_user_agent,
regexp_extract(`path`,'/recipe/([0-9]*)',1) as recipeid
from logs.m_haodou_com l
where logdate='${statis_date}' and `path` rlike '^/recipe/[\\d+].*$'
) ll
left semi join haodou_recipe_${curdate}.Video v on (ll.recipeid=v.recipeid)
;

--以上操作完成后，形成300余小文件，通过group by合并小文件，最终输出10来个文件
insert overwrite table bing.dw_rcp_video_log partition (statis_date='${statis_date}')
select app_id, device_id, function_id, itemid, sum(cnt)
from bing.dw_rcp_video_log
where statis_date='${statis_date}'
group by app_id, device_id, function_id, itemid
;
