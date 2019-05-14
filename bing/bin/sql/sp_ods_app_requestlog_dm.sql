
set mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec;
set mapreduce.output.fileoutputformat.compress=true;
set hive.exec.compress.output=true;

insert overwrite table bing.ods_app_requestlog_dm partition (statis_date='${statis_date}', source_type='applog')
select from_unixtime(cast(request_time as bigint)) as request_time, 
case when uuid is null or uuid='' then device_id else concat(device_id,'|',uuid) end as device_id, 
case when appid='4' then concat(channel_id,'_',version_id) else channel_id end, userip, appid as app_id, version_id, 
case when userid!='0' then userid 
when get_json_object(parameter_desc,'$.uid') not in ('0','') then get_json_object(parameter_desc,'$.uid')
end as userid, 
case when function_id='recipe.getcollectrecomment' then case
when get_json_object(parameter_desc,'$.offset')!='0' then 'recipe.getcollectrecomment_more'
when get_json_object(parameter_desc,'$.type')='热门菜谱' then 'recipe.getcollectrecomment_hot'
when get_json_object(parameter_desc,'$.type')='私人定制' then 'recipe.getcollectrecomment_private'
when get_json_object(parameter_desc,'$.type')='时令佳肴' then 'recipe.getcollectrecomment_season'
when get_json_object(parameter_desc,'$.type')='达人菜谱' then 'recipe.getcollectrecomment_guru'
when get_json_object(parameter_desc,'$.type')='最新菜谱' then 'recipe.getcollectrecomment_newest'
when get_json_object(parameter_desc,'$.type')='快乐的烘焙' then 'recipe.getcollectrecomment_bake'
when get_json_object(parameter_desc,'$.type')='猜你喜欢' then 'recipe.getcollectrecomment_guess'
when get_json_object(parameter_desc,'$.type') is not null then 'recipe.getcollectrecomment_other'
else 'recipe.getcollectrecomment' end
when function_id='search.getlist' then case
when get_json_object(parameter_desc,'$.offset')!='0' then 'search.getlist_more'
when get_json_object(parameter_desc,'$.scene')='k1' then 'search.getlist_normal'
when get_json_object(parameter_desc,'$.scene')='k2' then 'search.getlist_tag_list'
when get_json_object(parameter_desc,'$.scene')='k3' then 'search.getlist_tag_detail'
when get_json_object(parameter_desc,'$.scene')='t1' then 'search.getlist_tag_category'
when get_json_object(parameter_desc,'$.scene')='t2' then 'search.getlist_tag_hot'
when nvl(get_json_object(parameter_desc,'$.tagid'),'') not in ('','null') then 'search.getlist_tag'
else 'search.getlist' end
when function_id='topic.view' then case
when get_json_object(parameter_desc,'$.type')='3' and get_json_object(parameter_desc,'$.offset')='0' then 'topic.view_wiki3'
when get_json_object(parameter_desc,'$.type')='3' and get_json_object(parameter_desc,'$.offset')!='0' then 'topic.view_wiki3_more'
else 'topic.view' end
when function_id='read.getlist' then case
when get_json_object(parameter_desc,'$.offset')!='0' then 'read.getlist_more'
else 'read.getlist' end
else function_id end as function_id,
parameter_desc as parameter_info,
'' as parameter_desc,
case when uuid!='' then uuid end as uuid
from logs.log_php_app_log
where logdate='${statis_date}' and cast(request_time as bigint)>0 and device_id is not null 
and channel_id is not null and userip is not null and appid is not null and function_id is not null
;
