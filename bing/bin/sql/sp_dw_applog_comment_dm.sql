
insert overwrite table bing.dw_applog_comment_dm partition (statis_date='${statis_date}')
select cast(request_time as timestamp), app_id, device_id, channel_id, version_id, userip, userid, function_id,
coalesce(get_json_object(parameter_info,'$.rid'),get_json_object(parameter_info,'$.itemid')) as p_itemid,
get_json_object(parameter_info,'$.rrid') as p_replyid, 
get_json_object(parameter_info,'$.type') as p_type,
get_json_object(parameter_info,'$.atuid') as p_atuserid, 
parameter_info as p_content
from bing.ods_app_requestlog_dm
where statis_date='${statis_date}'
and function_id='comment.addcomment'
;
