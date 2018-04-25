
set names='utf8';

insert overwrite table bing.dw_app_function
select app_id, function_id,
'' as function_name,
'' as function_desc,
min(version_id) as first_version,
min(statis_date) as first_date,
-1 as value_factor
from bing.rpt_app_functiondetail_dm
where app_id in ('2','4','6') and version_id!='*' 
distribute by app_id, function_id
group by app_id, function_id
;

insert overwrite table bing.dw_app_function
select
ff.app_id,
ff.function_id,
case when coalesce(kf.function_name,'')!='' then kf.function_name else '' end,
'',
ff.first_version,
ff.first_date,
case when coalesce(kf.value_factor,-1)>-1 then kf.value_factor else -1 end
from bing.dw_app_function ff 
left outer join bing.dw_app_known_function kf on (ff.app_id=kf.app_id and ff.function_id=kf.function_id)
;
