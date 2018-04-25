
set names='utf8';

insert overwrite table bing.dw_app_function
select
nvl(oof.app_id,nnf.app_id) as app_id,
nvl(oof.function_id,nnf.function_id) as function_id,
nvl(oof.function_name,'') as function_name,
nvl(oof.function_desc,'') as function_desc,
nvl(oof.first_version,nnf.first_version) as first_version,
nvl(oof.first_date,'${statis_date}') as first_date,
nvl(oof.value_factor,-1) as value_factor
from bing.dw_app_function oof 
full join 
(select app_id, function_id,
min(version_id) as first_version
from bing.rpt_app_functiondetail_dm
where statis_date='${statis_date}' 
and app_id in ('2','4','6') and version_id!='*' 
group by app_id, function_id
) nnf on (oof.app_id=nnf.app_id and oof.function_id=nnf.function_id)
;

insert overwrite table bing.dw_app_function
select
ff.app_id,
ff.function_id,
case when coalesce(kf.function_name,'')!='' then kf.function_name else ff.function_name end,
ff.function_desc,
ff.first_version,
ff.first_date,
case when coalesce(kf.value_factor,-1)>-1 then kf.value_factor else ff.value_factor end
from bing.dw_app_function ff 
left outer join bing.dw_app_known_function kf on (ff.app_id=kf.app_id and ff.function_id=kf.function_id)
;
