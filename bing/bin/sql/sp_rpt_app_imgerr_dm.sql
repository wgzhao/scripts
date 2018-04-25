
--应用图片访问错误情况
insert overwrite table bing.rpt_app_imgerr_dm partition (statis_date='${statis_date}')
select
e.appid as app_id,
coalesce(parse_url(e.url,'HOST'),e.url) as host,
'*' as errormsg,
count(1) as errorcnt,
count(distinct e.uuid) as errordev
from behavior.ods_app_err_log e 
where log_date='${statis_date}'
group by e.appid, 
coalesce(parse_url(e.url,'HOST'),e.url)
; 
