
insert overwrite table bing.rpt_app_apistat_dm partition (statis_date='${statis_date}')
select appid, method, status,
count(1) as call_cnt,
count(distinct uuid) as dev_num
from
(select 
parse_url(concat('http://api.qunachi.com',`path`),'QUERY','appid') as appid,
parse_url(concat('http://api.qunachi.com',`path`),'QUERY','method') as method,
parse_url(concat('http://api.qunachi.com',`path`),'QUERY','uuid') as uuid,
status
from logs.api_haodou_com
where logdate='${statis_date}'
and `path` like '/index.php%'
) t
group by appid, method, status
;
