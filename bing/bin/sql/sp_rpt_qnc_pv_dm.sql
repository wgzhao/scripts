add jar /usr/lib/hive/lib/hive-contrib.jar;
add jar /usr/lib/hive/lib/hive-serde.jar;
insert overwrite table bing.rpt_qnc_pv_dm partition(ptdate='${statisdate}')
select * from 
(select 1 type, sum(count) sum
from(
select count(1) count
from logs.www_qunachi_com
where logdate='${statis_date}'
and method='GET' and status=200
and path like '/topic%'
and (http_user_agent not like 'Apache-HttpClient%' and http_user_agent not rlike '[Ss]pider' and http_user_agent not rlike 'bot[^a-zA-Z]')
union all
select count(1) count
from(
select regexp_extract(path, '/topic.*?id=([0-9]+)', 1) topicid
from logs.api_qunachi_com
where logdate='${statis_date}'
and method='GET' and status=200
and path like '/topic%'
and (http_user_agent not like 'Apache-HttpClient%') 
and regexp_extract(path, '/topic.*?id=([0-9]+)', 1) != '') t
left outer join
(select topicid from qnc_haodou_pai_${curdate}.shoptopic) v
on t.topicid = v.topicid ) m1
union all
select 2 type, sum(count) sum
from(
select count(1) count
from logs.log_php_app_log
where logdate='${statis_date}'
and device_id is not null
and function_id like '%pai.getinfov3%'
and (appid=1 or appid=3)
union all
select count(1) count
from logs.www_qunachi_com
where logdate='${statis_date}'
and method='GET' and status=200
and path rlike('^/share/[0-9]+')
and (http_user_agent not like 'Apache-HttpClient%' and http_user_agent not rlike '[Ss]pider' and http_user_agent not rlike 'bot[^a-zA-Z]') ) m2
union all
select 3 type, sum(count) sum
from(
select count(1) count
from logs.log_php_app_log
where logdate='${statis_date}'
and device_id is not null
and function_id like '%shop.getinfov2%'
and (appid=1 or appid=3)
union all
select count(1) count
from logs.www_qunachi_com
where logdate='${statis_date}'
and method='GET' and status=200
and path rlike('^/shop/[0-9]+')
and (http_user_agent not like 'Apache-HttpClient%' and http_user_agent not rlike '[Ss]pider' and http_user_agent not rlike 'bot[^a-zA-Z]') ) m3
union all
select 4 type, sum(count) sum
from(
select count(1) count
from logs.log_php_app_log
where logdate='${statis_date}'
and device_id is not null
and function_id like '%feature.featurecontent%'
and (appid=1 or appid=3)
union all
select count(1) count
from(
select regexp_extract(path, '/topic.*?id=([0-9]+)', 1) topicid
from logs.api_qunachi_com
where logdate='${statis_date}'
and method='GET' and status=200
and path like '/topic%'
and (http_user_agent not like 'Apache-HttpClient%') 
and regexp_extract(path, '/topic.*?id=([0-9]+)', 1) != '') t
left outer join
(select itemid from qnc_haodou_mobile_${curdate}.qncfeature where type = 4) v
on t.topicid = v.itemid) m4
) m;
