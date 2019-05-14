add jar /usr/lib/hive/lib/hive-contrib.jar;
add jar /usr/lib/hive/lib/hive-serde.jar;
insert overwrite table bing.rpt_qnc_useractive_dm partition(ptdate='${statisdate}')
select t1.android, t2.ios, t3.web
from
(select count(distinct regexp_extract(path, 'deviceid=([0-9a-z]+)', 1)) android
from logs.api_qunachi_com
where logdate='${statis_date}' and path rlike 'appid=3' and regexp_extract(path, 'deviceid=([0-9a-z]+)', 1) != '') t1
full outer join
(select count(distinct regexp_extract(path, 'deviceid=([0-9a-z]+)', 1)) ios
from logs.api_qunachi_com
where logdate='${statis_date}' and path rlike 'appid=1' and regexp_extract(path, 'deviceid=([0-9a-z]+)', 1) != '') t2
full outer join
(select count(distinct user_id) web
from logs.www_qunachi_com where logdate='${statis_date}'
and user_id != '-') t3;
