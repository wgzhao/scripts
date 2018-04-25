insert overwrite table bing.rpt_rcp_sign_dm partition(ptdate='${statis_date}')
select t.web_sign, tt.android_sign, tt.ios_sign
from(
select count(1) web_sign from logs.wo_haodou_com where logdate= '${statis_date}' and path like '%user/sign.php?do=Sign%') t
full outer join
(select sum(if(appid=2,1,0)) android_sign,  sum(if(appid=4,1,0)) ios_sign from logs.log_php_app_log where logdate= '${statis_date}' and function_id like '%account.checkin%' and (appid=2 or appid=4)) tt;
