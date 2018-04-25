
--管理员豆币发放明细记录
insert overwrite table bing.dw_haodou_adminlog_doubi partition (statis_month='${statis_month}')
select
userid as operid, 
username as opername,
itemname as userlist,
log as operinfo,
createtime as opertime,
size(split(itemname,',')) as usernum,
int(regexp_extract(log, '.*豆币(.*\\d)\;经验值.*', 1)) as perdoubi,
size(split(itemname,','))*int(regexp_extract(log, '.*豆币(.*\\d)\;经验值.*', 1)) as doubi
from haodou_admin_${curdate}.adminlog
where menukey='center_user_sendtb' 
and createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
;
