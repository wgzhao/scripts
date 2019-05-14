insert overwrite table bing.rpt_qnc_message_dm partition(ptdate='${statisdate}')
select count(1) cnt, count(distinct userid) user_cnt
from qnc_haodou_center_${curdate}.usermessagereply
where to_date(createtime) = '${statis_date}';
