insert overwrite table bing.rpt_qnc_signin_dm partition(ptdate='${statisdate}')
select count(1) count, count( distinct userid) user_count from qnc_haodou_center_${curdate}.usersignlog where to_date(signtime) = '${statis_date}';
