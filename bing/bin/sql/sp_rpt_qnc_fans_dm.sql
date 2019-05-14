insert overwrite table bing.rpt_qnc_fans_dm partition(ptdate='${statisdate}')
select count(1) count, count(distinct userid) user_count, count(distinct followuserid) followuserid_count from qnc_haodou_center_${curdate}.userfollow where to_date(createtime) = '${statis_date}';
