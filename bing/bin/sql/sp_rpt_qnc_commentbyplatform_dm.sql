insert overwrite table bing.rpt_qnc_commentbyplatform_dm partition(ptdate='${statisdate}')
select v.* from(
select platform, count(1) comment_count, count(distinct userid) comment_user_count
from qnc_haodou_comment_${curdate}.comment
where status = 1 and to_date(createtime) = '${statis_date}' and type in (2, 3, 11, 13)
group by platform
union all
select t.platforms platform, count(1) comment_count, count(1) comment_user_count
from (
select userid, concat_ws('', collect_set(string(platform))) platforms
from
qnc_haodou_comment_${curdate}.comment
where to_date(createtime) = '${statis_date}' and type in (2, 3, 11, 13)
group by userid
) t
where length(t.platforms) > 1
group by t.platforms) v;
