add jar hdfs://hdcluster/udf/joda-time-2.4.jar;
add jar hdfs://hdcluster/udf/continuous.jar;
CREATE TEMPORARY FUNCTION continuous AS 'com.haodou.hive.bing.com.Continuous';
insert overwrite table bing.rpt_qnc_signincontinuousdist_mm partition(ptdate='${statisdate}')
select t.userid, continuous(collect_set(t.signdate)) signin_max
from(
select userid, to_date(signtime) signdate
from qnc_haodou_center_${curdate}.usersignlog
where to_date(signtime) between '${preday30_date}' and '${statis_date}'
order by userid, signdate ) t
group by t.userid;
