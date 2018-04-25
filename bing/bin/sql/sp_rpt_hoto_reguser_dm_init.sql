
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;

create table if not exists bing.tmp_hoto_reguser_dm (
  statis_date string,
  newuser int
) row format delimited fields terminated by '\t' stored as textfile
;

insert overwrite table bing.tmp_hoto_reguser_dm
select to_date(regtime) as statis_date, count(userid) as usernum
from haodou_passport_${curdate}.`User`
where regtime<='${statis_date} 23:59:59'
group by to_date(regtime)
;

--直接写入动态分区表存在bug，写入分区与预期不一致，故先写入非分区表，再从非分区表导入分区表
--https://issues.apache.org/jira/browse/HIVE-8151
--Dynamic partition sort optimization inserts record wrongly to partition when used with GroupBy
insert overwrite table bing.rpt_hoto_reguser_dm_np partition (exec_date='${statis_date}')
select /*+ mapjoin(dd,ds)*/
sum(case when ds.statis_date<=dd.statis_date then ds.newuser end) as usernum,
sum(case when ds.statis_date=dd.statis_date then ds.newuser end) as newuser,
dd.statis_date
from (select * from bing.tmp_hoto_reguser_dm where statis_date between '${statis_year}-01-01' and '${statis_date}') dd cross join
bing.tmp_hoto_reguser_dm ds
group by dd.statis_date
;

insert overwrite table bing.rpt_hoto_reguser_dm partition (statis_date)
select 
np.usernum,
np.newuser,
np.statis_date
from bing.rpt_hoto_reguser_dm_np np
where np.exec_date='${statis_date}'
;
