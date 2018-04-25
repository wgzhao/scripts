
--设置
set mapreduce.map.memory.mb=8192;    --设置内存
set mapreduce.reduce.memory.mb=8192;
set hive.auto.convert.join=false;
set hive.cli.print.header=true;

--多查询
select t.*   --行注释 
from bing.rpt_hoto_devnum_dm t
where statis_date='2015-01-15'
;

--加载udf
add jar hdfs://hdcluster/udf/bing.jar;
create temporary function html2text as 'com.haodou.hive.bing.Html2Text';

select t.*, 'http://www.haodou.com/' as `链接`
from bing.rpt_hoto_remain_dm t
where statis_date='2015-01-14'
;
