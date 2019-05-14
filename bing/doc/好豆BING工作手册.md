好豆BING工作手册  

2014/06/04  


# 概述  
（此处省略宏图大计美好愿景800字。）  


# 工作环境  

**主机**  

天津 tj-wq.cluster-hadoop.1-16.hd.com    
注，连接天津VPN后，访问IP 10.1.1.16   外网IP 123.150.200.216  

**工作目录** 
 
/home/dc/bing      #主目录  
/home/dc/bing/bin  #代码部署目录
/home/dc/bing/bin/sql  #加入调度任务的SQL脚本
/home/dc/bing/lib  #库/工具目录  
/home/dc/bing/log  #日志目录
/home/dc/bing/tmp  #临时文件目录  

**hdfs目录**  

/bing        #主目录   
/bing/data   #标准数据目录  
/bing/ext    #扩展外部数据目录  
/bing/idx    #索引目录 

hdfs上的全路径为： hdfs://hdcluster/bing/ ...

> 参考命令  
> 查看目录 hadoop fs -ls /bing  
> 创建目录 hadoop fs -mkdir /bing  
> 改变所属组 hadoop fs -chgrp dc /bing  
> 给组赋权 hadoop fs -chmod -R g+w /bing  注意！每次在该目录下新增目录后，都要执行该命令。该步骤可以通过修改dfs.umask解决。  


**hive数据库**  

数据库名：bing  
实时数据库名：birt  

## 天津Hadoop集群参数信息

**主机参数**  

> HOSTNAME=tj-wq.cluster-hadoop.1-16.hd.com  
> HADOOP_DIR=/usr/lib/hadoop/  
> HADOOP_COMMON_HOME=/usr/lib/hadoop/  
> HADOOP_MAPRED_HOME=/usr/lib/hadoop-mapreduce  
> HADOOP_CONF_DIR=/etc/hadoop/conf  
> HIVE_HOME=/usr/lib/hive/  
> SCALA_HOME=/opt/app/scala  
> SPARK_HOME=/opt/app/spark/  
> SPARK_JAR=/opt/app/spark/assembly/target/scala-2.10/spark-assembly_2.10-0.9.0-incubating-hadoop2.2.0.jar  
> JAVA_HOME=/usr/java/jdk1.7.0_25/  
> ZOOOKEEPER_HOME=/usr/lib/zookeeper  
> YARN_HOME=/usr/lib/hadoop/  
> YARN_CONF_DIR=/usr/lib/hadoop/etc/hadoop  
> LD_LIBRARY_PATH=:/opt/app/mysql/lib  
> LANG=zh_CN.UTF-8  
> LC_CTYPE=zh_CN.UTF-8  

**Hadoop参数**  

>

**HBase参数**  

>

**Hive参数**  

> hive.metastore.warehouse.dir=/apps/hive/warehouse  


**Spark参数**  


# 数据仓库规范

## 表设计规范

- 建表时，必须明确的写明所属库（bing）。  
- 如果表中的数据可以依赖SQL完成，则设计为标准表；否则，应该设计为外部扩展表。  
- 创建外部扩展表时，应该指定存储路径为 location '/bing/ext/表名'  
- 创建索引时，应该指定存储路径为 location '/bing/idx/索引名'，并指定in table索引表名与索引名一致
- 如果表的数据为增量，则应该设计为相应增量周期的分区表。以便在需要重新跑数据时，可以按覆盖方式写入相应分区，避开数据删除问题。  
- 应在建表时加入字段与表注释。  
- 字段命名尽量保持一致，并且格式一致。典型的如统计日期字段，statis_date，格式yyyy-mm-dd。  
- --由于shark不支持date格式日期，所以时间字段需要使用timestamp，以保证hive和shark皆可访问。--  
- 设计时应规划好数据存储周期，比较常见的规则如：原始数据尽可能长久；中间数据6+1个月；报表等结果数据尽可能久。应开发自动化数据维护工具。

### 开发注意事项

- 在加载数据（load data）到Hive 表时，如果源文件在HDFS 上，会被从源路径移到Hive 表的存储路径上。
- SQL 中的CASE WHEN 不用OR 联合多个条件，分开写
- 对于timestamp 字段，hive 可直接使用字符串格式，如'2014-06-02 00:00:00' 做运算/判断，而shark 中要写为unix_timestamp('2014-06-02 00:00:00')

## 编码规范

**表命名**  

格式： (层级前缀)_(业务)_(具体内容)_(数据周期)(数据份数)[_(特定后缀)]  

- （层级）原始数据 ods_  
- （层级）中间数据 dw_
- （层级）报表数据 rpt_
- （层级）管理数据 mng_
- （层级）临时数据 tmp_
- （业务）应用端 app
- （业务）网站端 web
- （业务）菜谱综合 rcp
- （业务）去哪吃综合 qnc
- （业务）广场综合 grp
- （数据周期）日 d
- （数据周期）周 w
- （数据周期）月 m
- （数据份数）一份 s
- （数据份数）多份 m
- （特定后缀）扩展外部表 _ext
- （特定后缀）备份表 _bak

例如：表名 dw_app_device_dm_ext  表示这是一个与手机应用相关的中间层表，存放设备数据，按日增量更新，并且这是一个外部扩展表。  

**索引命名**  

基本原则为，与数据表名一致，加idx_前缀。  
如果一个表需要多个索引，在标准索引名后，补上数字序号或索引字段名。  

**过程命名**  

基本原则为，与结果数据表名一致，加sp_前缀。  
如：sp_dw_app_device_dm_ext.py 或 sp_dw_app_device_dm_ext.sql 或 sp_dw_app_device_dm_ext.sh  

## 自定义函数UDF

目前提供的自定义函数包括：

- getweekday
- getapppara 截取applog的desc中相应key的内容

自定义函数使用方式

'''
add jar hdfs://hdcluster/udf/bing.jar;
create temporary function getweekday as 'com.haodou.hive.bing.getWeekday';
create temporary function getapppara as 'com.haodou.hive.bing.getAppPara';
'''

由于Hive 0.12版本仅支持临时加载自定义函数，为减少每次手工加载过程，加载过程在sqlexec.py 中自动处理（使用sqlexec.rc 文件）。
日常维护sqlexec.rc 文件即可。

## 运行方式

**调度方式**

目前使用Azkaban（一个开源的工作流管理工具 https://github.com/azkaban/azkaban），执行节点在10.1.1.10机器上。
Web访问地址：https://10.1.1.17:8043 或 https://123.150.200.217:8043

在sh中调用SQL脚本方式为
/home/dc/bing/bin/sqlexec.py --sql=/home/dc/bing/bin/sql/sample.sql
注：此方式为默认调用方式，为当天运行使用。指定日期运行，需要带--date=yyyymmdd 参数。

**SQL脚本支持的变量**

根据开发需要进行支持。目前支持：

| SQL脚本变量 | 变量含义及取值 |
| --- | --- |
| ${statis_date} | 统计日期。根据sql传入参数确定，默认为运行时的前一天。格式yyyy-mm-dd |
| ${statisdate} | 统计日期。根据sql传入参数确定，默认为运行时的前一天。格式yyyymmdd |
| ${statis_begin_time} | 统计日期的开始时间。格式yyyy-mm-dd 00:00:00 |
| ${statis_end_time} | 统计日期的结束时间。格式yyyy-mm-dd 23:59:59 |
| ${preday_date} | 统计日期的前1天。格式yyyy-mm-dd |
| ${preday2_date} | 统计日期的前2天。格式yyyy-mm-dd |
| ${preday3_date} | 统计日期的前3天。格式yyyy-mm-dd |
| ${preday4_date} | 统计日期的前4天。格式yyyy-mm-dd |
| ${preday5_date} | 统计日期的前5天。格式yyyy-mm-dd |
| ${preday6_date} | 统计日期的前6天。格式yyyy-mm-dd |
| ${preday7_date} | 统计日期的前7天。格式yyyy-mm-dd |
| ${preday13_date} | 统计日期的前13天。格式yyyy-mm-dd |
| ${preday14_date} | 统计日期的前14天。格式yyyy-mm-dd |
| ${preday29_date} | 统计日期的前29天。格式yyyy-mm-dd |
| ${preday30_date} | 统计日期的前30天。格式yyyy-mm-dd |
| ${preday60_date} | 统计日期的前60天。格式yyyy-mm-dd |
| ${nextday_date} | 统计日期的后一天。格式yyyy-mm-dd |
| ${nextdaydate} | 统计日期的后一天。格式yyyymmdd |
| ${nextday30_date} | 统计日期的后30天。格式yyyy-mm-dd |
| ${nextday60_date} | 统计日期的后60天。格式yyyy-mm-dd |
| ${statis_month} | 统计日期所属月份。格式yyyy-mm。 |
| ${statis_year} | 统计日期所属年份。格式yyyy。 |
| ${firstday_date} | 统计日期当月的第一天。格式yyyy-mm-dd |
| ${lastday_date} | 统计日期当月的最后一天。格式yyyy-mm-dd |
| ${statis_week} | 统计日期所属周数。格式yyyy-ww。星期一至星期日算一周，每年的第一个星期一算当年第一周。 |
| ${statisweek_firstday} | 统计日期所属周的周一。格式yyyy-mm-dd。 |
| ${statisweek_lastday} | 统计日期所属周的周日。格式yyyy-mm-dd。 |
| ${preweek} | 统计日期所属周的前一周。格式yyyy-ww。 |
| ${cur_date} | 运行时的当前日期。格式yyyy-mm-dd |
| ${curdate} | 运行时的当前日期。格式yyyymmdd |
| ${xid} | 传递参数。根据sql传入参数xid确定，默认为null。 |
|  |  |

