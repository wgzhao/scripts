
--//创建数据库//--
create database bing comment '新BI库' location '/bing/data';
create database bingtest location '/bing/test';


--//系统表//--
create table bing.dual (x string) comment '虚表';
insert overwrite table bing.dual select x from (select count(1) as cnt, 'x' as x from bing.dual) t;


--//管理数据部分（MNG）//--



