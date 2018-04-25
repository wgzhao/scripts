
insert into table bing.dw_iplib_lite_log
select from_unixtime(unix_timestamp()) as logtime, count(*) as rownum
from logs.ip_address_warehouse
;

insert overwrite table bing.dw_iplib_lite
select ipseg, city
from (
select ipseg, city, cnt,
row_number() over(partition by ipseg order by cnt desc) as sn
from (
select substr(ipaddress,1,length(ipaddress)-instr(reverse(ipaddress),'.')+1) as ipseg,
city, count(1) as cnt
from logs.ip_address_warehouse
where ipaddress rlike '^[1-9]'
group by substr(ipaddress,1,length(ipaddress)-instr(reverse(ipaddress),'.')+1), city
) t0
) t1
where sn=1
;
