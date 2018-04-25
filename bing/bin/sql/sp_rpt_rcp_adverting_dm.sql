insert overwrite table bing.rpt_rcp_adverting_dm partition(ptdate='${statis_date}')
select 
sum(if(path like '%adhome_%',1,0)) ad_home, 
sum(if(path like '%adlife_%',1,0)) ad_life, 
sum(if(path like '%adgift_%',1,0)) ad_gift
from logs.shop_haodou_com 
where logdate='${statis_date}' and (path like '%adhome_%' or path like '%adlife_%' or path 
like '%adgift_%');
