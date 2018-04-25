insert overwrite table bing.rpt_rcp_wealthincrement_dm partition(ptdate='${statis_date}')
select 
sum(wealth), sum(if(wealth>0,1,0)),
sum(if(wealth>500,wealth,0)) gt500, sum(if(wealth>500,1,0)) gt500_user,
sum(if(wealth>=10000 and wealth<=50000,wealth,0)) bt10k_50k, sum(if(wealth>=10000 and 
wealth<=50000,1,0)) bt10k_50k_user,
sum(if(wealth>50000 and wealth<=100000,wealth,0)) bt50k_100k, sum(if(wealth>50000 and 
wealth<=100000,1,0)) bt50k_100k_user,
sum(if(wealth>100000,wealth,0)) gt100k, sum(if(wealth>100000,1,0)) gt100k_user
from haodou_passport_${curdate}.`user`;
