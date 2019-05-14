insert overwrite table bing.rpt_rcp_shoppv_dm partition(ptdate='${statis_date}')
select 
count(1) pv,
count(distinct concat(http_user_agent, remote_addr)) uv,
count(distinct concat(remote_addr)) ip
from logs.shop_haodou_com
where logdate='${statis_date}'
and (path='/' or path like '/life.php%' or path like '/lifeall.php%' or path like '/
lifeview.php%' or path like '/gift.php%' or path like '/all.php%' or path like '/view.
php%' or path like '/my.php%' or path like '/order.php%') and http_user_agent not rlike '[
Ss]pider' and http_user_agent not rlike 'bot[^a-zA-Z]';
