use logs;

--删除旧日志表的分区
-- alter table http_api_haodou_com DROP IF EXISTS PARTITION(logdate='${curdate}');
-- alter table http_api_qunachi_com DROP IF EXISTS PARTITION(logdate='${curdate}');
-- alter table http_group_haodou_com DROP IF EXISTS PARTITION(logdate='${curdate}');
-- alter table http_interface_haodou_com DROP IF EXISTS PARTITION(logdate='${curdate}');
-- alter table http_interface_qunachi_com DROP IF EXISTS PARTITION(logdate='${curdate}');
-- alter table http_login_haodou_com DROP IF EXISTS PARTITION(logdate='${curdate}');
-- alter table http_m_haodou_com DROP IF EXISTS PARTITION(logdate='${curdate}');
-- alter table http_m_qunachi_com DROP IF EXISTS PARTITION(logdate='${curdate}');
-- alter table http_shop_haodou_com DROP IF EXISTS PARTITION(logdate='${curdate}');
-- alter table http_wo_haodou_com DROP IF EXISTS PARTITION(logdate='${curdate}');
-- alter table http_wo_qunachi_com DROP IF EXISTS PARTITION(logdate='${curdate}');
-- alter table http_www_haodou_com DROP IF EXISTS PARTITION(logdate='${curdate}');
-- alter table http_www_qunachi_com DROP IF EXISTS PARTITION(logdate='${curdate}');

--覆盖式导入新数据

from access_log
insert overwrite table www_qunachi_com partition (logdate='${curdate}')
select ip,host,time,method,request,status,size,referer,cookieuid,requesttime,session,httpxrequestedwith,agent,upstreamresponsetime where logdate='${curdate}' and host = 'www.qunachi.com'
insert overwrite table www_haodou_com partition (logdate='${curdate}')
select ip,host,time,method,request,status,size,referer,cookieuid,requesttime,session,httpxrequestedwith,agent,upstreamresponsetime where logdate='${curdate}' and host = 'www.haodou.com'
insert overwrite table wo_qunachi_com partition (logdate='${curdate}')
select ip,host,time,method,request,status,size,referer,cookieuid,requesttime,session,httpxrequestedwith,agent,upstreamresponsetime where logdate='${curdate}' and host = 'wo.qunachi.com'
insert overwrite table wo_haodou_com partition (logdate='${curdate}')
select ip,host,time,method,request,status,size,referer,cookieuid,requesttime,session,httpxrequestedwith,agent,upstreamresponsetime where logdate='${curdate}' and host = 'wo.haodou.com'
insert overwrite table shop_haodou_com partition (logdate='${curdate}')
select ip,host,time,method,request,status,size,referer,cookieuid,requesttime,session,httpxrequestedwith,agent,upstreamresponsetime where logdate='${curdate}' and host = 'shop.haodou.com'
insert overwrite table m_qunachi_com partition (logdate='${curdate}')
select ip,host,time,method,request,status,size,referer,cookieuid,requesttime,session,httpxrequestedwith,agent,upstreamresponsetime where logdate='${curdate}' and host = 'm.qunachi.com'
insert overwrite table m_haodou_com partition (logdate='${curdate}')
select ip,host,time,method,request,status,size,referer,cookieuid,requesttime,session,httpxrequestedwith,agent,upstreamresponsetime where logdate='${curdate}' and host = 'm.haodou.com'
insert overwrite table m_dj_haodou_com partition (logdate='${curdate}')
select ip,host,time,method,request,status,size,referer,cookieuid,requesttime,session,httpxrequestedwith,agent,upstreamresponsetime where logdate='${curdate}' and host = 'm.dj.haodou.com'
insert overwrite table login_haodou_com partition (logdate='${curdate}')
select ip,host,time,method,request,status,size,referer,cookieuid,requesttime,session,httpxrequestedwith,agent,upstreamresponsetime where logdate='${curdate}' and host = 'login.haodou.com'
insert overwrite table interface_qunachi_com partition (logdate='${curdate}')
select ip,host,time,method,request,status,size,referer,cookieuid,requesttime,session,httpxrequestedwith,agent,upstreamresponsetime where logdate='${curdate}' and host = 'interface.qunachi.com'
insert overwrite table interface_haodou_com partition (logdate='${curdate}')
select ip,host,time,method,request,status,size,referer,cookieuid,requesttime,session,httpxrequestedwith,agent,upstreamresponsetime where logdate='${curdate}' and host = 'interface.haodou.com'
insert overwrite table group_haodou_com partition (logdate='${curdate}')
select ip,host,time,method,request,status,size,referer,cookieuid,requesttime,session,httpxrequestedwith,agent,upstreamresponsetime where logdate='${curdate}' and host = 'group.haodou.com'
insert overwrite table api_qunachi_com partition (logdate='${curdate}')
select ip,host,time,method,request,status,size,referer,cookieuid,requesttime,session,httpxrequestedwith,agent,upstreamresponsetime where logdate='${curdate}' and host = 'api.qunachi.com'
insert overwrite table api_haodou_com partition (logdate='${curdate}')
select ip,host,time,method,request,status,size,referer,cookieuid,requesttime,session,httpxrequestedwith,agent,upstreamresponsetime where logdate='${curdate}' and (host = 'api.haodou.com' or host = 'api.hoto.cn');

