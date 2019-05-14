
--短信下发统计日报

insert overwrite table bing.rpt_haodou_sendsms_dm partition (statis_date='${statis_date}')
select /*+ mapjoin(t)*/
'haodou' as source_type,
sms_type,
case 
when sms_type=1 then '发送菜谱内容到手机'
when sms_type=2 then '注册验证码'
when sms_type=3 then '好拍优惠卷'
when sms_type=4 then '发送店铺信息'
when sms_type=5 then '找回密码验证码'
when sms_type=6 then '绑定好豆验证码'
when sms_type=7 then '红楼优惠码'
when sms_type=8 then 'v3.2优惠码'
when sms_type=100 then '监控短信'
when sms_type=101 then '后台批量发送短信'
when sms_type=102 then 'BI平台短信'
else concat('未知类型',sms_type)
end as sms_typename,
send_cnt,
mobile_num,
failure_cnt
from
(select `type` as sms_type,
count(`id`) as send_cnt,
count(distinct mobile) as mobile_num,
count(case when sendresult=2 then `id` end) as failure_cnt
from haodou_${curdate}.sendsms
where sendtime between unix_timestamp('${statis_date} 00:00:00') and unix_timestamp('${statis_date} 23:59:59')
group by `type`
) t
;
