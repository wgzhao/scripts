
select app_id, version_id, device_id, case when userid!='' then userid end as userid, begin_time, end_time, request_dur, request_info,
case 
when instr(request_info,'passport.loginbyconnect')>0 then '3p'
when instr(request_info,'passport.bindconnectstatus')>0 and instr(request_info,'common.sendcode')>0 then '3p'
when instr(request_info,'common.sendcode')>0 then 'phone'
when instr(request_info,'passport.reg')>0 then 'email'
else 'other' end as regway
	from bing.dw_app_device_reglogin_dm
	where statis_date='${statis_date}'
	10:46:22
	老米钟鹏 2015/3/12 10:46:22


	from
	(select app_id, version_id, device_id, case when userid!='' then userid end as userid, begin_time, end_time, request_dur, request_info,
	case 
	when instr(request_info,'passport.loginbyconnect')>0 then '3p'
	when instr(request_info,'passport.bindconnectstatus')>0 and instr(request_info,'common.sendcode')>0 then '3p'
	when instr(request_info,'common.sendcode')>0 then 'phone'
	when instr(request_info,'passport.reg')>0 then 'email'
	else 'other' end as regway
		from bing.dw_app_device_reglogin_dm
		where statis_date='${statis_date}'
		) tt left outer join
		(select userid, email, mobile, regtime, regip
		from hd_haodou_passport_${curdate}.User
		where regtime between '${statis_date} 00:00:00' and '${statis_date} 23:59:59'
		) uu on (tt.userid is not null and tt.userid=uu.userid)
		where (uu.userid is not null and uu.regtime between tt.begin_time and tt.end_time)


