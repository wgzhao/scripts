# -*- coding: UTF-8 -*- 
#coding = utf-8

#每日新增applog日志入库
#每日新增统计表的分区

from optparse import OptionParser 
from datetime import *
from hiveDB import *
from sharkDB import *
from MailUtil import *
import traceback
import  time
import commands
import os
import sys

def checkData(table, data):
	wst = table + " 日志入库"
	if len(data)>0:
		line_count = 0
		for row in data:
			line_count += len(row)
		if line_count ==0:
			return wst + "<font color=\'red\'>缺失 - " + str(line_count) + "</font><br/>\n"
		return wst + "<font color=\'blue\'>正常 - " + str(line_count) + "</font><br/>\n"
	else:
		return wst + "<font color=\'red\'>缺失 - 0</font><br/>\n"
def errorData(table):
	wst = table + " 日志入库"
	return wst + "<font color=\'red\'>错误</font><br/>\n"


def start(curTime):
	
	print "当前日志检查时间是：" + curTime.strftime("%Y-%m-%d %H:%M:%S")
	
	curDate = curTime.strftime("%Y-%m-%d")
	
	minTime = curTime - timedelta(minutes = 10)
	minTimeStamp = int(time.mktime(minTime.timetuple()))
	minTimeStr = minTime.strftime("%Y-%m-%d %H:%M:%S")
	
	maxTime = curTime + timedelta(minutes = 10)
	maxTimeStamp = int(time.mktime(maxTime.timetuple()))
	maxTimeStr = maxTime.strftime("%Y-%m-%d %H:%M:%S")
	
	mailContent = ""
	
	#nginx 日志
	try:
		#cursor = shark.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.api_haodou_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		cursor = hive.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.api_haodou_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		mailContent = mailContent + str(checkData("api_haodou_com", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("api_haodou_com"))
		traceback.print_exc()
	
	
	try:
		#cursor = shark.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.api_qunachi_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		cursor = hive.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.api_qunachi_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		mailContent = mailContent + str(checkData("api_qunachi_com", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("api_qunachi_com"))
		traceback.print_exc()
	
	try:
		#cursor = shark.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.group_haodou_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		cursor = hive.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.group_haodou_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		mailContent = mailContent + str(checkData("group_haodou_com", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("group_haodou_com"))
		traceback.print_exc()
	
	
	try:
		#cursor = shark.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.hd_bi_exp where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		cursor = hive.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.hd_bi_exp where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		mailContent = mailContent + str(checkData("hd_bi_exp", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("hd_bi_exp"))
		traceback.print_exc()
		
	try:
		#cursor = shark.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.interface_haodou_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		cursor = hive.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.interface_haodou_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		mailContent = mailContent + str(checkData("interface_haodou_com", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("interface_haodou_com"))
		traceback.print_exc()
	
	try:
		#cursor = shark.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.interface_qunachi_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		cursor = hive.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.interface_qunachi_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		mailContent = mailContent + str(checkData("interface_qunachi_com", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("interface_qunachi_com"))
		traceback.print_exc()
	
	try:
		#cursor = shark.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.login_haodou_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		cursor = hive.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.login_haodou_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		mailContent = mailContent + str(checkData("login_haodou_com", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("login_haodou_com"))
		traceback.print_exc()
	
	try:
		#cursor = shark.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.m_haodou_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		cursor = hive.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.m_haodou_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		mailContent = mailContent + str(checkData("m_haodou_com", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("m_haodou_com"))
		traceback.print_exc()
	
	try:
		#cursor = shark.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.m_qunachi_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		cursor = hive.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.m_qunachi_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		mailContent = mailContent + str(checkData("m_qunachi_com", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("m_qunachi_com"))
		traceback.print_exc()
	
	try:
		#cursor = shark.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.shop_haodou_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		cursor = hive.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.shop_haodou_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		mailContent = mailContent + str(checkData("shop_haodou_com", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("shop_haodou_com"))
		traceback.print_exc()
	
	try:
		#cursor = shark.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.wo_haodou_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		cursor = hive.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.wo_haodou_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		mailContent = mailContent + str(checkData("wo_haodou_com", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("wo_haodou_com"))
		traceback.print_exc()
	
	try:
		#cursor = shark.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.wo_qunachi_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		cursor = hive.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.wo_qunachi_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		mailContent = mailContent + str(checkData("wo_qunachi_com", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("wo_qunachi_com"))
		traceback.print_exc()
	
	try:
		#cursor = shark.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.www_haodou_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		cursor = hive.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.www_haodou_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		mailContent = mailContent + str(checkData("www_haodou_com", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("www_haodou_com"))
		traceback.print_exc()
	
	try:
		#cursor = shark.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.www_qunachi_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,minTimeStamp,curDate))
		cursor = hive.execute("select remote_addr,host,log_time,method,path,status,body_bytes_sent,referer,user_id,request_time from logs.www_qunachi_com where log_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,minTimeStamp,curDate))
		mailContent = mailContent + str(checkData("www_qunachi_com", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("www_qunachi_com"))
		traceback.print_exc()
	
	
	#app 日志
	"""
	try:
		cursor = shark.execute("select appid,appkey,format,sessionid,vc,vn,loguid,deviceid,time from logs.app_err_log where time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStr,maxTimeStr,curDate))
		mailContent = mailContent + str(checkData("app_err_log", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("app_err_log"))
		traceback.print_exc()
	
	try:
		cursor = shark.execute("select appid,appkey,format,sessionid,vc,vn,loguid,deviceid,time from logs.app_page_view where time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStr,maxTimeStr,curDate))
		mailContent = mailContent + str(checkData("app_page_view", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("app_page_view"))
		traceback.print_exc()
	
	try:
		cursor = shark.execute("select appid,appkey,format,sessionid,vc,vn,loguid,deviceid,time from logs.app_push_received where time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStr,maxTimeStr,curDate))
		mailContent = mailContent + str(checkData("app_push_received", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("app_push_received"))	
		traceback.print_exc()
	
	try:
		cursor = shark.execute("select appid,appkey,format,sessionid,vc,vn,loguid,deviceid,time from logs.app_push_view where time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStr,maxTimeStr,curDate))
		mailContent = mailContent + str(checkData("app_push_view", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("app_push_view"))	
		traceback.print_exc()
	
	try:
		cursor = shark.execute("select appid,appkey,format,sessionid,vc,vn,loguid,deviceid,time from logs.app_unparsed_err_log where time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStr,maxTimeStr,curDate))
		mailContent = mailContent + str(checkData("app_unparsed_err_log", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("app_unparsed_err_log"))	
		traceback.print_exc()
	"""
	try:
		#cursor = shark.execute("select request_time,device_id,channel_id from logs.log_php_app_log where request_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		cursor = hive.execute("select request_time,device_id,channel_id from logs.log_php_app_log where request_time between '%s' and '%s' and logdate='%s' limit 10"%(minTimeStamp,maxTimeStamp,curDate))
		mailContent = mailContent + str(checkData("log_php_app_log", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("log_php_app_log"))
		traceback.print_exc()
	
	try:
		#cursor = shark.execute("select appid,appkey,sessionid from logs.behavior_pushview where logdate='%s' limit 10"%curDate)
		cursor = hive.execute("select appid,appkey,sessionid from logs.behavior_pushview where logdate='%s' limit 10"%curDate)
		mailContent = mailContent + str(checkData("behavior_pushview", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("behavior_pushview"))
		traceback.print_exc()
		
	try:
		#cursor = shark.execute("select get_json_object(json,'$.status') from logs.log_php_app_resp_log where logdate='%s' and get_json_object(json,'$.status') is not null limit 10"%curDate)
		cursor = hive.execute("select get_json_object(json,'$.status') from logs.log_php_app_resp_log where logdate='%s' and get_json_object(json,'$.status') is not null limit 10"%curDate)
		mailContent = mailContent + str(checkData("log_php_app_resp_log", cursor))
	except Exception:
		mailContent = mailContent + str(errorData("log_php_app_resp_log"))
		traceback.print_exc()
		
	errorMail.sendMail("zhaoweiguo@haodou.com", "%s日志实时入库监控"%curTime.strftime("%Y-%m-%d %H:%M:%S"), mailContent)
	errorMail.sendMail("lifangxing@haodou.com", "%s日志实时入库监控"%curTime.strftime("%Y-%m-%d %H:%M:%S"), mailContent)


#测试模式可以指定启动时间
#自动完成给定天的1点，12点，23点时间段的数据校验

#非测试模式是根据程序运行的当前时间校验数据
#校验的范围是根据当前时间前后各取5分钟共10分钟的数据
#判断是否有数据入库
nowday = datetime.now()
hive = HiveDB()
#shark = SharkDB()
errorMail = MailSender()
path  = os.path.dirname(os.path.abspath(__file__))

if __name__ == '__main__':
	
	usage = "usage: %prog [options] arg1 arg2"
	parser = OptionParser(usage = usage, version="%prog 0.1")
	parser.add_option("-d", "--days", help = 'Specify delay the few days, default value is 1, use \',\' separated, like 10,1', dest = "days")
	parser.add_option("-m", "--mode", help = 'Test mode for specify delay the few days, input value is 1', dest = "mode")
	parser.add_option("-t", "--time", help = 'Execute by specify time, default value is now time', dest = "time")

	(options, args) = parser.parse_args()
	
	if len(sys.argv)<=1:
		parser.print_help()
		sys.exit(2)

	days = options.days or "1"
	delay = days.split(",")
	
	mode = int(options.mode or 0)
	testtime = options.time
	
	if len(delay) != 1 and mode != 0:
	
		for today in range(int(delay[1]),int(delay[0])+1,1):
			
			curDate = (nowday - timedelta(days = today)).strftime("%Y-%m-%d")
			
			start(datetime.strptime(curDate + " 00:10:00", "%Y-%m-%d %H:%M:%S"))
			
			start(datetime.strptime(curDate + " 12:10:00", "%Y-%m-%d %H:%M:%S"))
			
			start(datetime.strptime(curDate + " 23:10:00", "%Y-%m-%d %H:%M:%S"))
			
	elif len(delay) == 1 and mode != 0:	
		
		curDate = (nowday - timedelta(days = int(delay[0]))).strftime("%Y-%m-%d")
			
		start(datetime.strptime(curDate + " " + nowday.strftime("%H:%M:%S"), "%Y-%m-%d %H:%M:%S"))
	elif testtime != None:
		start(datetime.strptime(testtime, "%Y-%m-%d %H:%M:%S"))
	else:
		start(nowday)
	
	
