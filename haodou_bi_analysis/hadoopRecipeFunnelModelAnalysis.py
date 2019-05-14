# -*- coding: UTF-8 -*- 
#coding = utf-8
#好豆菜谱客户端 数据统计 漏斗模型

from optparse import OptionParser
import commands
from time import ctime
from MailUtil import *
from XLSWriter import XLSWriter
from hiveDB import *
import traceback
from  datetime  import  *
import time
import sys
import os
import re

class RecipeFunnelModelAnalysis:
	
	def __init__(self, fpath, delay=1):
		self.delay = delay
		self.hive = HiveDB()
		dt = datetime.now()
		self._datetime = dt - timedelta(days = delay)
		self.curDate = self._datetime.strftime("%Y-%m-%d")
		self.file_path = fpath + "/" + self._datetime.strftime("%Y%m") + "/"
		
		if os.path.isdir(self.file_path) is False:
			os.mkdir(self.file_path)
		
		self.shortDate = (self._datetime + timedelta(days = 1)).strftime("%Y%m%d")
		
	def start(self):
		
		#测试
		print "当前分析时间是：" + self.curDate
		
		# 1个月前
		self.findLeftUserRare(30)
		# 2个月前
		self.findLeftUserRare(60)
		# 3个月前
		self.findLeftUserRare(90)
		
 	def findLeftUserRare(self, day):
		
		
		#先找N个月前那天新增的用户
		threeMonthDate = (self._datetime - timedelta(days = day)).strftime("%Y-%m-%d")

		sql = """\
		select upper(ds.device_id) from bing.dw_app_device_ds ds
		inner join bing.dw_app_device_duration_dm dm on ds.device_id=dm.device_id
		where to_date(ds.first_day) = '%s' and ds.app_id in (2,4,6)
		and dm.duration <= 60 and dm.statis_date='%s' and dm.app_id in (2,4,6);
		"""
		
		#cursor = self.hive.execute(sql%(self.curDate))
		cursor = self.hive.execute(sql%(threeMonthDate,threeMonthDate))
		
		
		devices = {}
		for cols in cursor:
			try:
				row = re.split(r"\s+",cols)
				device_id = str(row[0])
				devices[device_id] = device_id
				
				
			except Exception:
				traceback.print_exc()
				continue
				
				
		
		#再到applog日志里面找当天的对应设备ID
		sql = "select upper(device_id) from logs.log_php_app_log where logdate='%s' and appid in (2,4,6)"
		#cursor = self.hive.execute(sql%(self.curDate))
		
		cursor = self.hive.execute(sql%self.curDate)
		
		rowlist = {}
		for cols in cursor:
			try:
				row = re.split(r"\s+",cols)
				deviceid = str(row[0])
				rowlist[deviceid] = deviceid
					
			except Exception:
				traceback.print_exc()
				continue		
					
		#计算这批用户的留存率	
		
		left = {}
		
		for deviceid in rowlist:
			if deviceid in devices:
				left[deviceid] = deviceid
		
		print (day/30),"个月前余下用户的比率 ",len(left)/len(devices)
		
if __name__ == '__main__':
	
	path  = os.path.dirname(os.path.abspath(__file__))
	
	usage = "usage: %prog [options] arg1 arg2"
	parser = OptionParser(usage = usage, version="%prog 0.1")
	parser.add_option("-d", "--delay", help = 'Delay a number day', dest = "delay")
	(options, args) = parser.parse_args()
	
	if len(sys.argv)<=1:
		parser.print_help()
		sys.exit(2)
	
	delay = options.delay or 1
	
	delay = int(delay)
	
	rjcca = RecipeFunnelModelAnalysis(path, delay)
	rjcca.start()
