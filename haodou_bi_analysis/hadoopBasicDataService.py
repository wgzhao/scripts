# -*- coding: UTF-8 -*- 
#coding = utf-8
#后台抽取程序的基础数据服务

from hadoopChannelUninstallAnalysis import *
from optparse import OptionParser
from time import ctime
from  datetime  import  *
from sharkDB import *
from hiveDB import *
from MailUtil import *
import traceback
from DBUtil import *
import threading
import commands
import random
import time
import pickle
import sys
import os
import re

#保留5天的数据
class DailyCleaner(threading.Thread):
	def __init__(self,path):
		threading.Thread.__init__(self,name='DailyCleaner')
		dt = datetime.now()
		self._datetime = dt - timedelta(days = 3)
		self.path = path
		
	def run(self):
		file_path = self.path + "/" + self._datetime.strftime("%Y%m") + "/get*" + self._datetime.strftime("%Y-%m-%d") + ".txt"
		if os.path.exists(file_path):
			os.popen('rm -rf ' + file_path)

#后台数据库同步基础数据
class DailyBeforeSearch():
	
	def __init__(self, delay=1, path=""):
		self.path = path
		self.yesterdayMap = {}
		self.weekMap = {}
		self.monthMap = {}
		self.old_recipe_devices = {}
		
		self.delay = delay
		
		dt = datetime.now()
		self._datetime = dt - timedelta(days = self.delay)
		self.curDate = self._datetime.strftime("%Y-%m-%d")
		self.hive = HiveDB()
		#self.shark = SharkDB()
		self.db = DB()
	
	def search(self):
		
		printLog("查往期的渠道和设备列表")
		
		self.recipeSearch()
		
		self.qunachiSearch()
				
	
	def recipeSearch(self):
		
		file_path = self.path + "/" + self._datetime.strftime("%Y%m")
		if os.path.isdir(file_path) is False:
			os.mkdir(file_path)
			
		filename = file_path + "/get_yesterdayMap_" + self.curDate + ".txt"
		if os.path.exists(filename):
			printLog(filename + ' 文件已存在，不加载...')
		
		else:	
			self.yesterdayMap = {}
			printLog("生成菜谱昨日渠道-设备号列表")
			for day in range(1,2,1):
				self.prepareForPerDate(day,self.yesterdayMap, "appid = 2")			
			
			pstuff=pickle.dumps(self.yesterdayMap)
			
			self.saveToFile(filename, pstuff)
		
		filename = file_path + "/get_weekMap_" + self.curDate + ".txt"
		if os.path.exists(filename):
			printLog(filename + ' 文件已存在，不加载...')
		else:	
			self.weekMap = {}
			printLog("生成菜谱7日渠道-设备号列表")
			for day in range(1,8,1):
				self.prepareForPerDate(day,self.weekMap, "appid = 2")
			
			pstuff=pickle.dumps(self.weekMap)
			
			self.saveToFile(filename, pstuff)
		
		filename = file_path + "/get_monthMap_" + self.curDate + ".txt"
		if os.path.exists(filename):
			printLog(filename + ' 文件已存在，不加载...')
		else:	
			self.monthMap = {}
			printLog("生成菜谱30天的渠道-设备号列表")
			for day in range(1,31,1):
				self.prepareForPerDate(day,self.monthMap, "appid = 2")
			
			pstuff=pickle.dumps(self.monthMap)
			
			self.saveToFile(filename, pstuff)
		
		"""	
		filename = file_path + "/get_oldRecipeDevicesMap_" + self.curDate + ".txt"
		if os.path.exists(filename):
			printLog(filename + ' 文件已存在，不加载...')
		else:
			self.old_recipe_devices = {}
			printLog("获取菜谱老用户设备号")
			cursor = self.db.execute("SELECT device_id FROM bi_app_device_recipe")
			for row in cursor:
				deviceid = ""
				if isinstance(row[0], str) == False:
					deviceid = row[0].encode('utf-8')
				else:
					deviceid = str(row[0])
				self.old_recipe_devices[deviceid] = 0
					
			pstuff=pickle.dumps(self.old_recipe_devices)
			
			self.saveToFile(filename, pstuff)
		"""
		
		filename = file_path + "/get_recipeChannelUninstallMap_" + self.curDate + ".txt"
		if os.path.exists(filename):
			printLog(filename + ' 文件已存在，不加载...')
		else:
			printLog("获取渠道卸载用户数")
			cua = ChannelUninstallAnalysis(self.delay, self.path)
			uninstalls = cua.startExtract()
			pstuff=pickle.dumps(uninstalls)
			
			self.saveToFile(filename, pstuff)
	
	def qunachiSearch(self):
		
		file_path = self.path + "/" + self._datetime.strftime("%Y%m")
		if os.path.isdir(file_path) is False:
			os.mkdir(file_path)
			
		filename = file_path + "/get_qunachi_yesterdayMap_" + self.curDate + ".txt"
		if os.path.exists(filename):
			printLog(filename + ' 文件已存在，不加载...')
		
		else:	
			self.yesterdayMap = {}
			printLog("生成去哪吃昨日渠道-设备号列表")
			#for day in range(1,2,1):
			self.prepareForPerDate(0,self.yesterdayMap, "(appid = 1 or appid = 3)")			
			
			pstuff=pickle.dumps(self.yesterdayMap)
			
			self.saveToFile(filename, pstuff)
		
		filename = file_path + "/get_qunachi_weekMap_" + self.curDate + ".txt"
		if os.path.exists(filename):
			printLog(filename + ' 文件已存在，不加载...')
		else:	
			self.weekMap = {}
			printLog("生成去哪吃7日渠道-设备号列表")
			for day in range(1,8,1):
				self.prepareForPerDate(day,self.weekMap, "(appid = 1 or appid = 3)")
			
			pstuff=pickle.dumps(self.weekMap)
			
			self.saveToFile(filename, pstuff)
		
		filename = file_path + "/get_qunachi_monthMap_" + self.curDate + ".txt"
		if os.path.exists(filename):
			printLog(filename + ' 文件已存在，不加载...')
		else:	
			self.monthMap = {}
			printLog("生成去哪吃30天的渠道-设备号列表")
			for day in range(1,31,1):
				self.prepareForPerDate(day,self.monthMap, "(appid = 1 or appid = 3)")
			
			pstuff=pickle.dumps(self.monthMap)
			
			self.saveToFile(filename, pstuff)
		
		"""
		filename = file_path + "/get_oldQunachiDevicesMap_" + self.curDate + ".txt"
		if os.path.exists(filename):
			printLog(filename + ' 文件已存在，不加载...')
		else:
			self.old_recipe_devices = {}
			printLog("获取去哪吃老用户设备号")
			cursor = self.db.execute("SELECT device_id FROM bi_app_device_qunachi where create_date <='%s'"%self.curDate)
			for row in cursor:
				deviceid = ""
				if isinstance(row[0], str) == False:
					deviceid = row[0].encode('utf-8')
				else:
					deviceid = str(row[0])
				self.old_recipe_devices[deviceid] = 0
					
			pstuff=pickle.dumps(self.old_recipe_devices)
			
			self.saveToFile(filename, pstuff)
		"""
		
	def saveToFile(self, filename, data):
		out = open(filename,'w');
		out.write(data)
		out.flush()
				
	
	#查往期的渠道和设备列表
	def prepareForPerDate(self, delay, maps, appid):
		
		sql = "SELECT channel_id,upper(device_id) from logs.log_php_app_log WHERE logdate='%s' and %s group by channel_id,device_id"
		
		day = (self._datetime - timedelta(days = delay)).strftime("%Y-%m-%d")
		printLog("生成 %s 的渠道-设备号列表"%day)
		
		#cursor = self.shark.execute(sql%(day,appid))
		cursor = self.hive.execute(sql%(day,appid))
		for cols in cursor:
			row = re.split(r"\s+",cols)
			channel = str(row[0])
			device = str(row[1])
			if device.strip() != '' and channel.strip() != '':
				if channel not in maps:
					maps[channel] = {}
					maps[channel][delay] = []
				elif delay not in maps[channel]:
					maps[channel][delay] = []
						
				maps[channel][device] = 0
				#未来需要按每IP获取设备数用以计算日活
				maps[channel][delay].append(device)		
	
	def destroy(self):
		self.yesterdayMap = {}
		self.weekMap = {}
		self.monthMap = {}
		self.old_recipe_devices = {}

def start(delay):
	
	nowday = datetime.now()
	curDate = (nowday - timedelta(days = delay))
		
	printLog("当前预处理时间是：" + curDate.strftime("%Y-%m-%d"))
	
	dbs = DailyBeforeSearch(delay,path)
	dbs.search()
	dbs.destroy()


path  = os.path.dirname(os.path.abspath(__file__))

f = open(path+"/hbds.log",'w');

def printLog(src, ts=""):
	if ts == "":
		ts = ctime()
	print ts, src
	f.write(ts + " " + str(src) + "\n")
	f.flush()

if __name__ == '__main__':
		
	usage = "usage: %prog [options] arg1 arg2"
	parser = OptionParser(usage = usage, version="%prog 0.1")
	parser.add_option("-d", "--days", help = 'Download the log back for a few days, default value is 1, use \',\' separated, like 10,1', dest = "days")
	(options, args) = parser.parse_args()
	
	if len(sys.argv)<=1:
		parser.print_help()
		sys.exit(2)
		
	days = options.days or "1"
	delay = days.split(",")
	
	if len(delay) != 1:
		if int(delay[0]) <= int(delay[1])-1:
			printLog(delay[0] + " - " + delay[1] + " 时间区间错误")
			
		for today in range(int(delay[0]),int(delay[1])-1,-1):
			start(today)
			
	else:
		start(int(delay[0]))
		
	dc = DailyCleaner(path)
	dc.start()
