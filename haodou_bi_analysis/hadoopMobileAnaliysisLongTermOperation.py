# -*- coding: UTF-8 -*- 
#coding = utf-8
#渠道质量统计

from socket import * 
import pickle
from optparse import OptionParser
import commands
import threading
import traceback
from time import ctime
from MailUtil import *
from DBUtil import *
from XLSWriter import XLSWriter
from hiveDB import *
from  datetime  import  *
import csv, codecs
import time
import sys
import os
import re


#开始自动和后台数据库同步基础数据
class DailyBeforeUpdateThread(threading.Thread):
	
	def __init__(self, sinal=None, skipLoad=0, delay=1):
		threading.Thread.__init__(self,name='DailyBeforeUpdateThread')
		self.sinal = sinal

		self.db = DB()
		self.skipLoad = skipLoad
		self.delay = delay
		dt = datetime.now()
		self._datetime = dt - timedelta(days = self.delay)
		
		self.survivalYesterdayMap = {}	
		self.survivalWeekMap = {}
		self.survivalMonthMap = {}
		
		self.hive = HiveDB()
	
	def run(self):
		
		#while True:
			
		if self.skipLoad == 0:
			#对外发每日邮件只有非市场渠道的数据
			printLog("获取非市场渠道信息")
			sqlExt = "SELECT CONCAT(channel,'_v',`version`) FROM bi_app_channel_ext where disable=1"
			cursor = self.db.execute(sqlExt)
			self.channelExt = {}
			for row in cursor:
				channel = str(row[0])
				self.channelExt[channel] = 0
			
			#dt = datetime.now()
			#self._datetime = dt - timedelta(days = self.delay)
			
			printLog("查往期的渠道和设备列表")
				
			printLog("生成昨日留存渠道-设备号列表")
			self.survivalYesterdayMap = {}
			self.prepareForPerDate(1,self.survivalYesterdayMap)
				
			printLog("生成第7日留存渠道-设备号列表")
			self.survivalWeekMap = {}
			self.prepareForPerDate(7,self.survivalWeekMap)
			
			printLog("生成第30日留存渠道-设备号列表")
			self.survivalMonthMap = {}
			self.prepareForPerDate(30,self.survivalMonthMap)
				
			self.sinal.set()
		
		#只允许跳过一次
		#self.skipLoad = 0
		
		"""
		#在每天1点开始
		printLog("在每天5点开始")
		nowTime = datetime.now()
		tomorrow = nowTime + timedelta(days = 1)
		secDeliy = datetime(nowTime.year,nowTime.month,nowTime.day,5,0,0) - nowTime
			
		time.sleep(secDeliy.seconds)
		"""
			
	#查往期的渠道和设备列表
	def prepareForPerDate(self, delay, maps):
		
		sql = "SELECT channel_id,upper(device_id) from logs.log_php_app_log WHERE logdate='%s' and appid=2 group by channel_id,device_id"
		
		day = (self._datetime - timedelta(days = delay)).strftime("%Y-%m-%d")
		printLog("生成 %s 的渠道-设备号列表"%day)
		
		cursor = self.hive.execute(sql%day)
		
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
				maps[channel][delay].append(device)
	
	def destroy(self):		
		self.survivalYesterdayMap = {}	
		self.survivalWeekMap = {}
		self.survivalMonthMap = {}
	
	def getSurvivalYesterdayMap(self):
		return self.survivalYesterdayMap
	
	def getSurvivalWeekMap(self):
		return self.survivalWeekMap
	
	def getSurvivalMonthMap(self):
		return self.survivalMonthMap
	
	def getChannelExt(self):
		return self.channelExt		

	
class DailyAfterUpdateThread(threading.Thread):
	
	def __init__(self, ma=None, event=None, delay=1, fpath=""):
		threading.Thread.__init__(self,name='DailyAfterUpdateThread')
		self.ma = ma
		self.threadEvent = event
		self.delay = delay
		self.db = DB()
		self.db.execute("set autocommit = 1")
		#错误日志邮件发送
		self.errorMail = MailSender()
		
		dt = datetime.now()
		self._datetime = dt - timedelta(days = self.delay)
		self.curDate = self._datetime.strftime("%Y-%m-%d")
		
		self.file_path = fpath + "/" + self._datetime.strftime("%Y%m") + "/%s"
		
		if os.path.isdir(fpath + "/" + self._datetime.strftime("%Y%m")) is False:
			os.mkdir(fpath + "/" + self._datetime.strftime("%Y%m"))
		
	def run(self):
		
		self.threadEvent.wait()
		"""
		nowTime = datetime.now()
		secDeliy = datetime(nowTime.year,nowTime.month,nowTime.day,10,0,0) - nowTime
		
		time.sleep(secDeliy.seconds)
		"""
		
		self.update()
		
		#清空内存占用
		self.destroy()
		
		self.threadEvent.clear()
	
	def destroy(self):
		self.ma.destroy()
	
	#开始自动和后台数据库同步基础数据
	def update(self):
		printLog( "开始自动和后台数据库同步基础数据")
		
		#清空原始数据
		printLog("清空原始数据")
		sql = "delete from bi_app_device_startup where create_time = '%s' and appid=2"
		try:
			self.db.execute(sql%(self.curDate))
		except Exception:
			printLog("删除失败！")
			traceback.print_exc()
			
		
		#增量更新用户启动APP次数与时段分布
		printLog("增量更新用户启动APP次数与时段分布")
		sql = "INSERT INTO bi_app_device_startup (channel,startup,time_area,appid,create_time) VALUES ('%s','%s','%s','%s','%s')"
		
		timeArea = self.ma.getTimeArea()
		for channel in timeArea.keys():
			for area in timeArea[channel].keys():
				area_count = timeArea[channel][area]
				appid = 2
				try:		
					self.db.execute(sql%(channel,area_count,area,appid,self.curDate))
				except Exception:
					printLog("insert error!")
					printLog(sql%(channel,area_count,area,appid,self.curDate))
					traceback.print_exc()
					continue
					
		printLog( "增量更新老用户表")
		printLog("今日新增用户 " + str(len(self.ma.getNewUsers())))
		printLog("清空原始数据")
		self.db.execute("DELETE FROM bi_app_device_recipe where date(first_day)='%s'"%self.curDate)
		
		
		#增量更新老用户表
		increcipe = "INSERT INTO bi_app_device_recipe (device_id,os_type,first_day,appid,user_id,userip,channel,version,create_date,ios_idfa,ios_idfv) "
		increcipe = increcipe + " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');"
		
		newUsers = self.ma.getNewUsers()
		for key in newUsers.keys():
			try:		
				self.db.execute(increcipe%(newUsers[key]['deviceid'],newUsers[key]['os'],newUsers[key]['firstDay'],newUsers[key]['appid'],newUsers[key]['userId'],newUsers[key]['userIp'],newUsers[key]['pag'],newUsers[key]['var'],newUsers[key]['date'],newUsers[key]['idfa'],newUsers[key]['idfv']))
			except Exception:
				printLog("insert error!")
				errorMsg = "增量更新老用户表 错误\n"
				errorMsg += "sql语句: " + increcipe%(newUsers[key]['deviceid'],newUsers[key]['os'],newUsers[key]['firstDay'],newUsers[key]['appid'],newUsers[key]['userId'],newUsers[key]['userIp'],newUsers[key]['pag'],newUsers[key]['var'],newUsers[key]['date'],newUsers[key]['idfa'],newUsers[key]['idfv'])
				
				printLog(errorMsg)
				
				traceback.print_exc()
				continue
		
		"""				
		#增量更新第2天的用户设备号缓存
		#取出新老混合的用户设备号
		
		oldRecipeDevices = self.ma.getOldRecipeDevicesMap()
		
		data=pickle.dumps(oldRecipeDevices)
		
		dt = datetime.now()
		nextDate = dt.strftime("%Y-%m-%d")
		filename = self.file_path%("get_oldRecipeDevicesMap_" + nextDate + ".txt")
		
		#回写到第2天的用户设备号缓存
		out = open(filename,'w');
		out.write(data)
		out.flush()
		"""
				
		printLog("更新结束")
		ms = MailSender()
		ms.sendMail("zhaoweiguo@haodou.com", "好豆菜谱渠道统计监控", "增量更新用户启动APP次数与时段分布、增量更新老用户表都已更新结束")
		
		self.db.close()
	
	
class MobileAnaliysisLongTermOperation(threading.Thread):
	
	def __init__(self, before=None, fpath="", beforEvent=None, sinal=None, delay=1):
		threading.Thread.__init__(self,name='MobileAnaliysisLongTermOperation')
		self.db = DB()
		self.db.execute("""set autocommit = 1""")
		self.threadEvent = beforEvent
		self.hive = HiveDB()
		self.hive = HiveDB()
		self.delay = delay
		dt = datetime.now()
		self._datetime = dt - timedelta(days = self.delay)
		self.curDate = self._datetime.strftime("%Y%m%d")
		
		self.file_path = fpath + "/" + self._datetime.strftime("%Y%m") + "/%s"
		
		if os.path.isdir(fpath + "/" + self._datetime.strftime("%Y%m")) is False:
			os.mkdir(fpath + "/" + self._datetime.strftime("%Y%m"))
			
		self.sinal = sinal
		self.before = before
		
		self.timeArea = {}
		self.newusers = {}
		self.newUsersInfo = {}
		self.oldRecipeDevicesMap = {}
		
		self.channelExt = {}
		
		#错误日志邮件发送
		self.errorMail = MailSender()
		
		self.biAppChannelAnalysis = []
		
		self.BUFSIZE = 1024 * 2
		self.HOST = 'localhost'		
		self.PORT = 1234567
		self.ADDR = (self.HOST, self.PORT)
	
	def getTimeArea(self):
		return self.timeArea
		
	def getNewUsers(self):	
		
		return self.newUsersInfo
	
	def getOldRecipeDevicesMap(self):
		
		return self.oldRecipeDevicesMap
		
	def splitChannelName(self, channel):
		#包号
		pag = ""
		#版本号
		ver = ""
		p = re.compile('^(\w*)_v(\w*)$')
		if p.match(channel) is not None:
			ls = p.findall(channel)
			pag = ls[0][0]
			ver = ls[0][1]
			
		return (pag, ver)
		
	
	#主方法
	def run(self):
		
		#while True:
		
		self.threadEvent.wait()
		"""
		dt = datetime.now()
		self._datetime = dt - timedelta(days = 1)
		self.curDate = self._datetime.strftime("%Y%m%d")
		"""
		
		printLog("Now time is %s"%self.curDate)
		self.channelExt = self.before.getChannelExt()
		
		
		#统计原始数据
		self.startCount()
		
		#统计明细
		self.statisticSummary()
		
		#统计可能有问题的渠道
		problem = self.statisticMaybeProblemChannel()
		
		html = """\
		<html>
				<head></head>
				<body>
				%s
				</body>
		</html>
		"""

		
		#发邮件
		printLog("发邮件")
		ms = MailSender(self.file_path%("channel_"+self.curDate+".xls"), "channel_"+self.curDate+".xls")
		ms.sendMail("zhaoweiguo@haodou.com", "好豆菜谱在非市场渠道%s质量反馈"%self.curDate, html%problem)
		ms.sendMail("wenzhenfei@haodou.com", "好豆菜谱在非市场渠道%s质量反馈"%self.curDate, html%problem)
		
		#清空内存占用
		self.before.destroy()
		
		self.threadEvent.clear()
		
		self.sinal.set()
	
	#销毁内存占用
	def destroy(self):
		self.newusers = {}
		self.channelExt = {}
		self.newUsersInfo = {}
		self.biAppChannelAnalysis = []
		self.oldRecipeDevicesMap = {}
		self.timeArea = {}
	
	def getPersistentData(self, topic):
		regex_getMap = "(?P<method>get):(?P<key>.+?Map):(?P<date>[0-9]{4}-[0-9]{1,2}-[0-9]{1,2})"
		m = re.findall(regex_getMap,topic)
		for (method, key, date) in m:
			if method == "get":
				filename = self.file_path%(topic.replace(":","_").strip() + ".txt")
				
				if os.path.exists(filename):
					printLog(filename + ' 取文件...')
					files=open(filename)
					value = ""
					for chunk in files:
						value += chunk
					stuff = pickle.loads(value.strip())
					return stuff
				else:
					value = {}
					return value
	
	def getReceiveData(self, topic):
		value = self.sendDataAndReceive(topic)
		if value == "-1":
			value = {}
			return value
		else:			
			files=open(value)
			value = ""
			for chunk in files:
				value += chunk
			stuff = pickle.loads(value.strip())
			return stuff
	
	def sendDataAndReceive(self,data):
		tcpCliSock = socket(AF_INET, SOCK_STREAM)
		tcpCliSock.connect(self.ADDR)
		
		tcpCliSock.send('%s\r\n' % data)
		data = tcpCliSock.recv(self.BUFSIZE)
		if not data:
			return "-1"
		tcpCliSock.close()
		return data.strip()
		
	#统计全部的启动次数 单次使用时长 新用户数
	def startCount(self):
			
		date = self._datetime.strftime("%Y-%m-%d")
		
		sql = "select request_time,upper(device_id),channel_id,userip,appid,userid from logs.log_php_app_log where logdate='%s' and appid=2 order by request_time"
		cursor = self.hive.execute(sql%(date))
		"""
		sql = "select request_time,device_id,channel_id,userip,appid,userid from logs.log_php_app_log where logdate='%s' and request_time between '%s' and '%s' and appid=2 order by request_time"
				
		res = []		
		for i in range(0,24,1):
			
			year = int(self._datetime.strftime("%Y"))
			month = int(self._datetime.strftime("%m"))
			day = int(self._datetime.strftime("%d"))
			
			min = int(time.mktime(datetime(year,month,day,i,0,0).timetuple()))
			max = int(time.mktime(datetime(year,month,day,i,59,59).timetuple()))
			
			cursor = self.hive.execute(sql%(date,min,max))
			
			res.extend(cursor)
		"""
		#用户启动APP次数与时段分布
		self.timeArea = {}
		
		totalCount = 0
		totalUseTime = 0
		
		consUseTime = 5*60
		p = re.compile('^(\w*)_v(\w*)$')
		rowlist = {}
		for cols in cursor:
			try:
				row = re.split(r"\s+",cols)
				if len(row) != 6:			
					continue
					
				chl = row[2]
				
				#校验渠道号格式
				m = p.findall(chl)
				if not m:
					continue
				
				dv = row[1]
				et = int(row[0])
				
				et = datetime.fromtimestamp(et)
				
				if chl is None:
					key = str(dv)
				else:
					key = str(dv) + "_" + chl
				
				if key not in rowlist:
					rowlist[key] = {}
					
					if chl == "appstore":
						rowlist[key]['os'] = "ios"
					else:
						rowlist[key]['os'] = "android"
							
					rowlist[key]['appid'] = row[4]
					rowlist[key]['device'] = str(dv)
					rowlist[key]['channel'] = str(chl)
					rowlist[key]['count'] = 1
					rowlist[key]['endTime'] = et
					rowlist[key]['useTime'] = 0
					rowlist[key]['firstDay'] = et
					rowlist[key]['userId'] = row[5]
					rowlist[key]['userIp'] = row[3]
					
					totalCount += 1
					
					nowTime = et.strftime("%Y-%m-%d %H") + ":00:00"
					if chl not in self.timeArea:
						self.timeArea[chl] = {}
	
					if nowTime not in self.timeArea[chl]:
							self.timeArea[chl][nowTime] = 1
							
				elif (et-rowlist[key]['endTime']).seconds>consUseTime:
						
					rowlist[key]['count'] += 1
					totalCount += 1
					
					rowlist[key]['endTime'] = et
						
					nowTime = et.strftime("%Y-%m-%d %H") + ":00:00"
					if chl in self.timeArea:
						if nowTime in self.timeArea[chl]:
							self.timeArea[chl][nowTime] += 1	
						
				elif (et-rowlist[key]['endTime']).seconds<=consUseTime:
						
					singleUseTime = (et-rowlist[key]['endTime']).seconds
					rowlist[key]['useTime'] += singleUseTime
					totalUseTime += singleUseTime
					
					rowlist[key]['endTime'] = et
					
			except Exception:
				printLog("calculate error!",ctime())
				errorMsg = "启动次数 单次使用时长统计 错误<br />"
				errorMsg += "错误数据：" + cols
			
				printLog(errorMsg)
				traceback.print_exc()
				continue
			
		printLog(str(totalCount))
		printLog(str(int(totalUseTime/totalCount)))
			
		#sql = "INSERT INTO bi_app_channel_analysis (request_time,device,userip,channel,startup,use_time,appid) "
		#sql = sql + "VALUES ('%s','%s','%s','%s',%s,%s,%s)"
		
		#printLog( "清空用户-渠道信息")
		
		#self.db.execute("DELETE FROM bi_app_channel_analysis WHERE date(request_time)='%s' and appid = 2"%date)
		
		#channels = {}
		
		
		#新用户数统计
		self.newusers = {}
		self.newUsersInfo = {}
		self.oldRecipeDevicesMap = {}
			
		self.oldRecipeDevicesMap = self.getPersistentData("get:oldRecipeDevicesMap:" + date)
		if len(self.oldRecipeDevicesMap) == 0:
			printLog("数据没有生成，处理中...")
			printLog("获取菜谱老用户设备号")
			cursor = self.db.execute("SELECT upper(device_id) FROM bi_app_device_recipe where date(first_day) <'%s'"%date)
			for row in cursor:
				deviceid = ""
				if isinstance(row[0], str) == False:
					deviceid = row[0].encode('utf-8')
				else:
					deviceid = str(row[0])
				self.oldRecipeDevicesMap[deviceid] = 0
			
			filename = self.file_path%("get_oldRecipeDevicesMap_" + date + ".txt")
					
			pstuff=pickle.dumps(self.oldRecipeDevicesMap)
			
			out = open(filename,'w');
			out.write(pstuff)
			out.flush()
	
				
		printLog("菜谱老用户数")
		printLog(len(self.oldRecipeDevicesMap))
			
		printLog("启动次数 单次使用时长统计")
		for key in rowlist.keys():
			channel = rowlist[key]['channel']
						
			spName = self.splitChannelName(channel)
			
			deviceid = rowlist[key]['device']
			rs = deviceid.split('|')
			idfa = ""
			idfv = ""
			if len(rs)>=2:
				idfa = rs[1].upper().strip()
				if len(rs)==3:
					idfv = rs[2].upper().strip()
					
			deviceid = rs[0]
			
			item = {}
			item[0] = self.curDate
			item[1] = rowlist[key]['device']
			item[2] = rowlist[key]['userIp']
			item[3] = rowlist[key]['channel']
			item[4] = rowlist[key]['count']
			item[5] = rowlist[key]['useTime']
			item[6] = rowlist[key]['appid']
			
			self.biAppChannelAnalysis.append(item)
			
			"""
			try:
				
				self.db.execute(sql%(self.curDate,rowlist[key]['device'],rowlist[key]['userIp'],rowlist[key]['channel'],rowlist[key]['count'],rowlist[key]['useTime'],rowlist[key]['appid']))
				
			except Exception:
				printLog("insert error!",ctime())
				errorMsg = "启动次数 单次使用时长统计 入库 错误<br />"
				errorMsg += "sql语句: " + sql%(self.curDate,rowlist[key]['device'],rowlist[key]['userIp'],rowlist[key]['channel'],rowlist[key]['count'],rowlist[key]['useTime'],rowlist[key]['appid'])
				
				self.errorMail.sendMail("zhaoweiguo@haodou.com", "好豆菜谱渠道抽取%s错误日志监控"%date, errorMsg)
				
				traceback.print_exc()
				continue
			"""
			
			
			#--1:去哪吃iphone/2:菜谱安卓/3:去哪吃安卓/4:菜谱iphone/5:华为机顶盒/6:菜谱ipad
			try:
				if deviceid not in self.oldRecipeDevicesMap:
					
					appid = int(rowlist[key]['appid'])
					if appid == 2:
						if deviceid not in self.newUsersInfo:
							self.newUsersInfo[deviceid] = {}
							self.newUsersInfo[deviceid]['deviceid'] = deviceid
							self.newUsersInfo[deviceid]['os'] = rowlist[key]['os']
							self.newUsersInfo[deviceid]['firstDay'] = rowlist[key]['firstDay']
							self.newUsersInfo[deviceid]['appid'] = appid
							self.newUsersInfo[deviceid]['userId'] = rowlist[key]['userId']
							self.newUsersInfo[deviceid]['userIp'] = rowlist[key]['userIp']
							self.newUsersInfo[deviceid]['pag'] = spName[0]
							self.newUsersInfo[deviceid]['var'] = spName[1]
							self.newUsersInfo[deviceid]['date'] = date
							self.newUsersInfo[deviceid]['idfa'] = idfa
							self.newUsersInfo[deviceid]['idfv'] = idfv
					
					firstTime = rowlist[key]['firstDay'].strftime("%H") + ":00:00"
					
					#统计新用户在小时时段内的数量
					if channel not in self.newusers:
						self.newusers[channel] = {}
						self.newusers[channel]['count'] = 1
							
					if firstTime not in self.newusers[channel]:
						self.newusers[channel][firstTime] = 1
					else:
						self.newusers[channel][firstTime] = self.newusers[channel][firstTime] + 1
						self.newusers[channel]['count'] = self.newusers[channel]['count'] + 1
					
					
					self.oldRecipeDevicesMap[deviceid] = 0
					
			except Exception:
				printLog("insert error!",ctime())
				errorMsg = "统计新用户在小时时段内的数量 错误<br />"
				errorMsg += "错误数据：日期=>" + str(self.curDate) + " 设备号=>" + str(rowlist[key]['device']) + " 用户IP=>" + str(rowlist[key]['userIp']) + " 渠道=>" + str(rowlist[key]['channel']) + " 启动次数=>" + str(rowlist[key]['count']) + " 使用时长=>" + str(rowlist[key]['useTime']) + " 应用类型=>" + str(rowlist[key]['appid'])
			
				printLog(errorMsg)
				traceback.print_exc()
				continue
	
	def statisticSummary(self):
				
		today = self._datetime.strftime("%Y-%m-%d")
				
		#sql = "SELECT * FROM bi_app_channel_analysis WHERE date(request_time)='%s' and appid=2"
		
		#cursor = self.db.execute(sql%(today))
		
		channels = {}
		
		for row in self.biAppChannelAnalysis:
			channel = str(row[3])
			if channel not in channels:
				channels[channel] = {}
				#使用时长
				channels[channel]['use_time'] = int(row[5])
				#启动次数
				channels[channel]['startup'] = int(row[4])
				#设备列表
				#未来需要按每IP获取设备数用以计算日活
				#userip = str(row[2])
				channels[channel]['devices'] = []
				channels[channel]['devices'].append(str(row[1]))
				
				#活跃用户数	
				channels[channel]['active_user'] = 1
			else:
				
				channels[channel]['use_time'] += int(row[5])
		
				channels[channel]['active_user'] += 1
				channels[channel]['startup'] += int(row[4])
			
			channels[channel]['devices'].append(str(row[1]))
			channels[channel]['appid'] = str(row[6])
								
				
		insertSql = "INSERT INTO bi_app_channel_summary (request_time,channel,new_user,increment_time,startup,use_time,active_user,seven_active_user,thirty_active_user,survival_probability,seven_survival_probability,thirty_survival_probability,uninstall,appid) "
		insertSql = insertSql + "VALUES ('%s','%s',%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
		
			
		#获取渠道卸载用户数
		printLog("获取渠道卸载用户数")
		uninstalls = {}
		while True:
			uninstalls = self.getPersistentData("get:recipeChannelUninstallMap:" + today)
			if len(uninstalls) == 0:
				printLog("数据没有生成，等待中...")
				time.sleep(10)
				continue
			else:
				break
		
		#写文件
		xlswriter = XLSWriter(self.file_path%("channel_"+self.curDate+".xls"))
		
		xlswriter.writerow(["日期","当日新增用户","当日增量最多时间","包号","版本","昨日活跃用户","7天活跃用户","过去30天活跃用户","次日留存率","7天留存率","30天留存率","昨日卸载数","启动次数","单次使用时长"], sheet_name=u'渠道信息')
		
		printLog("清空旧的原始数据")
		self.db.execute("DELETE FROM bi_app_channel_summary WHERE request_time='%s 00:00:00' and appid=2"%today)
		
		printLog( "生成昨日渠道明细表")
		
		yesterdayMap = {}
		
		while True:
			yesterdayMap = self.getPersistentData("get:yesterdayMap:" + today)
			if len(yesterdayMap) == 0:
				printLog("数据没有生成，等待中...")
				time.sleep(10)
				continue
			else:
				break
		
		weekMap = {}
		
		while True:
			weekMap = self.getPersistentData("get:weekMap:" + today)
			if len(weekMap) == 0:
				printLog("数据没有生成，等待中...")
				time.sleep(10)
				continue
			else:
				break
				
		monthMap = {}
		
		while True:
			monthMap = self.getPersistentData("get:monthMap:" + today)
			if len(monthMap) == 0:
				printLog("数据没有生成，等待中...")
				time.sleep(10)
				continue
			else:
				break
				
		survivalYesterdayMap = self.before.getSurvivalYesterdayMap()
		survivalWeekMap = self.before.getSurvivalWeekMap()
		survivalMonthMap = self.before.getSurvivalMonthMap()
		
		for channel in channels:
			
			spName = self.splitChannelName(channel)
					
			#应用ID
			appid = channels[channel]['appid']		
			
			startup = channels[channel]['startup']
			use_time = channels[channel]['use_time']
			
			#昨日活跃用户数
			active_user = channels[channel]['active_user']
			
			#当日新增用户数
			new_user = 0
			if channel in self.newusers:
				new_user = self.newusers[channel]['count']
			
			#当日增量最多时间
			increment_time = "00:00:00"
			increment_time_count = 0
			
			if channel in self.newusers:
				for c_t in self.newusers[channel].keys():
					if c_t != "count":
						if increment_time_count <= self.newusers[channel][c_t]:
							increment_time_count = self.newusers[channel][c_t]
							increment_time = c_t
			
			
			
			#7天活跃用户率 
			min_active_user = 0
			for day in range(1,8,1):
				if channel in weekMap:
					if day in weekMap[channel]:
						min_active_user = min_active_user + len(weekMap[channel][day])
			if min_active_user >0:
				min_active_user = round(float(active_user)/float(active_user + min_active_user)*100,2)

					
			#30天活跃用户率
			max_active_user = 0
			for day in range(1,31,1):
				if channel in monthMap:
					if day in monthMap[channel]:
						max_active_user = max_active_user + len(monthMap[channel][day])
			if max_active_user >0:
				max_active_user = round(float(active_user)/float(active_user + max_active_user)*100,2)
			
			todayMap = dict(zip(channels[channel]['devices'], range(active_user)))
			
			#昨日渠道留存率
			sp = 0.0
			if channel in survivalYesterdayMap:
				sp = self.survivalProbability(todayMap,survivalYesterdayMap[channel])
			
			#第7日留存率
			minsp = 0.0
			if channel in survivalWeekMap:
				minsp = self.survivalProbability(todayMap,survivalWeekMap[channel])
			
			#第30日留存率
			maxsp = 0.0
			if channel in survivalMonthMap:
				maxsp = self.survivalProbability(todayMap,survivalMonthMap[channel])
			
			#昨日卸载用户数(率？)
			#uninstall_rate = 0
			uninstall_user = 0
			if channel in uninstalls:
				uninstall_user = uninstalls[channel]
				#if active_user !=0:  (率？)
				#		uninstall_rate = int(uninstall_user/float(active_user)*100)
			
			
			try:
				self.db.execute(insertSql%(today,channel,new_user,increment_time,startup,use_time,active_user,min_active_user,max_active_user,sp,minsp,maxsp,uninstall_user,2))
				
				if channel in self.channelExt:
					xlswriter.writerow([self.curDate, new_user, increment_time, spName[0], spName[1], str(active_user),str(min_active_user)+"%",str(max_active_user)+"%",str(sp)+"%",str(minsp)+"%",str(maxsp)+"%",str(uninstall_user),str(startup),str(int(use_time/startup))], sheet_name=u'渠道信息')
					print self.curDate, new_user, increment_time, spName[0], spName[1], str(active_user),str(min_active_user)+"%",str(max_active_user)+"%",str(sp)+"%",str(minsp)+"%",str(maxsp)+"%",str(uninstall_user),str(startup),str(int(use_time/startup))
				
			except Exception:
				
				printLog("insert error!",ctime())
				errorMsg = "入库统计结果数据 错误<br />"
				errorMsg += "错误数据：日期=>" + str(today) + " 渠道=>" + str(channel) + " 新增用户数=>" + str(new_user) + " 最多增量时间=>" + str(increment_time) + " 启动次数=>" + str(startup) + " 使用时长=>" + str(use_time) + " 用户活跃数=>" + str(active_user) + " 7天活跃用户=>" + str(min_active_user) + " 30天活跃用户=>" + str(max_active_user) + " 次日留存率=>" + str(sp) + " 7天留存率=>" + str(minsp) + " 30天留存率=>" + str(maxsp) + " 昨日卸载数=>" + str(uninstall_user) + " 应用类型=>" + str(appid)
			
				printLog(errorMsg)
				traceback.print_exc()
				continue
				
		xlswriter.save()
		
	#计算留存率
	def survivalProbability(self, todayMap = {}, yesterdayMap={}):
		
		today = len(todayMap)
		
		live = {}
		
		for device in yesterdayMap:
			if device in todayMap:
				live[device] = 0
		
		lives = len(live)
		yesterdays = len(yesterdayMap)
		sp = round((float(lives) / float(yesterdays))*100,2)
		return sp
	
	def statisticMaybeProblemChannel(self):
			
		today = self._datetime.strftime("%Y-%m-%d")
		
		sql = "SELECT t.* FROM( SELECT channel, SUM(startup) startup ,SUM(use_time)/SUM(startup) use_time FROM bi_app_channel_summary WHERE request_time='%s 00:00:00' " 
		sql = sql + "GROUP BY channel ORDER BY use_time,startup DESC ) as t where t.use_time<=100"
		
		cursor = self.db.execute(sql%(today))
		
		totalStartup = 0
		channels = {}
		for row in cursor:
			channel = str(row[0])
			if channel not in channels:
				channels[channel] = {}
			
			channels[channel]['startup'] = int(row[1])
			channels[channel]['use_time'] = int(row[2])
			
			totalStartup = totalStartup + channels[channel]['startup']
		
		
		content = ""
		
		for channel in channels:
				
			rate = channels[channel]['startup'] / float(totalStartup)
			channels[channel]['rate'] = rate
			
			if rate >= 0.13:
				content = content + "渠道 " + channel + " 启动次数占比偏高 达到了 <font color=\'red\'>" + str(int(rate*100)) + "%   " + str(channels[channel]['startup']) + "</font>次 单次使用时长为 <font color=\'red\'>" + str(channels[channel]['use_time']) + "</font> 秒<br>\r\n"
		
		if content != "":
			content = "<font color=\'blue\'>渠道报警监控</font><br>时间 " + str(today) + "<br>" + content
		
		#生成全渠道留存率时长数据
		printLog( "生成全渠道留存率时长数据")
		
		sql = "SELECT channel, survival_probability, seven_survival_probability,SUM(use_time)/SUM(startup) use_time FROM bi_app_channel_summary WHERE request_time='%s 00:00:00' GROUP BY channel"
		cursor = self.db.execute(sql%(today))
		
		channelContent = ""
		for row in cursor:
			channel = str(row[0])
			if channel in self.channelExt:
				channelContent = channelContent + "渠道 " + str(row[0]) + " 次日留存率 <font color=\'red\'>" + str(row[1]) + "%</font>  7天留存率 <font color=\'red\'>" + str(row[2]) + "%</font> 单次使用时长 <font color=\'red\'>" + str(int(row[3])) + "</font> 秒<br>\r\n"
						
		if channelContent != "":
			channelContent = "<font color=\'blue\'>全渠道 留存率 时长数据</font><br>" + channelContent
		
		content = content + channelContent
		
		
		#统计单次使用时长低于2分钟且次日留存率小于20%
		
		sql = "SELECT channel, survival_probability, use_time/startup use_time FROM bi_app_channel_summary "
		sql = sql + "WHERE use_time <120 AND survival_probability <20 AND request_time='%s 00:00:00' GROUP BY channel"
		
		cursor = self.db.execute(sql%(today))
		
		
		contentSP = ""
		
		for row in cursor:
			channel = str(row[0])
			if channel in self.channelExt:
				contentSP = contentSP + "渠道 " + channel + " 次日留存率是 <font color=\'red\'>" + str(row[1]) + "%</font> 单次使用时长为 <font color=\'red\'>" + str(int(row[2])) + "</font> 秒<br>\r\n"
		
		if contentSP != "":
			contentSP = "<font color=\'blue\'>非市场渠道单次使用时长低于2分钟且次日留存率小于20%</font><br>"  + contentSP
		
		content = content + contentSP
		
		return content

def printLog(src, ts=""):
	if ts == "":
		ts = ctime()
	print ts, src
	f.write(ts + " " + str(src) + "\n")
	f.flush()

def start(delay):
	beforeSinal = threading.Event()
	before = DailyBeforeUpdateThread(beforeSinal, skip, delay)
	before.start()
	
	afterSinal = threading.Event()
	ma = MobileAnaliysisLongTermOperation(before, path, beforeSinal, afterSinal, delay)
	ma.start()
	
	after = DailyAfterUpdateThread(ma, afterSinal, delay, path)
	after.start()

path  = os.path.dirname(os.path.abspath(__file__))

if __name__ == '__main__':
	
	f = open(path+"/hmalto.log",'w');
	
	usage = "usage: %prog [options] arg1 arg2"
	parser = OptionParser(usage = usage, version="%prog 0.1")
	parser.add_option("-s", "--skip", help = 'Skip data loading', dest = "skip")
	parser.add_option("-d", "--days", help = 'Download the log back for a few days, default value is 1, use \',\' separated, like 10,1', dest = "days")
	(options, args) = parser.parse_args()

	if len(sys.argv)<=1:
		parser.print_help()
		sys.exit(2)
	
	skip = options.skip or 0
	days = options.days or "1"
	delay = days.split(",")
	
	if len(delay) != 1:
		if int(delay[0]) <= int(delay[1])-1:
			printLog(delay[0] + " - " + delay[1] + " 时间区间错误")
			
		for today in range(int(delay[0]),int(delay[1])-1,-1):
			start(today)
			
	else:
		start(int(delay[0]))
