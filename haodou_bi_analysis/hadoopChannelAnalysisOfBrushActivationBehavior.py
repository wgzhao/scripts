# -*- coding: UTF-8 -*- 
#coding = utf-8
#从2014年9月11日每隔3天严查一次安卓渠道

from optparse import OptionParser
from operator import add
from time import ctime
from MailUtil import *
from XLSWriter import XLSWriter
from sharkDB import *
from hiveDB import *
from timeUtils import *
from  datetime import *
from urllib import *
import traceback
import commands
import pickle
import base64
import time
import rsa
import sys
import os
import re


class ChannelUninstallAnalysis:
	
	def __init__(self, fpath="", delay=1):
		
		self.delay = delay
		
		dt = datetime.now()
		self._datetime = dt - timedelta(days = self.delay)
		self.curDate = self._datetime.strftime("%Y-%m-%d")
		
		self.path = fpath
		self.hive = HiveDB()
		#self.shark = SharkDB()
		
		with open(self.path + '/rsa-transform-key.txt' , 'rb') as privatefile:
			p = privatefile.read()
			self.privkey = rsa.PrivateKey.load_pkcs1(p)
		
		self.regex_url = r'^.*uninstall.php?(?P<url>.+?)&time.*'
		self.regex = r'^.*&encode=(?P<crypto>.+).*$'
		self.regex_channel = r'^.*channel=(?P<chl>.+?)&vc=.*$'
		self.regex_deviceid = r'^.*toten=(?P<did>.+?)&uuid=.*'
	
	#主方法
	def start(self):
		channels = {}
		
		mindate = self._datetime - timedelta(days = (self.delay+2))
		mindate = mindate.strftime("%Y-%m-%d")
		maxdate = self._datetime - timedelta(days = (self.delay))
		maxdate = maxdate.strftime("%Y-%m-%d")
		
		#取前3天的渠道卸载用户
		print ctime(),"取前3天的渠道卸载用户"
		
		sql = "select referer from m_haodou_com where referer like '%uninstall.php%' and logdate between '" + mindate + "' and '" + maxdate + "'"
		
		#res = self.shark.execute(sql)
		res = self.hive.execute(sql)
		for chunk in res:
			m = re.findall(self.regex_url, chunk)
			for (url) in m:
				url = unquote(url)
				m = re.findall(self.regex, url, re.S)
				try:
					for (crypto) in m:
						crypto = base64.b64decode(crypto)
						message = rsa.decrypt(crypto, self.privkey)
						
						deviceid = ""
						m = re.findall(self.regex_deviceid, url)
						for (did) in m:
							deviceid = did
						p = re.compile('^(\w*)_v(\w*)$')
						m = re.findall(self.regex_channel, message)
						for (chl) in m:
							#校验渠道号格式
							m = p.findall(chl)
							if not m:
								continue
							if chl not in channels:
								channels[chl] = {}
							channels[chl][deviceid] = 1
								
				except:
					print chunk
					traceback.print_exc()
					continue
		return channels

class ChannelAnalysisOfBrushActivationBehavior:
	
	def __init__(self, fpath, delay=1):
		self.delay = delay
		dt = datetime.now()
		self._datetime = dt - timedelta(days = delay)
		self.curDate = self._datetime.strftime("%Y-%m-%d")
		self.file_path = fpath + "/" + self._datetime.strftime("%Y%m") + "/"
		self.tu = TimeUtils()
		self.shortDate = dt.strftime("%Y%m%d")
		self.path = fpath
		
		if os.path.isdir(self.file_path) is False:
			os.mkdir(self.file_path)
		
		self.hive = HiveDB()
		#self.shark = SharkDB()
		self.originalChannelMap = {}
		
		#用来检查的渠道版本号
		self.check_version = "4.";
	
	def saveToFile(self, filename, data):
		out = open(filename,'w');
		out.write(data)
		out.flush()


	def getPersistentData(self, topic):
		regex_getMap = "(?P<method>get):(?P<key>.+?Map):(?P<date>[0-9]{4}-[0-9]{1,2}-[0-9]{1,2})"
		m = re.findall(regex_getMap,topic)
		for (method, key, date) in m:
			if method == "get":
				filename = self.file_path + topic.replace(":","_").strip() + ".txt"
				
				if os.path.exists(filename):
					print ctime(),filename + ' 取文件...'
					files=open(filename)
					value = ""
					for chunk in files:
						value += chunk
					stuff = pickle.loads(value.strip())
					return stuff
				else:
					value = {}
					return value
	
		
	#计算渠道留存率
	def retentionRate(self,original,left):
		liveUsers = len(left)
		newUsers = len(original)
		if newUsers !=0:
			sp = round((float(liveUsers) / float(newUsers))*100,2)
			original["retentionRate"] = sp
		else:
			original["retentionRate"] = 0.00
	
	#在各渠道生成减掉卸载用户后的存活用户
	def survivalProbability(self, channel, leftUsers, original, uninstall):
		
		for deviceid in original:
			if deviceid not in uninstall:
				if channel not in leftUsers:
					leftUsers[channel] = {}
				leftUsers[channel][deviceid] = 1
		
		return leftUsers
		
					
	def start(self):
		
		filename = self.file_path+"channelAnalysisOfBrushActivationBehavior_three_day_one_run.txt"
		if os.path.exists(filename) == True:
			pstuff=pickle.dumps(self._datetime)
			self.saveToFile(filename, pstuff)
			
			files=open(filename)
			value = ""
			for chunk in files:
				value += chunk
			prevDatetime = pickle.loads(value.strip())
			if (self._datetime - prevDatetime).days < 4:
				print "当前时间",prevDatetime,"未达4天退出"
				sys.exit(2)
			else:
				print "当前时间",prevDatetime,"已达4天，分析开始执行"
				pstuff=pickle.dumps(self._datetime)
				self.saveToFile(filename, pstuff)
		else:
			print "当前时间",self._datetime.strftime("%Y-%m-%d"),"已达4天，分析开始执行"
			pstuff=pickle.dumps(self._datetime)
			self.saveToFile(filename, pstuff)
		
		mindate = self._datetime - timedelta(days = (self.delay+2))
		mindate = mindate.strftime("%Y-%m-%d")
		maxdate = self._datetime - timedelta(days = (self.delay))
		maxdate = maxdate.strftime("%Y-%m-%d")
		
		sql = """\
		select first_channel,device_id from bing.dw_app_device_ds
		where app_id=2 and to_date(first_day) between '""" + mindate + """' and '""" + maxdate + """' and first_version like '""" + self.check_version + """%';
		"""
		
		self.originalChannelMap = {}
		p = re.compile('^(\w*)_v(\w*)$')
		#前取3天的菜谱安卓新增用户
		print ctime(),"前取3天的菜谱安卓新增用户"
		#cursor = self.shark.execute(sql%(mindate,maxdate))
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			channel = str(row[0])
			#校验渠道号格式
			m = p.findall(channel)
			if not m:
				continue
			
			deviceid = str(row[1])
			if channel not in self.originalChannelMap:
				self.originalChannelMap[channel] = {}
			self.originalChannelMap[channel][deviceid] = 1
			
		#写文件
		xlswriter = XLSWriter(self.file_path+"channelAnalysisOfBrushActivationBehavior_"+self.curDate+".xls")
		
		problem = self.getRetentionRate(xlswriter)
		problem += "<hr>\n"
		problem += "<br>\n"
		
		problem += self.getSurvivalProbability(xlswriter)
		problem += "<hr>\n"
		problem += "<br>\n"
		
		problem += self.getUserRegister(xlswriter)
		
		self.getMobileType(xlswriter)
		"""
		problem = self.getMobileType(xlswriter)
		"""
		xlswriter.save()
		
		os.popen('zip -j -r '+self.file_path+'channelAnalysisOfBrushActivationBehavior_'+self.curDate+'.zip '+self.file_path+'channelAnalysisOfBrushActivationBehavior_'+self.curDate+'.xls')
		
		html = """\
		<html>
				<head></head>
				<body>
				%s
				</body>
		</html>
		"""
		
		#发邮件
		print ctime(),"发邮件"
		ms = MailSender(self.file_path+"channelAnalysisOfBrushActivationBehavior_"+self.curDate+".zip", "安卓渠道刷量抽查_"+self.curDate+".zip")
		ms.sendMail("zhaoweiguo@haodou.com", "安卓渠道%s数据质量分析报告"%self.curDate, html%problem)
		ms.sendMail("wenzhenfei@haodou.com", "安卓渠道%s数据质量分析报告"%self.curDate, html%problem)
		
		os.popen('rm -f '+self.file_path+'channelAnalysisOfBrushActivationBehavior_'+self.curDate+'.xls')
		
	def getRetentionRate(self,xlswriter):
		#留存率
		#3天前菜谱渠道新增的用户，在第4天留下了多少用户还在活跃，这批留下的用户除以3天之内所产生的菜谱渠道新增用户的比率就是新增用户的留存率
		mindate = self._datetime - timedelta(days = (self.delay+2))
		mindate = mindate.strftime("%Y-%m-%d")
		maxdate = self._datetime - timedelta(days = (self.delay))
		maxdate = maxdate.strftime("%Y-%m-%d")
		
		leftChannelMap = {}
		
		problem = '<table border="1" cellspacing="0" width="30%">\n'
		
		sql = """\
		select ds.first_channel,ds.device_id from bing.dw_app_device_ds ds
		inner join logs.log_php_app_log lg on lg.device_id=ds.device_id
		where ds.app_id=2 and to_date(ds.first_day) between '%s' and '%s'
		and lg.appid=2 and lg.logdate='%s'
		group by ds.first_channel,ds.device_id;
		"""
		p = re.compile('^(\w*)_v(\w*)$')
		#在第4天留下了多少用户还在活跃
		print ctime(),"在第4天留下了多少用户还在活跃"
		#cursor = self.shark.execute(sql%(mindate,maxdate,self.curDate))
		cursor = self.hive.execute(sql%(mindate,maxdate,self.curDate))
		for cols in cursor:
			row = re.split(r"\s+",cols)
			channel = str(row[0])
			#校验渠道号格式
			m = p.findall(channel)
			if not m:
				continue
			
			deviceid = str(row[1])
			if channel not in leftChannelMap:
				leftChannelMap[channel] = {}
			leftChannelMap[channel][deviceid] = 1
		
		xlswriter.writerow(["日期","渠道","留存率"], sheet_name=u'留存率信息')
		problem += '<tr>\n<td colspan="3" align="center" align="center"><b style="color:#FF0000">留存率(疑似)异常渠道(低于5%)</b></td>\n</tr>\n'
		problem += '<tr>\n<td>日期</td>\n<td>渠道</td>\n<td>留存率</td>\n</tr>\n'
		#开始比较
		print ctime(),"开始比较"
		for channel in self.originalChannelMap:
			sp = 0
			if channel in leftChannelMap:
				self.retentionRate(self.originalChannelMap[channel],leftChannelMap[channel])
				sp = self.originalChannelMap[channel]["retentionRate"]
				self.originalChannelMap[channel]["retentionRate"] = 0
			
				xlswriter.writerow([self.curDate,channel,str(sp)+"%"], sheet_name=u'留存率信息')
			if sp <= 5:
				problem += '<tr>\n<td>%s</td>\n<td>%s</td>\n<td>%s</td>\n</tr>\n'%(self.curDate,channel,str(sp)+"%")
		
		problem += '</table>\n'
		
		return problem
			
	def getSurvivalProbability(self,xlswriter):
		mindate = self._datetime - timedelta(days = (self.delay+2))
		mindate = mindate.strftime("%Y-%m-%d")
		maxdate = self._datetime - timedelta(days = (self.delay))
		maxdate = maxdate.strftime("%Y-%m-%d")
		
		#存活率
		#取3天前菜谱渠道新增用户，在第4天后取前3天的卸载用户，减掉这批卸载用户之后除以3天前的菜谱渠道新增用户的比率就是APP的存活率
		print ctime(),"APP存活率"
		uninstalls = self.getPersistentData("get:channelUninstallMap:" + self.curDate)
		if len(uninstalls) == 0:
			print ctime(),"数据没有生成，处理中..."
		
			filename = self.file_path + "get_channelUninstallMap_" + self.curDate + ".txt"		
			cua = ChannelUninstallAnalysis(self.path, self.delay)
			uninstalls = cua.start()
			pstuff=pickle.dumps(uninstalls)
			self.saveToFile(filename, pstuff)
		
		problem = '<table border="1" cellspacing="0" width="30%">\n'
		leftUsers = {}
		xlswriter.writerow(["日期","渠道","APP存活率"], sheet_name=u'存活率信息')
		problem += '<tr>\n<td colspan="3" align="center"><b style="color:#FF0000">APP存活率(疑似)异常渠道(低于15%)</b></td>\n</tr>\n'
		problem += '<tr>\n<td>日期</td>\n<td>渠道</td>\n<td>APP存活率</td>\n</tr>\n'
		print ctime(),"开始比较"
		for channel in self.originalChannelMap:
			sp = 0
			if channel in uninstalls: #找有卸载记录的渠道
				self.survivalProbability(channel, leftUsers, self.originalChannelMap[channel], uninstalls[channel])
				if len(self.originalChannelMap[channel]) > 0 and channel in leftUsers:
					sp = round((float(len(leftUsers[channel])) / float(len(self.originalChannelMap[channel])))*100,2)
					#print self.curDate,channel,sp
					xlswriter.writerow([self.curDate,channel,str(sp)+"%"], sheet_name=u'存活率信息')
			else:
				sp = 100
				xlswriter.writerow([self.curDate,channel,str(100)+"%"], sheet_name=u'存活率信息')
			if sp <= 15:
				problem += '<tr>\n<td>%s</td>\n<td>%s</td>\n<td>%s</td>\n</tr>\n'%(self.curDate,channel,str(sp)+"%")
		
		problem += '</table>\n'
		
		return problem
		
	def getUserRegister(self,xlswriter):
		mindate = self._datetime - timedelta(days = (self.delay+2))
		mindate = mindate.strftime("%Y-%m-%d")
		maxdate = self._datetime - timedelta(days = (self.delay))
		maxdate = maxdate.strftime("%Y-%m-%d")
		
		#注册率
		#取3天前菜谱渠道的新增用户，在第4天统计有多少用户注册到网站除以某渠道有多少新增用户的数
		
		sql = """\
		select ds.first_channel,count(distinct(lg.userid)) cnt from bing.dw_app_device_ds ds
		inner join logs.log_php_app_log lg on lg.device_id=ds.device_id
		where ds.app_id=2 and to_date(ds.first_day) between '%s' and '%s'
		and lg.appid=2 and lg.logdate between '%s' and '%s' and lg.userid <> 0
		group by ds.first_channel
		order by cnt desc;
		"""
		
		problem = '<table border="1" cellspacing="0" width="30%">\n'
		problem += '<tr>\n<td colspan="3" align="center"><b style="color:#FF0000">注册率(疑似)异常渠道(低于0.5%)</b></td>\n</tr>\n'
		problem += '<tr>\n<td>日期</td>\n<td>渠道</td>\n<td>注册率</td>\n</tr>\n'
		xlswriter.writerow(["日期","渠道","注册率"], sheet_name=u'注册率信息')
		p = re.compile('^(\w*)_v(\w*)$')
		print ctime(),"在活跃表里面找有多少渠道带来的用户已经注册到网站"
		#cursor = self.shark.execute(sql%(mindate,maxdate,mindate,self.curDate))
		cursor = self.hive.execute(sql%(mindate,maxdate,mindate,self.curDate))
		for cols in cursor:
			row = re.split(r"\s+",cols)
			channel = str(row[0])
			#校验渠道号格式
			m = p.findall(channel)
			if not m:
				continue
			cnt = float(row[1])
			#print channel,cnt
			sp = 0
			if channel in	self.originalChannelMap:
				sp = round((cnt / float(len(self.originalChannelMap[channel])))*100,2)
				xlswriter.writerow([self.curDate,channel,str(sp)+"%"], sheet_name=u'注册率信息')
				if sp <= 0.5:
					problem += '<tr>\n<td>%s</td>\n<td>%s</td>\n<td>%s</td>\n</tr>\n'%(self.curDate,channel,str(sp)+"%")
			
		problem += '</table>\n'
		
		return problem
			
		#渠道带来用户对软件使用程度
		#渠道带来用户使用软件的路径图，相当于漏斗模型
	def getAccessDepthOfSiteMap(self,xlswriter):
		
		print 1
		
		return ""
		
	def getMobileType(self,xlswriter):	
		mindate = self._datetime - timedelta(days = (self.delay+2))
		mindate = mindate.strftime("%Y-%m-%d")
		maxdate = self._datetime - timedelta(days = (self.delay))
		maxdate = maxdate.strftime("%Y-%m-%d")
		#渠道带过来的机型
		
		sql = """\
		select regexp_extract(path,'&channel=(.+?)&method=') channel,gmtfhua(http_user_agent) mobiletype
		from api_haodou_com where logdate between '""" + mindate + """' and '""" + maxdate + """' and path like '%appid=2%'
		and gmtfhua(http_user_agent) is not null and path like '%vn=""" + self.check_version + """%'
		group by regexp_extract(path,'&channel=(.+?)&method='),gmtfhua(http_user_agent)
		order by channel desc,mobiletype;
		"""
		
		#problem = '<table border="1" cellspacing="0" width="30%">\n'
		#problem += '<tr>\n<td>日期</td>\n<td>渠道</td>\n<td>设备信息</td>\n</tr>\n'
		xlswriter.writerow(["日期","渠道","设备信息"], sheet_name=u'设备机型信息')
		p = re.compile('^(\w*)_v(\w*)$')
		print ctime(),"获取渠道设备类型"
		#cursor = self.shark.execute(sql)
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			channel = str(row[0])
			#校验渠道号格式
			m = p.findall(channel)
			if not m:
				continue
			mobiletype = str(row[1])
			print channel,unquote(mobiletype)
			xlswriter.writerow([self.curDate,channel,unquote(mobiletype)], sheet_name=u'设备机型信息')	
			#problem += '<tr>\n<td>%s</td>\n<td>%s</td>\n<td>%s</td>\n</tr>\n'%(self.curDate,channel,unquote(mobiletype))
			
		#problem += '</table>\n'
		
		#return problem
		
		
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
	
	rmpds = ChannelAnalysisOfBrushActivationBehavior(path,delay)
	rmpds.start()
