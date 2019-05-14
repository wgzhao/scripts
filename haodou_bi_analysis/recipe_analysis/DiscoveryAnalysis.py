# -*- coding: UTF-8 -*- 
#coding = utf-8

import sys
sys.path.append("/home/likunjian/")
from optparse import OptionParser
from DBUtil import *
from hiveDB import *
from sparkDB import *
from datetime import *
from time import ctime
import time
import json
import urllib2
import commands
import os
import re



class DiscoveryAnalysis:
	
	def __init__(self, delay):
		self.delay = delay
		self.db = DB()		
		self.db.execute("""set autocommit = 1""")
		#self.db.close();
		
		self.hive = HiveDB()
		#self.spark = SparkDB()
		
		dt = datetime.now()
		
		self.nowDate = (dt - timedelta(days = self.delay - 1)).strftime("%Y%m%d")
		self.curDate = (dt - timedelta(days = self.delay)).strftime("%Y-%m-%d")
		
	def startExtract(self):
		
		#发现页面广告
		sql = "delete from bi_recipe_discovery_summary WHERE createtime='%s' and type = 0"
		self.db.execute(sql%self.curDate)
		
			
		sql = "select gtifurl(url) from rcp_haodou_mobile_%s.discovery where status=1 and url <> 'null' and gtifurl(url) <> 0;";
		
		topics = ""
		
		cursor = self.hive.execute(sql%self.nowDate)
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				topicid = str(row[0])
				if "" != topicid:
					topics += topicid + ","
			
			if "" != topics:
				topics = topics[0:len(topics)-1]
		
		
				sql = """\
				select logdate,-1,count(m.path) cnt, count(distinct(concat(remote_addr,'-',http_user_agent)))
				from logs.m_haodou_com m
				where logdate = '%s' and gtifurl(m.path) in (%s)
				group by logdate;
				"""
				
				cursor = self.hive.execute(sql%(self.curDate,topics))
				if cursor is not None:
					for res in cursor:
						row = re.split(r"\s+",res)
						if len(row) == 4:
							logdate = str(row[0])
							appid = int(row[1])
							cnt = int(row[2])
							pnt = int(row[3])
						
							sql = "INSERT INTO bi_recipe_discovery_summary (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,0)"
							self.db.execute(sql%(logdate,appid,cnt,pnt))
		
		#发现页面浏览量
		sql = "delete from bi_recipe_discovery_summary WHERE createtime='%s' and type = 1"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select logdate,appid,sum(cnt),sum(pnt) from (
		select logdate,appid,count(1) cnt, 0 as pnt from logs.log_php_app_log 
		where function_id='recipe.getfindrecipe'
		and logdate = '%s'
		and appid in (2,4)
		group by logdate,appid
		union all 
		select logdate,appid,0 as cnt, count(distinct(device_id)) pnt from logs.log_php_app_log 
		where function_id='recipe.getfindrecipe'
		and logdate = '%s'
		and appid in (2,4)
		group by logdate,appid
		) tmp
		group by logdate,appid;
		"""
		
		cursor = self.hive.execute(sql%(self.curDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					logdate = str(row[0])
					appid = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_recipe_discovery_summary (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,1)"
					self.db.execute(sql%(logdate,appid,cnt,pnt))
		
		
		#早午晚餐
		
		sql = "delete from bi_recipe_discovery_summary WHERE createtime='%s' and type = 2"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select logdate,appid,sum(cnt),sum(pnt) from (
		select logdate,appid,count(device_id) cnt, 0 as pnt from logs.log_php_app_log 
		where function_id='recipephoto.getproducts'
		and gmifp(parameter_desc) in (1,2,3)
		and gtifp(parameter_desc)=1
		and logdate = '%s'
		and appid in (2,4)
		group by logdate,appid
		union all 
		select logdate,appid,0 as cnt, count(distinct(device_id)) pnt from logs.log_php_app_log 
		where function_id='recipephoto.getproducts'
		and gmifp(parameter_desc) in (1,2,3)
		and gtifp(parameter_desc)=1
		and logdate = '%s'
		and appid in (2,4)
		group by logdate,appid
		) tmp
		group by logdate,appid;
		"""
		
		cursor = self.hive.execute(sql%(self.curDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					logdate = str(row[0])
					appid = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_recipe_discovery_summary (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,2)"
					self.db.execute(sql%(logdate,appid,cnt,pnt))
		
		#新手课堂
		
		sql = "delete from bi_recipe_discovery_summary WHERE createtime='%s' and type = 3"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select logdate,appid,sum(cnt),sum(pnt) from (
		select logdate,appid,count(device_id) cnt, 0 as pnt from logs.log_php_app_log 
		where function_id='recipephoto.getproducts'
		and gtifp(parameter_desc)=2
		and logdate = '%s'
		and appid in (2,4)
		group by logdate,appid
		union all 
		select logdate,appid,0 as cnt, count(distinct(device_id)) pnt from logs.log_php_app_log 
		where function_id='recipephoto.getproducts'
		and gtifp(parameter_desc)=2
		and logdate = '%s'
		and appid in (2,4)
		group by logdate,appid
		) tmp
		group by logdate,appid;
		"""
		
		ncr = {}
		
		cursor = self.hive.execute(sql%(self.curDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					logdate = str(row[0])
					appid = str(row[1])	
					key = logdate+"_"+appid
					if key not in ncr:
						ncr[key] = {}
					
					ncr[key]["logdate"] = str(row[0])
					ncr[key]["appid"] = int(row[1])
					ncr[key]["cnt"] = int(row[2])
					ncr[key]["pnt"] = int(row[3])
					
					
		
		#叠加wap页的访问量数据
		sql = """\
		select logdate,gaidfua(http_user_agent),count(1),count(distinct(concat(remote_addr,'-',http_user_agent)))
		from m_haodou_com where gaidfua(http_user_agent)>0 and path='/app/recipe/act/novice.php' and logdate='%s' 
		group by logdate,gaidfua(http_user_agent);
		"""
		cursor = self.hive.execute(sql%(self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					logdate = str(row[0])
					appid = str(row[1])
					
					cnt = ncr[logdate+"_"+appid]["cnt"] + int(row[2])
					pnt = ncr[logdate+"_"+appid]["pnt"] + int(row[3])
					
					sql = "INSERT INTO bi_recipe_discovery_summary (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,3)"
					self.db.execute(sql%(logdate,appid,cnt,pnt))
		
		#随手晒晒
		
		sql = "delete from bi_recipe_discovery_summary WHERE createtime='%s' and type = 4"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select logdate,appid,sum(cnt),sum(pnt) from (
		select logdate,appid,count(device_id) cnt, 0 as pnt from logs.log_php_app_log 
		where function_id='recipephoto.getproducts'
		and gmifp(parameter_desc) = 12
		and gtifp(parameter_desc) = 1
		and logdate = '%s'
		and appid in (2,4)
		group by logdate,appid
		union all 
		select logdate,appid,0 as cnt, count(distinct(device_id)) pnt from logs.log_php_app_log 
		where function_id='recipephoto.getproducts'
		and gmifp(parameter_desc) = 12
		and gtifp(parameter_desc) = 1
		and logdate = '%s'
		and appid in (2,4)
		group by logdate,appid
		) tmp
		group by logdate,appid;
		"""
		
		cursor = self.hive.execute(sql%(self.curDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					logdate = str(row[0])
					appid = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_recipe_discovery_summary (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,4)"
					self.db.execute(sql%(logdate,appid,cnt,pnt))
		
		
		#其他版块
		sql = "select id,title from rcp_haodou_mobile_%s.showtopic where id not in (1,2,3,12)"
		cursor = self.hive.execute(sql%self.nowDate)
		models = {}
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				model = str(row[0])
				if "" != model:
					models[model] = str(row[1])
		
		for id in models.keys():
			
			sql = "delete from bi_recipe_discovery_summary WHERE createtime='%s' and type = '%s'"
			self.db.execute(sql%(self.curDate,models[id]))
			
			sql = """\
			select logdate,appid,sum(cnt),sum(pnt) from (
			select logdate,appid,count(device_id) cnt, 0 as pnt from logs.log_php_app_log 
			where function_id='recipephoto.getproducts'
			and gmifp(parameter_desc) = %s
			and gtifp(parameter_desc) = 1
			and logdate = '%s'
			and appid in (2,4)
			group by logdate,appid
			union all 
			select logdate,appid,0 as cnt, count(distinct(device_id)) pnt from logs.log_php_app_log 
			where function_id='recipephoto.getproducts'
			and gmifp(parameter_desc) = %s
			and gtifp(parameter_desc) = 1
			and logdate = '%s'
			and appid in (2,4)
			group by logdate,appid
			) tmp
			group by logdate,appid;
			"""
			
			cursor = self.hive.execute(sql%(id,self.curDate,id,self.curDate))
			if cursor is not None:
				for res in cursor:
					row = re.split(r"\s+",res)
					if len(row) == 4:
						logdate = str(row[0])
						appid = int(row[1])
						cnt = int(row[2])
						pnt = int(row[3])
						
						sql = "INSERT INTO bi_recipe_discovery_summary (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,'%s')"
						self.db.execute(sql%(logdate,appid,cnt,pnt,models[id]))
	
	def test(self):	
		
		#热门活动 只是安卓客户端
		
		sql = "delete from bi_recipe_discovery_summary WHERE createtime='%s' and type = 5"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select logdate,count(remote_addr),count(distinct(concat(remote_addr,'-',http_user_agent))) from logs.m_haodou_com 
		where logdate='""" + self.curDate + """' and path like '/app/recipe/act/home.php%' and gaidfua(http_user_agent)=2
		group by logdate;
		"""
		
		cursor = self.hive.execute(sql)
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					logdate = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_recipe_discovery_summary (createtime,platform,total,total_person,type) VALUES ('%s',2,%s,%s,5)"
					self.db.execute(sql%(logdate,cnt,pnt))
		
		
		modelid = {}
		modelid['年度菜谱盘点 分享得好豆台历!']=10
		modelid['开门有喜，豆来参与！']=11
		modelid['蒙牛寻宝之旅 全家总动员']=12
		modelid['今天你用好豆菜谱了吗？']=13
		modelid['最新菜谱']=14
		modelid['更多专属特权 等的就是你']=15
		modelid['砸蛋赢大奖，双节嗨翻天']=16
		modelid['厨房·好豆特别日 送礼啦']=17
		modelid['回复话题逢8有喜 有钱就是任性']=18
		modelid['来说说您与好豆的故事']=19
		modelid['佳厨房·好豆特别日 送礼啦']=20
		
    
		#获取热门活动列表链接
		
		hostregex = re.compile(r"^http[s]?://([a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,3})/\S*?$", re.IGNORECASE)
		pathregex = re.compile(r"^http[s]?://[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,3}(/\S*)?$", re.IGNORECASE)
		url ='http://m.haodou.com/app/recipe/act/home.php?do=actList'  
		data = urllib2.urlopen(url)
		po = json.loads(data.read())
		for i in range(0,len(po)):
			url = po[i][0]["url"]
			title = po[i][0]["title"]
			host = str(hostregex.findall(url)[0]).replace('.','_')
			path = str(pathregex.findall(url)[0])
			
			sql = """\
			select logdate,count(remote_addr),count(distinct(concat(remote_addr,'-',http_user_agent))) from logs.""" + host + """ 
			where logdate='""" + self.curDate + """' and path like '""" + path + """%' and gaidfua(http_user_agent)=2
			group by logdate;
			"""			
			
			cursor = self.hive.execute(sql)
			if cursor is not None:
				for res in cursor:
					row = re.split(r"\s+",res)
					if len(row) > 1:
						logdate = str(row[0])
						cnt = int(row[1])
						pnt = int(row[2])
						
						print title
						if title.encode("utf-8") in modelid:
							sql = "INSERT INTO bi_recipe_discovery_summary (createtime,platform,total,total_person,type) VALUES ('%s','%s',%s,%s,5)"
							self.db.execute(sql%(logdate,modelid[title.encode("utf-8")],cnt,pnt))
						else:
							sql = "INSERT INTO bi_recipe_discovery_summary (createtime,platform,total,total_person,type) VALUES ('%s','%s',%s,%s,5)"
							self.db.execute(sql%(logdate,title.encode("utf-8"),cnt,pnt))
							
						
						
		
		
	def dbClose(self):
		
		self.db.close()	
		
#paths = os.popen("pwd").readlines()
#path = paths[0].replace("\n","")

path  = os.path.dirname(os.path.abspath(__file__))
nowday = datetime.now()
	
def start(today,test=False):
	
	curDate = (nowday - timedelta(days = today))
		
	print "当前分析时间是：" + curDate.strftime("%Y-%m-%d")
	
	file_path = path
	
	#数据分析
	es = DiscoveryAnalysis(today)
	es.startExtract()
	es.test()
			
	es.dbClose()

if __name__ == '__main__':

	usage = "usage: %prog [options] arg1 arg2"
	parser = OptionParser(usage = usage, version="%prog 0.1")
	parser.add_option("-d", "--days", help = 'Download the log back for a few days, default value is 1, use \',\' separated, like 10,1', dest = "days")
	parser.add_option("-t", "--test", help = 'Open to the test mode', action="store_true", default=False, dest="test")
	(options, args) = parser.parse_args()
	
	if len(sys.argv)<=1:
		parser.print_help()
		sys.exit(2)
	
	days = options.days or "1"
	delay = days.split(",")
	
	if len(delay) != 1:

		for today in range(int(delay[1]),int(delay[0])+1,1):
			start(today,options.test)
			
	else:
		start(int(delay[0]),options.test)