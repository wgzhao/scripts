# -*- coding: UTF-8 -*- 
#coding = utf-8

import sys
sys.path.append("/home/likunjian/")
from optparse import OptionParser
from DBUtil import *
from hiveDB import *
from sparkDB import *
from datetime import *
import time
from time import ctime
import commands
import os
import re


class DigestAlbumAnalysis:
	
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
		
		sql = "delete from bi_recipe_digest_album_analysis WHERE createtime='%s' and type = 0"
		self.db.execute(sql%self.curDate)
		
		#单个专辑
		
		sql = """ \
		select logdate,appid,sum(cnt),sum(pnt) from (
		select logdate,appid,count(device_id) cnt, 0 as pnt from logs.log_php_app_log 
		where function_id='info.getalbuminfo' and logdate = '%s' and appid in (2,4)
		group by logdate,appid
		union all 
		select logdate,appid,0 as cnt, count(distinct(device_id)) pnt from logs.log_php_app_log 
		where function_id='info.getalbuminfo' and logdate = '%s' and appid in (2,4)
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
			
					sql = "INSERT INTO bi_recipe_digest_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,0)"
					self.db.execute(sql%(logdate,appid,cnt,pnt))
		
		sql = "delete from bi_recipe_digest_album_analysis WHERE createtime='%s' and type = 1"
		self.db.execute(sql%self.curDate)
		
		#专辑列表页
		
		sql = """ \
		select logdate,appid,sum(cnt),sum(pnt) from (
		select logdate,appid,count(1) cnt, 0 as pnt from logs.log_php_app_log 
		where function_id='recipe.getalbumlist' and logdate = '%s' and appid in (2,4)
		group by logdate,appid
		union all 
		select logdate,appid,0 as cnt, count(distinct(device_id)) pnt from logs.log_php_app_log 
		where function_id='recipe.getalbumlist' and logdate = '%s' and appid in (2,4)
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
			
					sql = "INSERT INTO bi_recipe_digest_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,1)"
					self.db.execute(sql%(logdate,appid,cnt,pnt))
		
		#单个厨房宝典
		
		#取厨房宝典下发布的话题
		sql = "select gtifurl(url) from rcp_haodou_mobile_%s.rcpwiki where type=1 and gtifurl(url) <> 0"
		
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
		
				sql = "delete from bi_recipe_digest_album_analysis WHERE createtime='%s' and type = 2"
				self.db.execute(sql%self.curDate)
				
				sql = """ \
				select logdate,-1,count(path) cnt, count(distinct(concat(remote_addr,'-',http_user_agent)))
				from logs.m_haodou_com
				where logdate = '%s' and gtifurl(path) in (%s) and gtifurl(path) <> 0
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
							
							sql = "INSERT INTO bi_recipe_digest_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,2)"
							self.db.execute(sql%(logdate,appid,cnt,pnt))
			
		
		#厨房宝典列表页
		sql = "delete from bi_recipe_digest_album_analysis WHERE createtime='%s' and type = 3"
		self.db.execute(sql%self.curDate)	
				
		sql = """ \
		select logdate,appid,sum(cnt),sum(pnt) from (
		select logdate,appid,count(device_id) cnt, 0 as pnt from logs.log_php_app_log 
		where function_id='wiki.getlistbytype'
		and logdate = '""" + self.curDate + """'
		and gtifp(parameter_desc) = 1
		and appid in (2,4)
		group by logdate,appid
		union all 
		select logdate,appid,0 as cnt, count(distinct(device_id)) pnt from logs.log_php_app_log 
		where function_id='wiki.getlistbytype'
		and logdate = '""" + self.curDate + """'
		and gtifp(parameter_desc) = 1
		and appid in (2,4)
		group by logdate,appid
		) tmp
		group by logdate,appid;
		"""
		
		cursor = self.hive.execute(sql)
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					logdate = str(row[0])
					appid = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_recipe_digest_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,3)"
					self.db.execute(sql%(logdate,appid,cnt,pnt))
		
		
		
		#单个营养餐桌
		
		#取营养餐桌下发布的话题
		sql = "select gtifurl(url) from rcp_haodou_mobile_%s.rcpwiki where type=2 and gtifurl(url) <> 0"
		
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
				
				sql = "delete from bi_recipe_digest_album_analysis WHERE createtime='%s' and type = 4"
				self.db.execute(sql%self.curDate)
				
				sql = """ \
				select logdate,-1,count(path) cnt, count(distinct(concat(remote_addr,'-',http_user_agent)))
				from logs.m_haodou_com
				where logdate = '%s' and gtifurl(path) in (%s)
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
							
							sql = "INSERT INTO bi_recipe_digest_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,4)"
							self.db.execute(sql%(logdate,appid,cnt,pnt))
		
		#营养餐桌列表页
		sql = "delete from bi_recipe_digest_album_analysis WHERE createtime='%s' and type = 5"
		self.db.execute(sql%self.curDate)	
				
		sql = """ \
		select logdate,appid,sum(cnt),sum(pnt) from (
		select logdate,appid,count(device_id) cnt, 0 as pnt from logs.log_php_app_log 
		where function_id='wiki.getlistbytype'
		and logdate = '""" + self.curDate + """'
		and gtifp(parameter_desc) = 2
		and appid in (2,4)
		group by logdate,appid
		union all 
		select logdate,appid,0 as cnt, count(distinct(device_id)) pnt from logs.log_php_app_log 
		where function_id='wiki.getlistbytype'
		and logdate = '""" + self.curDate + """'
		and gtifp(parameter_desc) = 2
		and appid in (2,4)
		group by logdate,appid
		) tmp
		group by logdate,appid;
		"""
		
		cursor = self.hive.execute(sql)
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					logdate = str(row[0])
					appid = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_recipe_digest_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,5)"
					self.db.execute(sql%(logdate,appid,cnt,pnt))	
				
		
		#单个榜单
		sql = "delete from bi_recipe_digest_album_analysis WHERE createtime='%s' and type = 6"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select logdate,appid,sum(cnt),sum(pnt) from (
		select logdate,appid,count(1) cnt, 0 as pnt from logs.log_php_app_log 
		where function_id='rank.getrankview'
		and logdate = '""" + self.curDate + """'
		and appid in (2,4)
		group by logdate,appid
		union all 
		select logdate,appid,0 as cnt, count(distinct(device_id)) pnt from logs.log_php_app_log 
		where function_id='rank.getrankview'
		and logdate = '""" + self.curDate + """'
		and appid in (2,4)
		group by logdate,appid
		) tmp
		group by logdate,appid;
		"""
		
		cursor = self.hive.execute(sql)
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					logdate = str(row[0])
					appid = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_recipe_digest_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,6)"
					self.db.execute(sql%(logdate,appid,cnt,pnt))
		
		#榜单列表页
		sql = "delete from bi_recipe_digest_album_analysis WHERE createtime='%s' and type = 7"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select logdate,appid,sum(cnt),sum(pnt) from (
		select logdate,appid,count(1) cnt, 0 as pnt from logs.log_php_app_log 
		where function_id='rank.getranklist'
		and logdate = '""" + self.curDate + """'
		and appid in (2,4)
		group by logdate,appid
		union all 
		select logdate,appid,0 as cnt, count(distinct(device_id)) pnt from logs.log_php_app_log 
		where function_id='rank.getranklist'
		and logdate = '""" + self.curDate + """'
		and appid in (2,4)
		group by logdate,appid
		) tmp
		group by logdate,appid;
		"""
		
		cursor = self.hive.execute(sql)
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					logdate = str(row[0])
					appid = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_recipe_digest_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,7)"
					self.db.execute(sql%(logdate,appid,cnt,pnt))
		
	def dbClose(self):
		
		self.db.close()	
		
#paths = os.popen("pwd").readlines()
#path = paths[0].replace("\n","")

path  = os.path.dirname(os.path.abspath(__file__))
nowday = datetime.now()
	
def start(today):
	
	curDate = (nowday - timedelta(days = today))
		
	print "当前分析时间是：" + curDate.strftime("%Y-%m-%d")
	
	file_path = path
	
	#数据分析
	es = DigestAlbumAnalysis(today)
	es.startExtract()

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

		for today in range(int(delay[1]),int(delay[0])+1,1):
			start(today)
			
	else:
		start(int(delay[0]))
