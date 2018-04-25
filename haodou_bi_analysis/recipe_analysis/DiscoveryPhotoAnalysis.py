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



	
class DiscoveryPhotoAnalysis:
	
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
				
		#早午晚餐作品发布数/人数
		
		sql = "delete from bi_recipe_discovery_photo_summary WHERE createtime='%s' and type in (1,2,3)"
		self.db.execute(sql%(self.curDate))
		
		sql = """\
		select to_date(createtime) createtime,topicid,\`from\`,count(id),count(distinct(userid)) from hd_haodou_photo_%s.photo
		where to_date(createtime) = '%s'
		and status =1
		and topicid in (1,2,3)
		group by to_date(createtime),topicid,\`from\`;
		"""
				
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 5:
					logdate = str(row[0])
					model = int(row[1])
					appid = int(row[2])
					cnt = int(row[3])
					pnt = int(row[4])
										
					sql = "INSERT INTO bi_recipe_discovery_photo_summary (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,%s)"
					self.db.execute(sql%(logdate,appid,cnt,pnt,model))
						
		
		#新手课堂作品
		
		sql = "delete from bi_recipe_discovery_photo_summary WHERE createtime='%s' and type = '0'"
		self.db.execute(sql%self.curDate)
		
		sql = "SELECT rid,createtime FROM rcp_haodou_mobile_%s.discovery WHERE type=2 AND status=1 ORDER BY createtime DESC"			
		
		cursor = self.hive.execute(sql%self.nowDate)
		
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				rid = str(row[0])
				if "" != rid and "0" != rid:
					self.getNewClassroomPhoto(rid)
					
		#随手晒晒和动态增加的版块
		
		sql = "select id from rcp_haodou_mobile_%s.showtopic where id not in (1,2,3)"
		
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
		
		sql = "delete from bi_recipe_discovery_photo_summary WHERE createtime='%s' and type in (%s)"
		self.db.execute(sql%(self.curDate,topics))
		
		sql = """\
		SELECT to_date(createtime) createtime,\`from\`,count(id),count(distinct(userid)),topicid FROM hd_haodou_photo_%s.photo 
		where to_date(createtime) = '%s' and topicid in (""" + topics + """) and status <> 0
		group by to_date(createtime),\`from\`,topicid;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 5:
					logdate = str(row[0])
					appid = str(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					topic = int(row[4])
										
					sql = "INSERT INTO bi_recipe_discovery_photo_summary (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,%s)"
					self.db.execute(sql%(logdate,appid,cnt,pnt,topic))
					
	
	def getNewClassroomPhoto(self,rid):
		
		sql = """\
		select to_date(createtime) createtime,\`from\`,count(id),count(distinct(userid)) 
		FROM hd_haodou_photo_%s.photo
		where to_date(createtime) = '%s' and status <> 0 and rid = %s
		group by to_date(createtime),\`from\`;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate,rid))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					logdate = str(row[0])
					appid = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
		
					sql = "INSERT INTO bi_recipe_discovery_photo_summary (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,'0')"
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
	es = DiscoveryPhotoAnalysis(today)
	es.startExtract()
	es.dbClose()

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