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

class CarefullySelectedAnalysis:
	
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
		
		
		#精选页面总浏览量
		
		sql = "delete from bi_recipe_carefully_selected_summary WHERE date(createtime)='%s' and type = '0'"
		self.db.execute(sql%self.curDate)
		
		
			
		sql = """\
		select logdate,appid,count(device_id),count(distinct(device_id)) from logs.log_php_app_log 
		where function_id='recipe.getcollectlist'
		and logdate = '%s'
		and appid in (2,4)
		group by logdate,appid;
		"""
				
		cursor = self.hive.execute(sql%self.curDate)
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					logdate = str(row[0])
					appid = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_recipe_carefully_selected_summary (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,0)"
					self.db.execute(sql%(logdate,appid,cnt,pnt))
		
		
		#精选页广告浏览量
		
		sql = "delete from bi_recipe_carefully_selected_summary WHERE date(createtime)='%s' and type = '1'"
		self.db.execute(sql%self.curDate)
		
		
		sql = "select gtifurl(url) from rcp_haodou_mobile_%s.recommenditem where type=5 AND status=1 and gtifurl(url) <> 0"
				
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
			
				sql = """ \
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
							
							sql = "INSERT INTO bi_recipe_carefully_selected_summary (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,1)"
							self.db.execute(sql%(logdate,appid,cnt,pnt))
			
		
		#精选页面其他版块浏览数据
		modelid = {}
		modelid['热门菜谱']=2
		modelid['私人定制']=3
		modelid['时令佳肴']=4
		modelid['达人菜谱']=5
		modelid['最新菜谱']=6
		modelid['快乐的烘焙']=7
		
		#取版块名称
		sql = """ \
		select gfidfurl(parameter_desc) from logs.log_php_app_log where appid in (2,4) and function_id='recipe.getcollectrecomment' 
		and logdate = '%s' group by gfidfurl(parameter_desc);
		"""
		
		cursor = self.hive.execute(sql%self.curDate)
		models = {}
		modelstr = ""
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				model = str(row[0])
				if model != 'NULL' and "" != model:
					models[model] = model
					modelstr += "'" + str(model) + "',"
		
		if "" != modelstr:
			modelstr = modelstr[0:len(modelstr)-1]
				
		for key in models:
			sql = "delete from bi_recipe_carefully_selected_summary WHERE date(createtime)='%s' and type = '%s'"
			if key in modelid:
				self.db.execute(sql%(self.curDate,modelid[key]))
				print sql%(self.curDate,modelid[key])
			else:
				self.db.execute(sql%(self.curDate,key))
				print sql%(self.curDate,key)
				
			sql = """\
			select * from bi_recipe_carefully_selected_summary where type in (2,3,4,5,6,7) order by createtime desc limit 40;;
			"""	
			cur = self.db.execute(sql)
			for row in cur.fetchall():
				print row[0],row[1],row[2],row[3],row[4],row[5]
						
			sql = """ \
			set hivevar:key=%s;
			set hivevar:date=%s;
			select logdate,appid,sum(cnt),sum(pnt) from (
			select logdate,appid,count(1) cnt, 0 as pnt from logs.log_php_app_log 
			where function_id='recipe.getcollectrecomment'
			and logdate = '\${date}'
			and gfidfurl(parameter_desc)='\${key}'
			and appid in (2,4)
			group by logdate,appid
			union all 
			select logdate,appid,0 as cnt, count(distinct(device_id)) pnt from logs.log_php_app_log 
			where function_id='recipe.getcollectrecomment'
			and logdate = '\${date}'
			and gfidfurl(parameter_desc)='\${key}'
			and appid in (2,4)
			group by logdate,appid
			) tmp
			group by logdate,appid;		
			"""
			
			cursor = self.hive.execute(sql%(key,self.curDate))
			if cursor is not None:
				for res in cursor:
					
					print res
					row = re.split(r"\s+",res)
					if len(row) > 1:
						logdate = str(row[0])
						appid = int(row[1])
						cnt = int(row[2])
						pnt = int(row[3])
						
						sql = "INSERT INTO bi_recipe_carefully_selected_summary (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,'%s')"
						if key in modelid:
							self.db.execute(sql%(logdate,appid,cnt,pnt,modelid[key]))
							print sql%(logdate,appid,cnt,pnt,modelid[key])
						else:	
							self.db.execute(sql%(logdate,appid,cnt,pnt,key))
							print sql%(logdate,appid,cnt,pnt,key)
		
		sql = """\
		select * from bi_recipe_carefully_selected_summary where type in (2,3,4,5,6,7) order by createtime desc limit 40;;
		"""	
		cur = self.db.execute(sql)
		for row in cur.fetchall():
			print row[0],row[1],row[2],row[3],row[4],row[5]
		
		
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
	es = CarefullySelectedAnalysis(today)
	es.startExtract()
	
	es.dbClose()

if __name__ == '__main__':
	
	reload(sys)
	sys.setdefaultencoding("utf8")

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