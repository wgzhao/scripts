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



	
class AlbumAnalysis:
	
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
		
		
		
		#创建数
		
		sql = "delete from bi_recipe_album_analysis where createtime='%s' and type=0"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(createtime) createtime,count(albumid),count(distinct(userid))
		from hd_haodou_recipe_%s.album
		where to_date(createtime) = '%s'
		group by to_date(createtime)
		"""
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 3:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_recipe_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',-1,%s,%s,0)"
					self.db.execute(sql%(createtime,cnt,pnt))
					
		#通过审核数
		
		sql = "delete from bi_recipe_album_analysis where createtime='%s' and type=1"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(createtime) createtime,count(albumid),count(distinct(userid))
		from hd_haodou_recipe_%s.album
		where to_date(createtime) = '%s' and status=1
		group by to_date(createtime)
		"""
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 3:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_recipe_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',-1,%s,%s,1)"
					self.db.execute(sql%(createtime,cnt,pnt))
		
		#1星/2星/3星/4星/5星专辑数
		
		sql = "delete from bi_recipe_album_analysis where createtime='%s' and type=2"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(createtime) createtime,rate,count(albumid)
		from hd_haodou_recipe_%s.album
		where to_date(createtime) = '%s'
		and status=1 and rate in (1,2,3,4,5)
		group by to_date(createtime),rate;
		"""
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 3:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_recipe_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',-1,%s,%s,2)"
					self.db.execute(sql%(createtime,cnt,pnt))
		
		
		#评论数
		
		sql = "delete from bi_recipe_album_analysis where createtime='%s' and type=3"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(createtime) createtime,platform,count(commentid),count(distinct(userid))
		from hd_haodou_comment_%s.comment
		where type=1 and status=1 and to_date(createtime) = '%s'
		group by to_date(createtime),platform
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					createtime = str(row[0])
					platform = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_recipe_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,3)"
					self.db.execute(sql%(createtime,platform,cnt,pnt))
		
		#关注数
		
		sql = "delete from bi_recipe_album_analysis where createtime='%s' and type=4"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(createtime) createtime,count(albumid),count(distinct(userid))
		from hd_haodou_recipe_albumfollow_%s.albumfollow
		where to_date(createtime) = '%s'
		group by to_date(createtime)
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 3:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_recipe_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',-1,%s,%s,4)"
					self.db.execute(sql%(createtime,cnt,pnt))
		
		#分享数
		
		sql = "delete from bi_recipe_album_analysis where createtime='%s' and type=5"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(createtime) createtime,count(feedid),count(distinct(userid))
		from hd_haodou_center_%s.userfeed
		where to_date(createtime) ='%s'
		and type in (113)
		group by to_date(createtime)
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 3:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_recipe_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',-1,%s,%s,5)"
					self.db.execute(sql%(createtime,cnt,pnt))
					
		
		#详情页阅读数  platform 0,2,4
		
		sql = "delete from bi_recipe_album_analysis where createtime='%s' and type=6"
		self.db.execute(sql%self.curDate)
		
		#web
		sql = """\
		select w.logdate,count(r.albumid),count(distinct(concat(remote_addr,'-',http_user_agent))) 
		from www_haodou_com w
		inner join hd_haodou_recipe_%s.album r on gamidfurl(w.path)=r.albumid
		where w.logdate = '%s' and r.status=1 and gamidfurl(w.path) <> 0
		group by w.logdate;
		"""
		
		dpc = {}
		key = "0"
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 3:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					if key not in dpc:
						dpc[key] = {}
					dpc[key]["createtime"] = createtime
					dpc[key]["cnt"] = cnt
					dpc[key]["pnt"] = pnt
		
		#wap
		sql = """\
		select to_date(w.logdate),count(r.albumid),count(distinct(concat(remote_addr,'-',http_user_agent))) 
		from m_haodou_com w
		inner join hd_haodou_recipe_%s.album r on gamidfurl(w.path)=r.albumid
		where w.logdate = '%s' and r.status=1 and gamidfurl(w.path) <> 0
		group by to_date(w.logdate);
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 3:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					if key in dpc:
						cnt = dpc[key]["cnt"] + cnt
						pnt = dpc[key]["pnt"] + pnt
					
					sql = "INSERT INTO bi_recipe_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',0,%s,%s,6)"
					self.db.execute(sql%(createtime,cnt,pnt))
					
		else:
			for key in dpc:
				createtime = dpc[key]["createtime"]
				cnt = dpc[key]["cnt"]
				pnt = dpc[key]["pnt"]
				sql = "INSERT INTO bi_recipe_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',0,%s,%s,6)"
				self.db.execute(sql%(createtime,cnt,pnt))
		
		#app
		sql = """\
		select logdate,appid,sum(cnt),sum(pnt) from (
		select logdate,appid,count(1) cnt, 0 as pnt from log_php_app_log 
		where function_id='info.getalbuminfo'
		and logdate ='%s'
		and appid in (2,4)
		group by logdate,appid
		union all 
		select logdate,appid,0 as cnt, count(distinct(device_id)) pnt from log_php_app_log 
		where function_id='info.getalbuminfo'
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
					createtime = str(row[0])
					platform = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_recipe_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,6)"
					self.db.execute(sql%(createtime,platform,cnt,pnt))
	
	
		
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
	es = AlbumAnalysis(today)
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