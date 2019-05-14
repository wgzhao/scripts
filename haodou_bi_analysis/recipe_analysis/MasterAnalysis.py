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




	
class MasterAnalysis:
	
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
		
		
		#登录数
		#网站端
		sql = "delete from bi_recipe_master_summary where createtime='%s' and type=0"
		self.db.execute(sql%self.curDate)
		
		master_web = {}
		
		sql = """\
		select vu.userid,u.username,count(h.user_id)
		from hd_haodou_center_%s.vipuser vu
		left outer join hd_haodou_passport_%s.user u on u.userid=vu.userid 
		inner join logs.www_haodou_com h on h.user_id=vu.userid
		where h.logdate='%s'
		group by vu.userid,u.username;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 3:
					logdate = self.curDate
					userid = int(row[0])
					username = str(row[1])
					master_web[userid] = username
		
		sql = """\
		select vu.userid,u.username,count(h.user_id)
		from hd_haodou_center_%s.vipuser vu
		left outer join hd_haodou_passport_%s.user u on u.userid=vu.userid 
		inner join logs.m_haodou_com h on h.user_id=vu.userid
		where h.logdate='%s'
		group by vu.userid,u.username;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 3:
					logdate = self.curDate
					userid = int(row[0])
					username = str(row[1])
					master_web[userid] = username
		
		#移动端
		
		master_mobile = {}
		
		sql = """\
		select vu.userid,u.username,appid,count(vu.userid)
		from hd_haodou_center_%s.vipuser vu
		left outer join hd_haodou_passport_%s.user u on u.userid=vu.userid 
		inner join logs.log_php_app_log h on h.userid=vu.userid
		where h.logdate='%s'
		group by vu.userid,u.username,appid;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					userid = str(row[0])
					username = str(row[1])
					appid = str(row[2])
					
					if userid not in master_mobile:
						master_mobile[userid] = {}
											
					master_mobile[userid][appid] = username
		
		
		for userid in master_web.keys():
			username = master_web[userid]
			sql = "INSERT INTO bi_recipe_master_summary (createtime,userid,username,count,platform,type) VALUES ('%s',%s,'%s',1,0,0)"
			self.db.execute(sql%(self.curDate,userid,username))
		
		for userid in master_mobile.keys():			
			for appid in master_mobile[userid].keys():
				if appid != 0:
					username = master_mobile[userid][appid]
					sql = "INSERT INTO bi_recipe_master_summary (createtime,userid,username,count,platform,type) VALUES ('%s',%s,'%s',1,%s,0)"
					self.db.execute(sql%(self.curDate,userid,username,appid))
		
		#评论数
		
		sql = "delete from bi_recipe_master_summary where createtime='%s' and type=1"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select c.userid,u.username,c.platform,count(distinct(c.commentid))
		from hd_haodou_comment_%s.comment c
		inner join hd_haodou_center_%s.vipuser vu on vu.userid=c.userid
		left outer join hd_haodou_passport_%s.user u on u.userid=c.userid
		where vu.viptype=1 and vu.status=1 and c.status=1
		and to_date(c.createtime) = '%s'
		group by c.userid,u.username,c.platform;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					logdate = self.curDate
					userid = int(row[0])
					username = str(row[1])
					platform = int(row[2])
					count = int(row[3])
					
					sql = "INSERT INTO bi_recipe_master_summary (createtime,userid,username,count,platform,type) VALUES ('%s',%s,'%s',%s,%s,1)"
					self.db.execute(sql%(logdate,userid,username,count,platform))
	
		#点赞数

		sql = "delete from bi_recipe_master_summary where createtime='%s' and type=2"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select d.userid,u.username,d.itemtype,count(distinct(d.id))
		from hd_haodou_digg_%s.digg d
		inner join hd_haodou_center_%s.vipuser vu on vu.userid=d.userid
		left outer join hd_haodou_passport_%s.user u on u.userid=d.userid
		where vu.viptype=1 and vu.status=1
		and to_date(d.createtime) = '%s'
		group by d.userid,u.username,d.itemtype;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					logdate = self.curDate
					userid = int(row[0])
					username = str(row[1])
					itemtype = int(row[2])
					count = int(row[3])
					
					sql = "INSERT INTO bi_recipe_master_summary (createtime,userid,username,count,platform,type) VALUES ('%s',%s,'%s',%s,%s,2)"
					self.db.execute(sql%(logdate,userid,username,count,itemtype))
		
		#菜谱发布
		
		sql = "delete from bi_recipe_master_summary where createtime='%s' and type=3"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select r.userid,u.username,r.postfrom,count(distinct(r.recipeid))
		from hd_haodou_recipe_%s.recipe r
		inner join hd_haodou_center_%s.vipuser vu on vu.userid=r.userid
		left outer join hd_haodou_passport_%s.user u on u.userid=r.userid
		where vu.viptype=1 and vu.status=1 and r.status=0
		and to_date(r.createtime) = '%s'
		group by r.userid,u.username,r.postfrom;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					logdate = self.curDate
					userid = int(row[0])
					username = str(row[1])
					platform = int(row[2])
					count = int(row[3])
					
					sql = "INSERT INTO bi_recipe_master_summary (createtime,userid,username,count,platform,type) VALUES ('%s',%s,'%s',%s,%s,3)"
					self.db.execute(sql%(logdate,userid,username,count,platform))
		
		#专辑总数
		
		sql = "delete from bi_recipe_master_summary where createtime='%s' and type=4"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select am.userid,u.username,count(distinct(am.albumid))
		from hd_haodou_recipe_%s.album am
		inner join hd_haodou_center_%s.vipuser vu on vu.userid=am.userid
		left outer join hd_haodou_passport_%s.user u on u.userid=am.userid
		where vu.viptype=1 and vu.status=1 and am.status=1
		and to_date(am.createtime) = '%s'
		group by am.userid,u.username;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 3:
					logdate = self.curDate
					userid = int(row[0])
					username = str(row[1])
					count = int(row[2])
					
					sql = "INSERT INTO bi_recipe_master_summary (createtime,userid,username,count,platform,type) VALUES ('%s',%s,'%s',%s,-1,4)"
					self.db.execute(sql%(logdate,userid,username,count))
		
		#作品总数
		sql = "delete from bi_recipe_master_summary where createtime='%s' and type=5"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select p.userid,u.username,p.\`from\`,count(distinct(p.id)) 
		from hd_haodou_photo_%s.photo p
		inner join hd_haodou_center_%s.vipuser vu on vu.userid=p.userid
		left outer join hd_haodou_passport_%s.user u on u.userid=p.userid
		where vu.viptype=1 and vu.status=1 and p.status=1
		and to_date(p.createtime) = '%s'
		group by p.userid,u.username,p.\`from\`;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					logdate = self.curDate
					userid = int(row[0])
					username = str(row[1])
					platform = int(row[2])
					count = int(row[3])
					
					sql = "INSERT INTO bi_recipe_master_summary (createtime,userid,username,count,platform,type) VALUES ('%s',%s,'%s',%s,%s,5)"
					self.db.execute(sql%(logdate,userid,username,count,platform))

		#3、乐在厨房/营养健康/厨房宝典话题总数
		
		sql = "delete from bi_recipe_master_summary where createtime='%s' and type=6"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select g.userid,u.username,g.cateid,count(distinct(g.topicid))
		from hd_haodou_center_%s.grouptopic g
		inner join hd_haodou_center_%s.vipuser vu on vu.userid=g.userid
		left outer join hd_haodou_passport_%s.user u on u.userid=g.userid
		where vu.viptype=1 and vu.status=1 and g.status=1 and g.cateid in (5,6,8)
		and to_date(g.createtime) = '%s'
		group by g.userid,u.username,g.cateid;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					logdate = self.curDate
					userid = int(row[0])
					username = str(row[1])
					cateid = int(row[2])
					count = int(row[3])
					
					sql = "INSERT INTO bi_recipe_master_summary (createtime,userid,username,count,platform,type) VALUES ('%s',%s,'%s',%s,%s,6)"
					self.db.execute(sql%(logdate,userid,username,count,cateid))
		
		#四星五星菜谱数
		
		sql = "delete from bi_recipe_master_rate_summary where createtime='%s' and type=0"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select r.userid,u.username,r.postfrom,r.rate,count(distinct(r.recipeid))
		from hd_haodou_recipe_%s.recipe r
		inner join hd_haodou_center_%s.vipuser vu on vu.userid=r.userid
		left outer join hd_haodou_passport_%s.user u on u.userid=r.userid
		where vu.viptype=1 and vu.status=1 and r.status=0 and r.rate in (4,5)
		and to_date(r.createtime) = '%s'
		group by r.userid,u.username,r.postfrom,r.rate;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 5:
					logdate = self.curDate
					userid = int(row[0])
					username = str(row[1])
					platform = str(row[2])
					rate = str(row[3])
					count = str(row[4])
					
					sql = "INSERT INTO bi_recipe_master_rate_summary (createtime,userid,username,count,platform,rate,type) VALUES ('%s',%s,'%s',%s,%s,%s,0)"
					self.db.execute(sql%(logdate,userid,username,count,platform,rate))
					
	
		
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
	es = MasterAnalysis(today)
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