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



	
class PhotoAnalysis:
	
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
		
		#发布数
		
		sql = "delete from bi_recipe_photo_analysis where createtime='%s' and type=0"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(createtime),\`from\`,count(id),count(distinct(userid))
		from hd_haodou_photo_%s.photo 
		where status in (1,2) and to_date(createtime) = '%s'
		group by to_date(createtime),\`from\`;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					createtime = str(row[0])
					platform = str(row[1])
					count = str(row[2])
					pcount = str(row[3])
					
					sql = "INSERT INTO bi_recipe_photo_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,0)"
					self.db.execute(sql%(createtime,platform,count,pcount))
		
		#点赞数
		
		sql = "delete from bi_recipe_photo_analysis where createtime='%s' and type=1"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(createtime),count(id),count(distinct(userid))
		from hd_haodou_digg_%s.digg
		where itemtype=1 and to_date(createtime) = '%s'
		group by to_date(createtime);
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 3:
					createtime = str(row[0])
					count = str(row[1])
					pcount = str(row[2])
					
					sql = "INSERT INTO bi_recipe_photo_analysis (createtime,platform,total,total_person,type) VALUES ('%s',-1,%s,%s,1)"
					self.db.execute(sql%(createtime,count,pcount))
					
		
		#评论数
		
		sql = "delete from bi_recipe_photo_analysis where createtime='%s' and type=2"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(createtime),platform,count(commentid),count(distinct(userid))
		from hd_haodou_comment_%s.comment
		where to_date(createtime) = '%s' and status=1 and type=12
		group by to_date(createtime),platform;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					createtime = str(row[0])
					platform = str(row[1])
					count = str(row[2])
					pcount = str(row[3])
					
					sql = "INSERT INTO bi_recipe_photo_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,2)"
					self.db.execute(sql%(createtime,platform,count,pcount))
		
		#分享数
		
		sql = "delete from bi_recipe_photo_analysis where createtime='%s' and type=3"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(createtime),channel,count(feedid),count(distinct(userid)) 
		from hd_haodou_center_%s.userfeed
		where to_date(createtime) = '%s'
		and channel in(0,3,4,5) and type=111
		group by to_date(createtime),channel;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					createtime = str(row[0])
					platform = str(row[1])
					count = str(row[2])
					pcount = str(row[3])
					
					sql = "INSERT INTO bi_recipe_photo_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,3)"
					self.db.execute(sql%(createtime,platform,count,pcount))
		
		#新增用户作品数
		
		sql = "delete from bi_recipe_photo_person_analysis where createtime='%s' and type=0"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(u.regtime),p.userid,u.username,p.\`from\`,count(p.id)
		from hd_haodou_passport_%s.user u
		inner join hd_haodou_photo_%s.photo p on p.userid=u.userid
		where to_date(u.regtime) = '%s' and to_date(p.createtime) = '%s' and p.status=1
		group by to_date(u.regtime),p.userid,u.username,p.\`from\`
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					userid = int(row[1])
					username = str(row[2])
					platform = int(row[3])
					count = int(row[4])
					
					sql = "INSERT INTO bi_recipe_photo_person_analysis (createtime,userid,username,platform,count,type) VALUES ('%s',%s,'%s',%s,%s,0)"
					self.db.execute(sql%(createtime,userid,username,platform,count))
	
	
		#新增作品人数
		#每天看发布作品的用户，再看之前有没有发布过作品，如果没有发布过，就算做是新增作品人数
		
		sql = "delete from bi_recipe_photo_person_analysis where createtime='%s' and type=1"
		self.db.execute(sql%self.curDate)
		
		
		#找今天发布作品的用户
		sql = """\
		select distinct(userid) from hd_haodou_photo_%s.photo where to_date(createtime) = '%s' and status=1
		"""
		
		users = {}
		userstr = ""
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				userid = str(res)
				if userid not in users:
					users[userid] = {}
					users[userid]["pcount"] = 0
					userstr += userid + ","
			
			if "" != userstr:
				userstr = userstr[0:len(userstr)-1]
		
		#看之前有没有发布过作品	
		sql = """\
		select userid,count(id) from hd_haodou_photo_%s.photo where to_date(createtime) > '%s' and status=1
		and userid in (%s)
		group by userid
		"""
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate,userstr))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					userid = str(row[0])
					pcount = int(row[1])
					if userid in users:
						users[userid]["pcount"] = pcount
		
		userstr = ""		
		for userid in users.keys():
			#没有发布过
			if users[userid]["pcount"] == 0:
				userstr += userid + ","
								
			
		if "" != userstr:
			userstr = userstr[0:len(userstr)-1]
		
		#找今日发布的作品数	
		sql = """\
		select u.username,p.userid,to_date(u.regtime),p.\`from\`,count(p.id)
		from hd_haodou_photo_%s.photo p
		left outer join hd_haodou_passport_%s.user u on u.userid=p.userid
		where p.status =1 and to_date(p.createtime) = '%s' and p.userid in (%s)
		group by u.username,p.userid,to_date(u.regtime),p.\`from\`;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate,userstr))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					username = str(row[0])
					userid = str(row[1])
					regtime = str(row[2])
					platform = int(row[3])
					count = int(row[4])
					
					sql = "INSERT INTO bi_recipe_photo_person_analysis (createtime,regtime,userid,username,platform,count,type) VALUES ('%s','%s',%s,'%s',%s,%s,1)"
					self.db.execute(sql%(self.curDate,regtime,userid,username,platform,count))
		
	
		
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
	es = PhotoAnalysis(today)
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