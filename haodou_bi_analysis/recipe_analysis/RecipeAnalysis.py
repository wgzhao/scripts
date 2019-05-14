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



	
class RecipeAnalysis:
	
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
		
		
		#菜谱pv/ip
		sql = "delete from bi_recipe_recipe_analysis where createtime='%s' and type =0"
		self.db.execute(sql%self.curDate)
		
		#web		
		sql = """\
		select logdate,count(remote_addr),count(distinct(concat(remote_addr,'-',http_user_agent))) 
		from www_haodou_com
		where logdate = '""" + self.curDate + """' and path like '/recipe/%'
		group by logdate;
		"""
		
		cursor = self.hive.execute(sql)
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_recipe_recipe_analysis (createtime,platform,total,total_person,type) VALUES ('%s',0,%s,%s,0)"
					self.db.execute(sql%(createtime,cnt,pnt))
			
		#app
		sql = """\
		select to_date(logdate) logdate,appid,count(concat(device_id,uuid)),count(distinct(concat(device_id,uuid)))
		from log_php_app_log
		where logdate = '%s' and appid in (2,4,6) and function_id in ('collect.getrecipe','collect.getrecipelistbycid','collect.info','info.downloadinfo','info.getfoodinfo','info.getinfo','info.getinfov2','info.getinfov3','info.getlastestinfo','info.randrecipeinfo','info.saveinfo','info.updateandpublic','recipe.getcollectlist','recipe.getcollectrecomment','recipe.getfindrecipe','share.share','share.sharefeed','suggest.recipe','suggest.recipead','suggest.recipev3','suggest.top')
		group by logdate,appid
		"""
		
		cursor = self.hive.execute(sql%self.curDate)
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					platform = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_recipe_recipe_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,0)"
					self.db.execute(sql%(createtime,platform,cnt,pnt))
		
		#菜谱详情页
		sql = "delete from bi_recipe_recipe_analysis where createtime='%s' and type =1"
		self.db.execute(sql%self.curDate)
		
		#web		
		sql = """\
		select w.logdate,count(w.remote_addr),count(distinct(concat(w.remote_addr,'-',w.http_user_agent))) 
		from www_haodou_com w
		inner join hd_haodou_recipe_%s.recipe r on r.recipeid=grifurl(w.path)
		where w.logdate = '%s'
		group by w.logdate;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_recipe_recipe_analysis (createtime,platform,total,total_person,type) VALUES ('%s',0,%s,%s,1)"
					self.db.execute(sql%(createtime,cnt,pnt))
					
					
		#app
		sql = """\
		select to_date(logdate) logdate,appid,count(concat(device_id,uuid)),count(distinct(concat(device_id,uuid)))
		from log_php_app_log
		where logdate = '%s' and appid in (2,4,6) and function_id in ('info.getinfo','info.getinfov2','info.getinfov3','info.getlastestinfo','info.randrecipeinfo')
		group by logdate,appid
		"""
		
		cursor = self.hive.execute(sql%self.curDate)
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					platform = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_recipe_recipe_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,1)"
					self.db.execute(sql%(createtime,platform,cnt,pnt))
		
		#菜谱发布数
		sql = "delete from bi_recipe_recipe_analysis where createtime='%s' and type =2"
		self.db.execute(sql%self.curDate)
		
		
		sql = """\
		select to_date(createtime),postfrom,count(recipeid),count(distinct(userid))
		from hd_haodou_recipe_%s.recipe
		where to_date(createtime) = '%s' and status in (0,10)
		group by to_date(createtime),postfrom;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					platform = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_recipe_recipe_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,2)"
					self.db.execute(sql%(createtime,platform,cnt,pnt))
					
		#通过审核菜谱数
		sql = "delete from bi_recipe_recipe_analysis where createtime='%s' and type =3"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(createtime),postfrom,count(recipeid),count(distinct(userid))
		from hd_haodou_recipe_%s.recipe
		where to_date(createtime) = '%s' and status=0
		group by to_date(createtime),postfrom;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					platform = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_recipe_recipe_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,3)"
					self.db.execute(sql%(createtime,platform,cnt,pnt))
		
		#1星/2星/3星/4星/5星/收录菜谱数
		
		sql = "delete from bi_recipe_recipe_analysis where createtime='%s' and type =4"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(createtime),postfrom,floor(rate),count(recipeid) 
		from hd_haodou_recipe_%s.recipe
		where record=1 and status=0 and floor(rate) in (1,2,3,4,5)
		and to_date(createtime) = '%s'
		group by to_date(createtime),postfrom,rate;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) == 4:
					createtime = str(row[0])
					platform = int(row[1])
					rate = int(row[2])
					cnt = int(row[3])
					
					sql = "INSERT INTO bi_recipe_recipe_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,4)"
					self.db.execute(sql%(createtime,platform,rate,cnt))
		
		#菜谱评论数
			
		sql = "delete from bi_recipe_recipe_analysis where createtime='%s' and type =5"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(createtime),platform,count(commentid),count(distinct(userid))
		from hd_haodou_comment_%s.comment
		where type=0 and status=1 and to_date(createtime) = '%s'
		group by to_date(createtime),platform;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					platform = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_recipe_recipe_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,5)"
					self.db.execute(sql%(createtime,platform,cnt,pnt))
		
		#菜谱收藏数
		
		sql = "delete from bi_recipe_recipe_analysis where createtime='%s' and type =6"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(createtime) createtime,sum(cnt),sum(pnt) from (
		select to_date(createtime) createtime,count(concat(albumid,'-',recipeid)) cnt, 0 as pnt
		from hd_haodou_recipe_albumrecipe_%s.albumrecipe
		where to_date(createtime) = '%s'
		group by to_date(createtime)
		union all
		select to_date(ar.createtime) createtime, 0 as cnt, count(distinct(a.userid)) pnt
		from hd_haodou_recipe_%s.album a
		inner join hd_haodou_recipe_albumrecipe_%s.albumrecipe ar on ar.albumid=a.albumid
		where to_date(ar.createtime) = '%s'
		group by to_date(ar.createtime)
		) tmp
		group by to_date(createtime);
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate,self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_recipe_recipe_analysis (createtime,platform,total,total_person,type) VALUES ('%s',-1,%s,%s,6)"
					self.db.execute(sql%(createtime,cnt,pnt))
					
		
		#菜谱喜欢数		
		sql = "delete from bi_recipe_recipe_analysis where createtime='%s' and type =7"
		self.db.execute(sql%self.curDate)
		
		
		#web
		sql = """\
		select w.logdate,count(w.remote_addr),count(distinct(concat(w.remote_addr,'-',w.http_user_agent)))  
		from www_haodou_com w
		inner join hd_haodou_recipe_""" + self.nowDate + """.recipe r on r.recipeid=grifurl(w.referer)
		where w.logdate = '""" + self.curDate + """' and path like '/recipe/ajax.php?do=like%'
		group by w.logdate;
		"""
		
		cursor = self.hive.execute(sql)
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_recipe_recipe_analysis (createtime,platform,total,total_person,type) VALUES ('%s',0,%s,%s,7)"
					self.db.execute(sql%(createtime,cnt,pnt))
		
		
		#app
		sql = """\
		select log.logdate,log.appid,count(concat(log.device_id,log.uuid)),count(distinct(concat(log.device_id,log.uuid)))
		from log_php_app_log log
		inner join hd_haodou_recipe_%s.recipe r on r.recipeid=gmifp(log.parameter_desc)
		where log.logdate = '%s' and log.appid in (2,4,6) and log.function_id = 'like.add'
		group by log.logdate,log.appid;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					platform = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_recipe_recipe_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,7)"
					self.db.execute(sql%(createtime,platform,cnt,pnt))
		
		#菜谱分享数
		sql = "delete from bi_recipe_recipe_analysis where createtime='%s' and type =8"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(createtime) createtime,count(feedid),count(distinct(userid))
		from hd_haodou_center_%s.userfeed
		where to_date(createtime) = '%s' and type in (110)
		group by to_date(createtime);
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_recipe_recipe_analysis (createtime,platform,total,total_person,type) VALUES ('%s',-1,%s,%s,8)"
					self.db.execute(sql%(createtime,cnt,pnt))
					
					
		#新增菜谱人数
		#每天看发布菜谱的用户，再看之前有没有发布过菜谱，如果没有发布过，就算做是新增菜谱人数
		
		sql = "delete from bi_recipe_recipe_person_analysis where createtime='%s' and type=0"
		self.db.execute(sql%self.curDate)
		
		
		#找今天发布作品的用户
		sql = """\
		select distinct(userid) from hd_haodou_recipe_%s.recipe where to_date(createtime) = '%s' and status in (0,10)
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
		select userid,count(id) from hd_haodou_recipe_%s.recipe where to_date(createtime) > '%s' and status in (0,10)
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
		
		#找今日发布的菜谱数	
		sql = """\
		select u.username,r.userid,to_date(u.regtime),r.postfrom,count(r.recipeid)
		from hd_haodou_recipe_%s.recipe r
		left outer join hd_haodou_passport_%s.user u on u.userid=r.userid
		where r.status in (0,10) and to_date(r.createtime) = '%s' and r.userid in (%s)
		group by u.username,r.userid,to_date(u.regtime),r.postfrom;
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
					
					sql = "INSERT INTO bi_recipe_recipe_person_analysis (createtime,regtime,userid,username,platform,count,type) VALUES ('%s','%s',%s,'%s',%s,%s,0)"
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
	es = RecipeAnalysis(today)
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