# -*- coding: UTF-8 -*- 
#coding = utf-8
#客浦创意月饼活动数据收集统计

from optparse import OptionParser
import commands
from time import ctime
from MailUtil import *
from XLSWriter import XLSWriter
from hiveDB import *
from sparkDB import *
from timeUtils import *
import traceback
from  datetime  import  *
import time
import csv
import sys
import os
import re



class RecipeGroupTopicDataSummary:

	def __init__(self, topicid="0", keyword="", start="", end=""):

		dt = time.strptime(end+" 23:59:59",'%Y-%m-%d %H:%M:%S')
		dt = datetime(*dt[:3])
		dt = dt + timedelta(days = 1)
		
		self.tu = TimeUtils()
		
		self._datetime = dt - timedelta(days = 1)
		self.nowDate = dt.strftime("%Y%m%d")
		self.curDate = self._datetime.strftime("%Y-%m-%d")
		
		self.keyword = keyword
		
		#开始时间
		self.startDate = start
		#结束时间
		self.endDate = end
		self.serialDate = []
		self.topicData = {}
		
		self.hive = HiveDB("--define date=" + self.nowDate + " --define topicid=" + topicid + " --define keyword=" + self.keyword + " --define sd=" + self.startDate + " --define ed=" + self.endDate)
		#self.spark = SparkDB()
		
		path  = os.path.dirname(os.path.abspath(__file__))
		self.file_path = path + "/" + self._datetime.strftime("%Y%m") + "/%s"
		
		self.usersTime = {}
		
	def start(self):
		
		print ctime()
		
		sd = time.strptime(self.startDate,'%Y-%m-%d')
		ed = time.strptime(self.endDate,'%Y-%m-%d')
		
		begin = datetime(*sd[:3])
		end = datetime(*ed[:3])		
		delta = end-begin
		
		self.topicData[self.startDate] = {}
		for day in range(1,delta.days,1):
			date = (begin + timedelta(days = day)).strftime("%Y-%m-%d")
			self.topicData[date] = {}
		self.topicData[self.endDate] = {}
		
		
		self.serialDate.append(self.startDate)
		for day in range(1,delta.days,1):
			date = (begin + timedelta(days = day)).strftime("%Y-%m-%d")
			self.serialDate.append(date)
		self.serialDate.append(self.endDate)
		
		self.total()
		self.detail()
		self.topicSummary()
		self.continueUser()
		self.share()
		self.invite()
		
		self.saveXLS()
		
		print ctime()
	
	def total(self):
	
		
		print "发帖数、发帖人数"
		
		sql = """ \
		select to_date(createtime) createtime,count(topicid),count(distinct(userid)) from hd_haodou_center_\${date}.grouptopic 
		where (title like '%\${keyword}%' or content like '%\${keyword}%') and to_date(createtime) between '\${sd}' and '\${ed}'
		group by to_date(createtime)
		"""
		
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				date = str(row[0])
				if date in self.topicData:
					c = int(row[1])
					u = int(row[2])
					self.topicData[date]["topic"] = c
					self.topicData[date]["topicusers"] = u
				else:
					self.topicData[date]["topic"] = 0
					self.topicData[date]["topicusers"] = 0
				
		print "回复数、回复人数"
		
		sql = """ \
		select to_date(a.createtime) createtime,count(distinct(a.commentId)),count(distinct(a.userid)) from hd_haodou_center_\${date}.grouptopic c
		inner join hd_haodou_comment_\${date}.comment a on a.itemid=c.topicid
		where to_date(c.createtime) between '\${sd}' and '\${ed}' and c.status =1
		and a.status=1 and a.type=6 and to_date(a.createtime) between '\${sd}' and '\${ed}'
		and (c.title like '%\${keyword}%' or c.content like '%\${keyword}%') group by to_date(a.createtime)
		"""
		
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				date = str(row[0])
				if date in self.topicData:
					c = int(row[1])
					u = int(row[2])
					self.topicData[date]["comment"] = c
					self.topicData[date]["commentusers"] = u
				else:
					self.topicData[date]["comment"] = 0
					self.topicData[date]["commentusers"] = 0
		
		
		print "总PV、总UV"
		
		sql = """\
		select m.logdate,count(m.remote_addr),count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) 
		from logs.m_haodou_com m
		inner join hd_haodou_center_\${date}.grouptopic g on g.topicid=gtifurl(m.path)
		where m.logdate between '\${sd}' and '\${ed}' 
		and g.status =1 and (g.title like '%\${keyword}%' or g.content like '%\${keyword}%')
		group by m.logdate
		"""
		
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				date = str(row[0])
				if date in self.topicData:
					c = int(row[1])
					u = int(row[2])
					self.topicData[date]["pv"] = c
					self.topicData[date]["uv"] = u
				else:
					self.topicData[date]["pv"] = 0
					self.topicData[date]["uv"] = 0
		
		
		sql = """\
		select m.logdate,count(m.remote_addr),count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) 
		from logs.group_haodou_com m
		inner join hd_haodou_center_\${date}.grouptopic g on g.topicid=gtifurl(m.path)
		where m.logdate between '\${sd}' and '\${ed}' 
		and g.status =1 and (g.title like '%\${keyword}%' or g.content like '%\${keyword}%')
		group by m.logdate
		"""
		
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				date = str(row[0])
				if date in self.topicData:
					c = int(row[1])
					u = int(row[2])
					self.topicData[date]["pv"] = self.topicData[date]["pv"] + c
					self.topicData[date]["uv"] = self.topicData[date]["uv"] + u
				else:
					self.topicData[date]["pv"] = 0
					self.topicData[date]["uv"] = 0
	
	def detail(self):
		
		print "网站端PV UV"
		
		sql = """ \
		select m.logdate,count(m.remote_addr),count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) 
		from logs.group_haodou_com m
		inner join hd_haodou_center_\${date}.grouptopic g on g.topicid=gtifurl(m.path)
		where m.logdate between '\${sd}' and '\${ed}' 
		and g.status =1 and (g.title like '%\${keyword}%' or g.content like '%\${keyword}%')
		and m.referer like '%haodou.com%'
		group by m.logdate
		"""
		
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				date = str(row[0])
				if date in self.topicData:
					c = int(row[1])
					u = int(row[2])
					self.topicData[date]["webpv"] = c
					self.topicData[date]["webuv"] = u
				else:
					self.topicData[date]["webpv"] = 0
					self.topicData[date]["webuv"] = 0
		
		print "移动端PV UV"
		
		sql = """ \
		select m.logdate,count(m.remote_addr),count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) 
		from logs.m_haodou_com m
		inner join hd_haodou_center_\${date}.grouptopic g on g.topicid=gtifurl(m.path)
		where m.logdate between '\${sd}' and '\${ed}' 
		and g.status =1 and (g.title like '%\${keyword}%' or g.content like '%\${keyword}%')
		and m.referer not like '%mp.weixin.qq.com%'
		group by m.logdate
		"""
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				date = str(row[0])
				if date in self.topicData:
					c = int(row[1])
					u = int(row[2])
					self.topicData[date]["mpv"] = c
					self.topicData[date]["muv"] = u
				else:
					self.topicData[date]["mpv"] = 0
					self.topicData[date]["muv"] = 0
					
		print "微信PV UV"
		
		sql = """ \
		select m.logdate,count(m.remote_addr),count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) 
		from logs.m_haodou_com m
		inner join hd_haodou_center_\${date}.grouptopic g on g.topicid=gtifurl(m.path)
		where m.logdate between '\${sd}' and '\${ed}' 
		and g.status =1 and (g.title like '%\${keyword}%' or g.content like '%\${keyword}%')
		and m.referer like '%mp.weixin.qq.com%'
		group by m.logdate
		"""
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				date = str(row[0])
				if date in self.topicData:
					c = int(row[1])
					u = int(row[2])
					self.topicData[date]["wpv"] = c
					self.topicData[date]["wuv"] = u
				else:
					self.topicData[date]["wpv"] = 0
					self.topicData[date]["wuv"] = 0
		
		sql = """\
		select m.logdate,count(m.remote_addr),count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) 
		from logs.group_haodou_com m
		inner join hd_haodou_center_\${date}.grouptopic g on g.topicid=gtifurl(m.path)
		where m.logdate between '\${sd}' and '\${ed}' 
		and g.status =1 and (g.title like '%\${keyword}%' or g.content like '%\${keyword}%')
		and m.referer like '%mp.weixin.qq.com%'
		group by m.logdate
		"""
		
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				date = str(row[0])
				if date in self.topicData:
					c = int(row[1])
					u = int(row[2])
					if "wpv" in self.topicData[date]:
						self.topicData[date]["wpv"] = self.topicData[date]["wpv"] + c
					else:
						self.topicData[date]["wpv"] = c
					if "wuv" in self.topicData[date]:
						self.topicData[date]["wuv"] = self.topicData[date]["wuv"] + u
					else:
						self.topicData[date]["wuv"] = u
				else:
					self.topicData[date]["wpv"] = 0
					self.topicData[date]["wuv"] = 0
					
					
		print "QQ空间PV UV"
		
		sql = """ \
		select m.logdate,count(m.remote_addr),count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) 
		from logs.m_haodou_com m
		inner join hd_haodou_center_\${date}.grouptopic g on g.topicid=gtifurl(m.path)
		where m.logdate between '\${sd}' and '\${ed}' 
		and g.status =1 and (g.title like '%\${keyword}%' or g.content like '%\${keyword}%')
		and (m.referer like '%qzs.qq.com%' or m.referer like '%qzone%')
		group by m.logdate
		"""
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				date = str(row[0])
				if date in self.topicData:
					c = int(row[1])
					u = int(row[2])
					self.topicData[date]["qqpv"] = c
					self.topicData[date]["qquv"] = u
				else:
					self.topicData[date]["qqpv"] = 0
					self.topicData[date]["qquv"] = 0
		
		sql = """\
		select m.logdate,count(m.remote_addr),count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) 
		from logs.group_haodou_com m
		inner join hd_haodou_center_\${date}.grouptopic g on g.topicid=gtifurl(m.path)
		where m.logdate between '\${sd}' and '\${ed}' 
		and g.status =1 and (g.title like '%\${keyword}%' or g.content like '%\${keyword}%')
		and (m.referer like '%qzs.qq.com%' or m.referer like '%qzone%')
		group by m.logdate
		"""
		
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				date = str(row[0])
				if date in self.topicData:
					c = int(row[1])
					u = int(row[2])
					if "qqpv" in self.topicData[date]:
						self.topicData[date]["qqpv"] = self.topicData[date]["qqpv"] + c
					else:
						self.topicData[date]["qqpv"] = c
					if "qquv" in self.topicData[date]:
						self.topicData[date]["qquv"] = self.topicData[date]["qquv"] + u
					else:
						self.topicData[date]["qquv"] = u
				else:
					self.topicData[date]["qqpv"] = 0
					self.topicData[date]["qquv"] = 0			
					
		print "腾讯微博PV UV"
		
		sql = """ \
		select m.logdate,count(m.remote_addr),count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) 
		from logs.m_haodou_com m
		inner join hd_haodou_center_\${date}.grouptopic g on g.topicid=gtifurl(m.path)
		where m.logdate between '\${sd}' and '\${ed}' 
		and g.status =1 and (g.title like '%\${keyword}%' or g.content like '%\${keyword}%')
		and m.referer like '%t.qq.com%'
		group by m.logdate
		"""
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				date = str(row[0])
				if date in self.topicData:
					c = int(row[1])
					u = int(row[2])
					self.topicData[date]["tpv"] = c
					self.topicData[date]["tuv"] = u
				else:
					self.topicData[date]["tpv"] = 0
					self.topicData[date]["tuv"] = 0
					
		
		sql = """\
		select m.logdate,count(m.remote_addr),count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) 
		from logs.group_haodou_com m
		inner join hd_haodou_center_\${date}.grouptopic g on g.topicid=gtifurl(m.path)
		where m.logdate between '\${sd}' and '\${ed}' 
		and g.status =1 and (g.title like '%\${keyword}%' or g.content like '%\${keyword}%')
		and m.referer like '%t.qq.com%'
		group by m.logdate
		"""
		
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				date = str(row[0])
				if date in self.topicData:
					c = int(row[1])
					u = int(row[2])
					if "tpv" in self.topicData[date]:
						self.topicData[date]["tpv"] = self.topicData[date]["tpv"] + c
					else:
						self.topicData[date]["tpv"] = c	
					if "tuv" in self.topicData[date]:
						self.topicData[date]["tuv"] = self.topicData[date]["tuv"] + u
					else:
						self.topicData[date]["tuv"] = u
				else:
					self.topicData[date]["tpv"] = 0
					self.topicData[date]["tuv"] = 0
					
		print "新浪微博PV UV"
		
		sql = """ \
		select m.logdate,count(m.remote_addr),count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) 
		from logs.m_haodou_com m
		inner join hd_haodou_center_\${date}.grouptopic g on g.topicid=gtifurl(m.path)
		where m.logdate between '\${sd}' and '\${ed}' 
		and g.status =1 and (g.title like '%\${keyword}%' or g.content like '%\${keyword}%')
		and m.referer like '%weibo%'
		group by m.logdate
		"""
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				date = str(row[0])
				if date in self.topicData:
					c = int(row[1])
					u = int(row[2])
					self.topicData[date]["xpv"] = c
					self.topicData[date]["xuv"] = u
				else:
					self.topicData[date]["xpv"] = 0
					self.topicData[date]["xuv"] = 0
		
		sql = """\
		select m.logdate,count(m.remote_addr),count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) 
		from logs.group_haodou_com m
		inner join hd_haodou_center_\${date}.grouptopic g on g.topicid=gtifurl(m.path)
		where m.logdate between '\${sd}' and '\${ed}' 
		and g.status =1 and (g.title like '%\${keyword}%' or g.content like '%\${keyword}%')
		and m.referer like '%weibo%'
		group by m.logdate
		"""
		
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				date = str(row[0])
				if date in self.topicData:
					c = int(row[1])
					u = int(row[2])
					if "xpv" in self.topicData[date]:
						self.topicData[date]["xpv"] = self.topicData[date]["xpv"] + c
					else:
						self.topicData[date]["xpv"] = c
					if "xuv" in self.topicData[date]:
						self.topicData[date]["xuv"] = self.topicData[date]["xuv"] + u
					else:
						self.topicData[date]["xuv"] = u
				else:
					self.topicData[date]["xpv"] = 0
					self.topicData[date]["xuv"] = 0
		
	def topicSummary(self):
		
		print "%s-%s标题或话题内容带%s的话题"%(self.startDate,self.endDate,self.keyword)
		
		sql = """ \
		select userid,'#',username,'#',title,'#',concat('http://group.haodou.com/topic-',topicid,'.html') topicid,'#',createtime,'#',viewcount,'#',sum(comment),'#',sum(cusers),'#',sum(favorite),'#',
		case 
		when recommend=1 then '推荐'
		when recommend=0 then ''
		end as recommend,'#',
		case 
		when digest=1 then '精华'
		when digest=0 then ''
		end as digest,'#',sum(wealth)+if(digest=1,100,0)+if(recommend=1,50,0) wealth from (
		select g.userid,u.username,g.title,g.topicid,g.createtime,g.viewcount,
		0 as comment, 0 as cusers, sum(g.favoritecount) favorite, g.recommend, g.digest, sum(w.wealth) wealth
		from hd_haodou_center_\${date}.grouptopic g
		left outer join hd_haodou_passport_\${date}.user u on u.userid=g.userid
		inner join hd_haodou_center_\${date}.topicwealthlog w on (w.rcvuserid=g.userid and w.topicid=g.topicid)
		where to_date(g.createtime) between '\${sd}' and '\${ed}' and g.status=1
		and to_date(w.createtime) between '\${sd}' and '\${ed}'
		and (g.title like '%\${keyword}%' or g.content like '%\${keyword}%')
		group by g.userid,u.username,g.title,g.topicid,g.createtime,g.viewcount,g.recommend, g.digest
		union all
		select g.userid,u.username,g.title,g.topicid,g.createtime,g.viewcount,
		count(c.commentid) comment, count(distinct(c.userid)) cusers, 0 as favorite, g.recommend, g.digest, 0 as wealth
		from hd_haodou_center_\${date}.grouptopic g
		left outer join hd_haodou_passport_\${date}.user u on u.userid=g.userid
		inner join hd_haodou_comment_\${date}.comment c on c.itemid=g.topicid
		where g.status=1 and c.type=6 and c.status =1 and c.replyid=0
		and to_date(c.createtime) between '\${sd}' and '\${ed}'
		and to_date(g.createtime) between '\${sd}' and '\${ed}'
		and (g.title like '%\${keyword}%' or g.content like '%\${keyword}%')
		group by g.userid,u.username,g.title,g.topicid,g.createtime,g.viewcount,g.recommend, g.digest
		) tmp
		group by userid,username,title,topicid,createtime,recommend,digest;
		"""
		
		self.topicData["topicsdata"] = []
		
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"#",cols)
			
			if len(row) > 1:
				data = {}
				data["userid"] = int(row[0].strip())
				data["username"] = row[1].strip()
				data["title"] = row[2].strip()
				data["web"] = row[3].strip()
				data["createtime"] = row[4]
				data["viewcount"] = int(row[5].strip())
				data["comment"] = int(row[6].strip())
				data["commentusers"] = int(row[7].strip())
				data["favorite"] = int(row[8].strip())
				data["recommend"] = row[9].strip()
				data["digest"] = row[10].strip()
				data["wealth"] = int(row[11].strip())
							
				self.topicData["topicsdata"].append(data)
	
	def continueUser(self):
		
		print "连续发布话题天数"
		
		#1.获取所有在指定时间范围内发过指定帖子的用户，按发布日期聚合
		#2.按连续日期循环，如果在指定的日期内某用户有记录则+1，没有的则用户的连续记录被打断，此记录继续累计
		#3.汇总取出每个用户连续天数最大的那个值作为用户最终连续天数
		
		sql = """\
		select g.userid,to_date(g.createtime),u.username,count(g.topicid) 
		from hd_haodou_center_\${date}.grouptopic g
		left outer join hd_haodou_passport_\${date}.user u on u.userid=g.userid
		where (g.title like '%\${keyword}%' or g.content like '%\${keyword}%')
		and to_date(g.createtime) between '\${sd}' and '\${ed}' and g.status=1
		group by g.userid,to_date(g.createtime),u.username
		"""
		
		dateUserTopic = {}
		userids = {}
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				
				userid = str(row[0])
				date = str(row[1])
				username = str(row[2])
				
				dateUserTopic[userid+"_"+date] = userid+"_"+date
				if userid not in userids:
					userids[userid]	= {}
					userids[userid]["temp"] = 1
					userids[userid]["top"] = 0
					userids[userid]["perday"] = ""
					userids[userid]["days"] = 0
					userids[userid]["username"] = username
		
		for userid in userids.keys():
			for date in self.serialDate:
				#只比较存在日期的
				if userid+"_"+date in dateUserTopic:
					if userids[userid]["perday"] != "":
					
						sd = time.strptime(userids[userid]["perday"],'%Y-%m-%d')
						ed = time.strptime(date,'%Y-%m-%d')
						
						begin = datetime(*sd[:3])
						end = datetime(*ed[:3])		
						delta = end-begin
						userids[userid]["days"] = delta.days
					
					#判断连续天
					if int(userids[userid]["days"]) == 1:
						userids[userid]["temp"] = userids[userid]["temp"] + 1
						
					else:
						temp = userids[userid]["temp"]
						#取最大
						if userids[userid]["top"] < temp:
							userids[userid]["top"] = temp
							userids[userid]["days"] = 0
						userids[userid]["temp"] = 1
						
					userids[userid]["perday"] = date
				
			temp = userids[userid]["temp"]
			if userids[userid]["top"] < temp:
				userids[userid]["top"] = temp
	
		self.topicData["userids"] = userids
	
	
	def	share(self):
		
		sql = """\
		select commentId,username,userid,type,tc from (
		select c.commentId,u.username,c.userid,'有图片' as type,gcpc(c.content) tc
		from hd_haodou_comment_\${date}.comment c
		left outer join hd_haodou_passport_\${date}.user u on c.userid=u.userid
		where c.itemid=\${topicid} and c.status=1 and c.type=6 and c.replyid=0
		and to_date(c.createtime) between '\${sd}' and '\${ed}'
		and gpfc(c.content) = 1
		union all
		select c.commentId,u.username,c.userid,'无图片' as type,gcpc(c.content) tc
		from hd_haodou_comment_\${date}.comment c
		left outer join hd_haodou_passport_\${date}.user u on c.userid=u.userid
		where c.itemid=\${topicid} and c.status=1 and c.type=6 and c.replyid=0
		and to_date(c.createtime) between '\${sd}' and '\${ed}'
		and gpfc(c.content) = 0
		) tmp
		order by commentId;
		"""
		
		shares = []
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				cid = int(row[0])
				
				share = {}
				share["username"] = str(row[1])
				share["userid"] = int(row[2])
				share["type"] = str(row[3])
				share["pcount"] = int(row[4])
				
				shares.append(share)
		
		self.topicData["share"] = shares
	
	#导出用户通过该页面邀请好友注册好豆的信息
	def invite(self):
		
		
		
		sql = """\
		select ui.userid,u.username,count(g.topicid),tmp.invite,tmp.username,tmp.it from (
		select u.userid invite,u.username,count(g.topicid) as it from hd_haodou_center_\${date}.userinvite ui
		inner join hd_haodou_passport_\${date}.user u on u.userid=ui.inviteuserid
		inner join hd_haodou_center_\${date}.grouptopic g on g.userid=ui.inviteuserid
		where to_date(u.regtime) between '2014-10-01' and '2014-11-30'
		group by u.userid,u.username
		) tmp
		inner join hd_haodou_center_\${date}.userinvite ui on ui.inviteuserid=tmp.invite
		inner join hd_haodou_passport_\${date}.user u on u.userid=ui.userid
		inner join hd_haodou_center_\${date}.grouptopic g on g.userid=ui.userid
		group by ui.userid,u.username,tmp.invite,tmp.username,tmp.it;
		"""
		
		invite = []
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				user = {}
				user["userid"] = int(row[0])
				user["username"] = str(row[1])
				user["cnt"] = int(row[2])
				user["invite"] = int(row[3])
				user["inviteusername"] = str(row[4])
				user["invitecnt"] = int(row[5])
				
				invite.append(user)
				
		
		self.topicData["invite"] = invite
					
	def eightFloor(self):
		
		sql = """\
		SELECT topicid,commentid,rank
		FROM (SELECT topicid,commentid,hrank(topicid) rank
    FROM (SELECT g.topicid,c.commentid
                FROM hd_haodou_comment_\${date}.comment c
                inner join hd_haodou_center_\${date}.grouptopic g on g.topicid=c.itemid 
                where c.status=1 and c.type=6 and c.replyid=0 and g.status=1 
                and to_date(g.createtime) between '\${sd}' and '\${ed}' 
                and to_date(c.createtime) between '\${sd}' and '\${ed}' 
                DISTRIBUTE BY g.topicid SORT BY concat(g.topicid,c.commentid)
              )  t1) 
    t2
		WHERE rank =7;
		"""
		
		commentstr = ""
		comments = {}
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				user = {}
				user["topicid"] = int(row[0])
				user["commentid"] = int(row[1])
				if user["commentid"] not in comments:
					commentstr += str(user["commentid"]) + ","
					comments[user["commentid"]] = user["commentid"]
					
		
		if "" != commentstr:
			commentstr = commentstr[0:len(commentstr)-1]
		
		sql = """\
		select /*+ mapjoin(c)*/
		c.userid,'#',u.username,'#',g.title,'#',concat('http://group.haodou.com/topic-',g.topicid,'.html'),'#',c.createtime,'#',html2text(c.content)
		from (select userid, itemid, createtime, content
		from hd_haodou_comment_%s.comment
		where createtime between '%s' and '%s' and status=1 and type=6 and commentid in (%s)
		) c
		inner join hd_haodou_center_%s.grouptopic g on g.topicid=c.itemid
		left outer join hd_haodou_passport_%s.user u on u.userid=c.userid;
		"""
		
		eightFloorUser = []
		
		cursor = self.hive.execute(sql%(self.nowDate,self.startDate,self.endDate,commentstr,self.nowDate,self.nowDate))
		for cols in cursor:
			row = re.split(r"#",cols)
			if len(row) > 1:
				data = {}
				data["userid"] = int(row[0].strip())
				data["username"] = str(row[1].strip())
				data["title"] = str(row[2].strip())
				data["web"] = str(row[3].strip())
				data["createtime"] = str(row[4].strip())
				data["content"] = str(row[5].strip())
				eightFloorUser.append(data)
				
		
		self.topicData["eightFloorUser"] = eightFloorUser
		
		self.saveXLS()
	
	def endOfYear(self):
				
		sql = """\
		SELECT userid,'#',username,'#',
		case 
		when cateid=5 then '乐在厨房'
		when cateid=6 then '营养健康'
		when cateid=8 then '厨房宝典'
		when cateid=12 then '好好生活'
		when cateid=11 then '摄影天地'
		when cateid=10 then '游山玩水'
		when cateid=23 then '亲子乐园'
		when cateid=27 then '豆有所好'
		end as cateid,'#',title,'#',concat('http://group.haodou.com/topic-',topicid,'.html') topicid,'#',createtime,'#',viewcount,'#',comment,'#',mcomment,'#',cusers,'#',favorite,'#',
		case 
		when recommend=1 then '推荐'
		when recommend=0 then ''
		end as recommend,'#',
		case 
		when digest=1 then '精华'
		when digest=0 then ''
		end as digest,'#',rank FROM   
		(
		SELECT userid,username,cateid,title,topicid,createtime,viewcount,comment,mcomment,cusers,favorite,recommend,digest,hrank(cateid) rank FROM (
		select userid,username,cateid,title,topicid,createtime,viewcount,sum(comment) comment,sum(mcomment) mcomment,sum(cusers) cusers,sum(favorite) favorite,recommend,digest, sum(comment) cnt from (
		select g.userid,u.username,g.cateid,g.title,g.topicid,g.createtime,g.viewcount,0 as comment,0 as mcomment, 0 as cusers, sum(g.favoritecount) favorite, g.recommend, g.digest
		from hd_haodou_center_20141211.grouptopic g
		left outer join hd_haodou_passport_20141211.user u on u.userid=g.userid
		where to_date(g.createtime) between '2014-01-01' and '2014-12-10' and g.status=1 
		and (g.recommend=1 or g.digest=1) and g.title not like '%抢楼%'
		and g.cateid in (5,6,8,12,11,10,23,27)
		group by g.userid,u.username,g.cateid,g.title,g.topicid,g.createtime,g.viewcount,g.recommend, g.digest
		union all
		select g.userid,u.username,g.cateid,g.title,g.topicid,g.createtime,g.viewcount,count(distinct(c.commentid)) comment, 0 as mcomment, count(distinct(c.userid)) cusers, 0 as favorite, g.recommend, g.digest
		from hd_haodou_center_20141211.grouptopic g
		left outer join hd_haodou_passport_20141211.user u on u.userid=g.userid
		inner join hd_haodou_comment_20141211.comment c on c.itemid=g.topicid
		where g.status=1 and c.type=6 and c.status =1 and c.replyid=0
		and to_date(c.createtime) between '2014-01-01' and '2014-12-10'
		and to_date(g.createtime) between '2014-01-01' and '2014-12-10'
		and (g.recommend=1 or g.digest=1) and g.title not like '%抢楼%'
		and g.cateid in (5,6,8,12,11,10,23,27)
		group by g.userid,u.username,g.cateid,g.title,g.topicid,g.createtime,g.viewcount,g.recommend, g.digest
		union all
		select g.userid,u.username,g.cateid,g.title,g.topicid,g.createtime,g.viewcount,0 as comment,count(distinct(c.commentid)) mcomment, 0 as cusers, 0 as favorite, g.recommend, g.digest
		from hd_haodou_center_20141211.grouptopic g
		left outer join hd_haodou_passport_20141211.user u on u.userid=g.userid
		inner join hd_haodou_comment_20141211.comment c on c.itemid=g.topicid
		where g.status=1 and c.type=6 and c.status =1 and c.replyid=0
		and to_date(c.createtime) between '2014-01-01' and '2014-12-10'
		and to_date(g.createtime) between '2014-01-01' and '2014-12-10'
		and (g.recommend=1 or g.digest=1) and g.title not like '%抢楼%'
		and g.cateid in (5,6,8,12,11,10,23,27) and length(html2text(c.content))>=3 and c.platform in(1,2,3)
		group by g.userid,u.username,g.cateid,g.title,g.topicid,g.createtime,g.viewcount,g.recommend, g.digest
		) tmp
		group by userid,username,cateid,title,topicid,createtime,viewcount,recommend,digest
		DISTRIBUTE BY cateid SORT BY cnt desc
		) tmp
		) tmp
		WHERE rank <50;
		"""		
		
		datas = []
		
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"#",cols)
			if len(row) > 1:
				data = {}
				data["userid"] = int(row[0].strip())
				data["username"] = str(row[1].strip())
				data["cateid"] = str(row[2].strip())
				data["title"] = str(row[3].strip())
				data["web"] = str(row[4].strip())
				data["createtime"] = str(row[5].strip())
				data["viewcount"] = int(row[6].strip())
				data["comment"] = int(row[7].strip())
				data["mcomment"] = int(row[8].strip())
				data["cusers"] = int(row[9].strip())
				data["favorite"] = int(row[10].strip())
				data["recommend"] = str(row[11].strip())
				data["digest"] = str(row[12].strip())
				data["rank"] = int(row[13].strip())
				
				datas.append(data)
				
		self.topicData["endOfYear"] = []
		self.topicData["endOfYear"].append(datas)
		
		
		sql = """\
		select tmp.userid,'#',u.username,'#',concat('http://wo.haodou.com/',tmp.userid,'/') web,'#',tmp.cnt cnt,'#',tmp.recommend,'#',tmp.digest,'#',tmp.life,'#',tmp.food,'#',tmp.alive,'#',tmp.mcomment,'#',tmp.comment,'#',tmp.ac,'#',tmp.topc,'#',ut.mindate,'#',ut.maxdate from (
		select userid,sum(cnt) cnt,sum(recommend) recommend,sum(digest) digest,sum(life) life,sum(food) food,sum(alive) alive,sum(mcomment) mcomment,sum(comment) comment,sum(ac) ac,sum(topc) topc from(
		select userid,count(topicid) cnt,0 as recommend,0 as digest,0 as life,0 as food,0 as alive,0 as mcomment,0 as comment,0 as ac,0 as topc from hd_haodou_center_20141211.grouptopic
		where status=1 and to_date(createtime) between '2014-01-01' and '2014-12-10'
		group by userid
		union all
		select userid,0 as cnt,count(topicid) recommend,0 as digest,0 as life,0 as food,0 as alive,0 as mcomment,0 as comment,0 as ac,0 as topc from hd_haodou_center_20141211.grouptopic 
		where recommend=1 and status=1 and to_date(createtime) between '2014-01-01' and '2014-12-10'
		group by userid
		union all
		select userid,0 as cnt,0 as recommend,count(topicid) digest,0 as life,0 as food,0 as alive,0 as mcomment,0 as comment,0 as ac,0 as topc from hd_haodou_center_20141211.grouptopic 
		where digest=1 and status=1 and to_date(createtime) between '2014-01-01' and '2014-12-10'
		group by userid
		union all
		select userid,0 as cnt,0 as recommend,0 as  digest,count(topicid) life,0 as food,0 as alive,0 as mcomment,0 as comment,0 as ac ,0 as topc from hd_haodou_center_20141211.grouptopic 
		where cateid in (12,11,10,23,27) and status=1 and to_date(createtime) between '2014-01-01' and '2014-12-10'
		group by userid
		union all
		select userid,0 as cnt,0 as recommend,0 as  digest,0 as life,count(topicid) food,0 as alive,0 as mcomment,0 as comment,0 as ac,0 as topc from hd_haodou_center_20141211.grouptopic 
		where cateid in (5,6,8) and status=1 and to_date(createtime) between '2014-01-01' and '2014-12-10'
		group by userid
		union all
		select g.userid,0 as cnt,0 as recommend,0 as  digest,0 as life,0 as food,count(distinct(c.commentid)) alive,0 as mcomment,0 as comment,0 as ac,0 as topc from hd_haodou_center_20141211.grouptopic g 
		inner join hd_haodou_comment_20141211.comment c on c.userid=g.userid
		where c.type=6 and c.status =1 and c.replyid=0 and length(html2text(c.content)) >10 
		and to_date(g.createtime) between '2014-01-01' and '2014-12-10'
		and to_date(c.createtime) between '2014-01-01' and '2014-12-10'
		and g.title not like '%抢楼%'
		group by g.userid
		union all
		select g.userid,0 as cnt,0 as recommend,0 as  digest,0 as life,0 as food,0 as alive,count(distinct(c.commentid)) mcomment,0 as comment,0 as ac,0 as topc from hd_haodou_center_20141211.grouptopic g 
		inner join hd_haodou_comment_20141211.comment c on c.userid=g.userid
		where c.type=6 and c.status =1 and c.replyid=0
		and to_date(g.createtime) between '2014-01-01' and '2014-12-10'
		and to_date(c.createtime) between '2014-01-01' and '2014-12-10'
		and g.title not like '%抢楼%' and length(html2text(c.content))>=3 and c.platform in(1,2,3)
		group by g.userid		
		union all
		select g.userid,0 as cnt,0 as recommend,0 as  digest,0 as life,0 as food,0 as alive,0 as mcomment,count(distinct(c.commentid)) comment,0 as ac,0 as topc from hd_haodou_center_20141211.grouptopic g 
		inner join hd_haodou_comment_20141211.comment c on c.userid=g.userid
		where c.type=6 and c.status =1 and g.title not like '%抢楼%'
		and to_date(g.createtime) between '2014-01-01' and '2014-12-10'
		and to_date(c.createtime) between '2014-01-01' and '2014-12-10'
		group by g.userid
		union all
		select g.userid,0 as cnt,0 as recommend,0 as  digest,0 as life,0 as food,0 as alive,0 as mcomment,0 as  comment,count(distinct(c.commentid)) ac,0 as topc from hd_haodou_center_20141211.grouptopic g 
		inner join hd_haodou_comment_20141211.comment c on c.userid=g.userid
		where c.type=6 and c.status =1 and g.title not like '%抢楼%' and length(html2text(c.content)) >10 
		and to_date(g.createtime) between '2014-01-01' and '2014-12-10'
		and to_date(c.createtime) between '2014-01-01' and '2014-12-10'
		group by g.userid
		union all
		select g.userid,0 as cnt,0 as recommend,0 as  digest,0 as life,0 as food,0 as alive,0 as mcomment,0 as  comment,0 as  ac,count(distinct(c.commentid)) topc from hd_haodou_center_20141211.grouptopic g 
		inner join hd_haodou_comment_20141211.comment c on c.userid=g.userid
		where c.type=6 and c.status =1 and g.title not like '%抢楼%' and length(html2text(c.content)) >40 
		and to_date(g.createtime) between '2014-01-01' and '2014-12-10'
		and to_date(c.createtime) between '2014-01-01' and '2014-12-10'
		group by g.userid
		) tmp
		group by userid
		) tmp
		inner join hd_haodou_passport_20141211.user u on u.userid=tmp.userid
		inner join logs.user_min_max_topic ut on ut.userid=tmp.userid
		order by cnt desc;
		"""
		
		datas = []
		
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"#",cols)
			if len(row) > 1:
				data = {}
				data["userid"] = int(row[0].strip())
				data["username"] = str(row[1].strip())
				data["web"] = str(row[2].strip())
				data["cnt"] = int(row[3].strip())
				data["recommend"] = int(row[4].strip())
				data["digest"] = int(row[5].strip())
				data["life"] = int(row[6].strip())
				data["food"] = int(row[7].strip())
				data["alive"] = int(row[8].strip())
				data["mcomment"] = int(row[9].strip())
				data["comment"] = int(row[10].strip())
				data["ac"] = int(row[11].strip())
				data["topc"] = int(row[12].strip())
				data["mindate"] = str(row[13].strip())
				data["maxdate"] = str(row[14].strip())
				
				datas.append(data)
		
		self.topicData["endOfYear"].append(datas)
		
		self.saveXLS()
	
	#导出以下话题的原始评论记录（包含删除的楼层和内容）
	def robFloor(self):
		
		sql = """\
		select c.userid,'#',u.username,'#',html2text(c.content),'#',c.createtime,'#',
		case
		when c.status=0 then '删除'
		when c.status=1 then '正常'
		end as status
		,'#',c.commentid commentid
		from hd_haodou_comment_\${date}.comment c
		inner join hd_haodou_center_\${date}.grouptopic g on g.topicid=c.itemid
		left outer join hd_haodou_passport_\${date}.user u on u.userid=c.userid
		where c.createtime between '\${sd} 00:00:00' and '\${ed} 23:59:59' and c.replyid=0
		and c.status in (0,1) and c.type=6 and g.topicid=\${topicid}
		order by commentid;
		"""
		
		datas = []
		i = 1
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"#",cols)
			if len(row) > 1:
				data = {}
				data["floor"] = i
				data["userid"] = int(row[0].strip())
				data["username"] = str(row[1].strip())
				data["content"] = str(row[2].strip())
				data["createtime"] = str(row[3].strip())
				data["status"] = str(row[4].strip())
				
				datas.append(data)
				i += 1
		
		self.topicData["robFloor"] = datas
		
		self.saveXLS()
		
		
		
	def saveXLS(self):	
		
		#写文件
		xlswriter = XLSWriter(self.file_path%("recipegrouptopicdatasummary_"+self.curDate+".xls"))
		
		xlswriter.writerow(["日期","总PV","总UV","发帖数","发帖人数","回复数","回复人数"], sheet_name=u'数据汇总')
		
		for date in self.serialDate:
			pv = 0
			if "pv" in self.topicData[date]:
				pv = self.topicData[date]["pv"]
			uv = 0	
			if "uv" in self.topicData[date]:
				uv = self.topicData[date]["uv"]
			tc = 0
			if "topic" in self.topicData[date]:
				tc = self.topicData[date]["topic"]
			tr = 0
			if "topicusers" in self.topicData[date]:
				tr = self.topicData[date]["topicusers"]
			cc = 0
			if "comment" in self.topicData[date]:
				cc = self.topicData[date]["comment"]
			cr = 0
			if "commentusers" in self.topicData[date]:
				cr = self.topicData[date]["commentusers"]
			
			xlswriter.writerow([date,pv,uv,tc,tr,cc,cr], sheet_name=u'数据汇总')
			
		
		xlswriter.writerow(["日期","网站端PV","网站端UV","","日期","移动端PV","移动端UV","","日期","微信PV","微信UV","","日期","QQ空间PV","QQ空间UV","","日期","腾讯微博PV","腾讯微博UV","","日期","新浪微博PV","新浪微博UV"], sheet_name=u'数据明细')
		
		for date in self.serialDate:
			webpv = 0
			webuv = 0
			mpv = 0
			muv = 0
			wpv = 0
			wuv = 0
			qqpv = 0
			qquv = 0
			tpv = 0
			tuv = 0
			xpv = 0
			xuv = 0
			if date in self.topicData:
				if "webpv" in self.topicData[date]:
					webpv = self.topicData[date]["webpv"]
				
				if "webuv" in self.topicData[date]:
					webuv = self.topicData[date]["webuv"]
				
				if "mpv" in self.topicData[date]:
					mpv = self.topicData[date]["mpv"]
				
				if "muv" in self.topicData[date]:
					muv = self.topicData[date]["muv"]
				
				if "wpv" in self.topicData[date]:
					wpv = self.topicData[date]["wpv"]
				
				if "wuv" in	self.topicData[date]:
					wuv =	self.topicData[date]["wuv"]
					
				if "qqpv" in self.topicData[date]:
					qqpv = self.topicData[date]["qqpv"]
				
				if "qquv"	in self.topicData[date]:
					qquv = self.topicData[date]["qquv"]
				
				if "tpv" in self.topicData[date]:
					tpv = self.topicData[date]["tpv"]
				
				if "tuv"in self.topicData[date]:
					tuv = self.topicData[date]["tuv"]
				
				if "xpv" in self.topicData[date]:
					xpv = self.topicData[date]["xpv"]
				
				if "xuv" in self.topicData[date]:
					xuv = self.topicData[date]["xuv"]	
					
			xlswriter.writerow([date,webpv,webuv,"",date,mpv,muv,"",date,wpv,wuv,"",date,qqpv,qquv,"",date,tpv,tuv,"",date,xpv,xuv], sheet_name=u'数据明细')
			
		
		if "topicsdata" in self.topicData:
			xlswriter.writerow(["用户昵称","用户ID","话题标题","话题链接","创建日期","浏览数","回复数","回复人数(一级回复的回复人数)","收藏数(该话题的收藏数)","推荐(推荐即表格显示推荐)","精华(精华即表格显示精华)","被赠送豆币数量"], sheet_name=u'话题详情')
			
			for data in self.topicData["topicsdata"]:
				xlswriter.writerow([data["username"],data["userid"],data["title"],data["web"],data["createtime"],data["viewcount"],data["comment"],data["commentusers"],data["favorite"],data["recommend"],data["digest"],data["wealth"]], sheet_name=u'话题详情')
			
				
		if "userids" in self.topicData:
			
			xlswriter.writerow(["用户昵称","用户ID","天数"], sheet_name=u'连续发布话题天数')
			userids = self.topicData["userids"]
			for userid in userids.keys():
				xlswriter.writerow([userids[userid]["username"],int(userid),userids[userid]["top"]], sheet_name=u'连续发布话题天数')
		
		
		index = 1
		if "share" in self.topicData:
			xlswriter.writerow(["楼层","用户名","用户ID","有无图片","图片数量"], sheet_name=u'话题传播分享统计')
			
			shares = self.topicData["share"]
			for share in shares:				
				xlswriter.writerow([index,share["username"],share["userid"],share["type"],share["pcount"]], sheet_name=u'话题传播分享统计')
				index += 1
		
		
		
		
		if "eightFloorUser" in self.topicData:
			
			xlswriter.writerow(["用户ID","用户昵称","话题标题","话题链接","创建时间","第8条回复内容"], sheet_name=u'逢8有喜回复有奖活动统计')
			
			datas = self.topicData["eightFloorUser"]
			for data in datas:	
				xlswriter.writerow([data["userid"],data["username"],data["title"],data["web"],data["createtime"],data["content"]], sheet_name=u'逢8有喜回复有奖活动统计')
		
		
		if "invite" in self.topicData:
			
			xlswriter.writerow(["邀请人用户名","邀请人用户ID","邀请人话题发布数","被邀请人用户名","被邀请人用户ID","被邀请人话题发布数"], sheet_name=u'邀请好友')
			datas = self.topicData["invite"]
			for data in datas:	
				xlswriter.writerow([data["userid"],data["username"],data["cnt"],data["invite"],data["inviteusername"],data["invitecnt"]], sheet_name=u'邀请好友')
		
		if "endOfYear" in self.topicData:
			
			xlswriter.writerow(["排名","用户ID","用户名","话题标题","话题链接","回复数","手机端回复数","回复人数","浏览数","收藏数","小组名","推荐","精华"], sheet_name=u'优秀热门话题')
			
			datas = self.topicData["endOfYear"][0]
			for data in datas:
				xlswriter.writerow([data["rank"]+1, data["userid"],data["username"],data["title"],data["web"],data["comment"],data["mcomment"],data["cusers"],data["viewcount"],data["favorite"],data["cateid"],data["recommend"],data["digest"]], sheet_name=u'优秀热门话题')
			
					
			xlswriter.writerow(["排名","用户ID","用户名","豆窝","话题总数","推荐话题总数","精华话题总数","爱生活话题","做美食话题","有效评论话题数","回复总数","手机端回复数","有效回复总数","优质回复总数","第一次创建话题时间","最后一次创建话题时间"], sheet_name=u'话题评论详情')
			i = 1
			datas = self.topicData["endOfYear"][1]	
			for user in datas:
				xlswriter.writerow([i, user["userid"],user["username"],user["web"],user["cnt"],user["recommend"],user["digest"],user["life"],user["food"],user["alive"],user["comment"],user["mcomment"],user["ac"],user["topc"],user["mindate"],user["maxdate"]], sheet_name=u'话题评论详情')
				i += 1	
		
		
		if "robFloor" in self.topicData:
				
			xlswriter.writerow(["楼层","用户ID","用户名","回复内容","回复时间","帖子状态"], sheet_name=u'小组抢楼活动数据')	
				
			datas = self.topicData["robFloor"]
			for data in datas:
				xlswriter.writerow([data["floor"],data["userid"],data["username"],data["content"],data["createtime"],data["status"]], sheet_name=u'小组抢楼活动数据')
				
		xlswriter.save()
		
		
		print "发邮件"
		ms = MailSender(self.file_path%("recipegrouptopicdatasummary_"+self.curDate+".xls"), "%s %s-%s活动统计数据汇总.xls"%(self.keyword,self.startDate,self.endDate))
		ms.sendMail("zhaoweiguo@haodou.com", "%s %s - %s 活动统计数据汇总"%(self.keyword,self.startDate,self.endDate), "数据详情见附件")
		#ms.sendMail("liujingming@haodou.com", "%s %s - %s 活动统计数据汇总"%(self.keyword,self.startDate,self.endDate), "数据详情见附件")
		
if __name__ == '__main__':
	
	usage = "usage: %prog [options] arg1 arg2"
	parser = OptionParser(usage = usage, version="%prog 0.1")
	parser.add_option("-t", "--topicid", help = 'Find by topicid', dest = "topicid")
	parser.add_option("-k", "--keyword", help = 'Find by some keyword', dest = "keyword")
	parser.add_option("-s", "--datestart", help = 'The date start', dest = "datestart")
	parser.add_option("-e", "--dateend", help = 'The date end', dest = "dateend")
	(options, args) = parser.parse_args()
	
	if len(sys.argv)<5:
		parser.print_help()
		sys.exit(2)
		
	keyword = options.keyword or "0"
	datestart = options.datestart
	dateend = options.dateend
	topicid = options.topicid or ""
		
	rs = RecipeGroupTopicDataSummary(topicid, keyword, datestart, dateend)
	#话题数据
	#rs.start()
	#逢8有喜
	#rs.eightFloor()
	#年终盘点
	#rs.endOfYear()
	#小组抢楼
	rs.robFloor()
