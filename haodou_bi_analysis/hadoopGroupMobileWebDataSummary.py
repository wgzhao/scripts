#!/usr/bin/env
# -*- coding: UTF-8 -*- 
#coding = utf-8
#小组移动端/网站端数据统计

from optparse import OptionParser
import commands
from time import ctime
from MailUtil import *
from XLSWriter import XLSWriter
from hiveDB import *
from DBUtil import *
from sparkDB import *
from timeUtils import *
import traceback
from  datetime  import  *
import time
import sys
import os
import re



class GroupMobileWebDataSummary:

	def __init__(self, fpath, delay=1):
		self.delay = delay
		self.db = DB()		
		self.db.execute("""set autocommit = 1""")
		
		dt = datetime.now()
		#设置根据delay时间倒减一天
		#方便取过去时间的数据测试
		self.nowDate = (dt - timedelta(days = self.delay - 1)).strftime("%Y%m%d")
		
		self._datetime = dt - timedelta(days = delay)
		self.curDate = self._datetime.strftime("%Y-%m-%d")
		self.tu = TimeUtils()
		
		self.usersTime = {}
		
		t = time.mktime(self._datetime.timetuple())
		#算上周一
		self.startWeekDate = time.strftime('%Y-%m-%d',time.localtime(self.tu.get_week_begin(t,-1)))
		#计算上周末
		self.endWeekDate = (datetime.fromtimestamp(self.tu.get_week_begin(t,-1)) + timedelta(days = 6)).strftime('%Y-%m-%d')
		
		#算上月一号
		self.startMonthDate = time.strftime('%Y-%m-%d',time.localtime(self.tu.get_month_begin(t,-1)))
		#算上月末
		self.endMonthDate = (datetime.fromtimestamp(self.tu.get_month_begin(t)) - timedelta(days = 1)).strftime('%Y-%m-%d')
				
		self.hive = HiveDB("--define date=%s"%self.curDate)
		#self.spark = SparkDB()
		
		self.file_path = fpath + "/" + self._datetime.strftime("%Y%m") + "/"
		
		if os.path.isdir(self.file_path) is False:
			os.mkdir(self.file_path)
	
	
	def start(self):
		
		
		self.basicData()
		
		self.qualityTopic()
		
		self.activeUser()
		
		self.qualityTopicSummary()
		
		self.topicShare()
		
		self.topicFavorite()
				
		self.groupData()
		
		#访问好豆的用户 到 访问广场的用户的 比例
		
		
	def basicData(self):
			
		#乐在厨房 5	营养健康 6	厨房宝典 8 好好生活	12 摄影天地	11 游山玩水	10 亲子乐园 23 豆有所好27 公告 17
		sql = "delete from bi_group_analysis where createtime='%s' and type =0"
		self.db.execute(sql%self.curDate)
		
		#web
		sql = """\
		select logdate,cateid,sum(pv),sum(uv) from (
		select logdate,ggifp(path) cateid,count(remote_addr) pv,count(distinct(concat(remote_addr,'-',http_user_agent))) uv
		from group_haodou_com
		where logdate='%s' and ggifp(path) in (5,6,8,12,11,10,23,27,17)
		group by logdate,ggifp(path)
		union all
		select m.logdate,gt.cateid,count(m.remote_addr) pv,count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) uv
		from m_haodou_com m
		inner join hd_haodou_center_%s.grouptopic gt on gtifurl(m.path)=gt.topicid
		where gt.status=1 and m.logdate='%s' and gaidfua(m.http_user_agent) =0
		group by m.logdate,gt.cateid
		) tmp
		group by logdate,cateid;
		"""
		
		cursor = self.hive.execute(sql%(self.curDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					groupid = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',%s,0,%s,%s,0)"
					self.db.execute(sql%(createtime,groupid,cnt,pnt))
		
		#app
		sql = """\
		select logdate,cateid,appid,sum(pv),sum(uv) from (
		select logdate,ggifp(parameter_desc) cateid,appid,count(concat(device_id,uuid)) pv,count(distinct(concat(device_id,uuid))) uv
		from log_php_app_log where logdate='%s' and appid in (2,4,6) and function_id in ('topic.getlist','topic.getgroupindexdata')
		group by logdate,ggifp(parameter_desc),appid
		union all
		select m.logdate,gt.cateid,gaidfua(m.http_user_agent) appid,count(m.remote_addr) pv,count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) uv
		from m_haodou_com m
		inner join hd_haodou_center_%s.grouptopic gt on gtifurl(m.path)=gt.topicid
		where gt.status=1 and m.logdate='%s' and gaidfua(m.http_user_agent) <> 0
		group by m.logdate,gt.cateid,gaidfua(m.http_user_agent)	
		)tmp
		group by logdate,cateid,appid;
		"""
		
		cursor = self.hive.execute(sql%(self.curDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					groupid = int(row[1])
					platform = int(row[2])
					cnt = int(row[3])
					pnt = int(row[4])
					print createtime,groupid,platform,cnt,pnt
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,%s,0)"
					self.db.execute(sql%(createtime,groupid,platform,cnt,pnt))
					
					
		#计算总PV、UV
		
		sql = """\
		select logdate,sum(pv),sum(uv) from (
		select logdate,count(remote_addr) pv, count(distinct(concat(remote_addr,'-',http_user_agent))) uv
		from group_haodou_com
		where logdate='%s'
		group by logdate
		union all
		select logdate,count(concat(device_id,uuid)) pv,count(distinct(concat(device_id,uuid))) uv
		from log_php_app_log where logdate='%s' and appid in (2,4,6) and function_id in('topic.getlist','topic.getgroupindexdata') 
		group by logdate
		union all
		select m.logdate,count(m.remote_addr) pv,count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) uv
		from m_haodou_com m
		inner join hd_haodou_center_%s.grouptopic gt on gtifurl(m.path)=gt.topicid
		where gt.status=1 and m.logdate='%s'
		group by m.logdate
		) tmp
		group by logdate;
		"""
		
		cursor = self.hive.execute(sql%(self.curDate,self.curDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',-1,-1,%s,%s,0)"
					self.db.execute(sql%(createtime,cnt,pnt))
		
		#计算web端总PV、UV
		
		sql = """\
		select logdate,sum(pv),sum(uv) from (
		select logdate,count(remote_addr) pv,count(distinct(concat(remote_addr,'-',http_user_agent))) uv
		from group_haodou_com
		where logdate='%s'
		group by logdate
		union all
		select m.logdate,count(m.remote_addr) pv,count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) uv
		from m_haodou_com m
		inner join hd_haodou_center_%s.grouptopic gt on gtifurl(m.path)=gt.topicid
		where gt.status=1 and m.logdate='%s' and gaidfua(m.http_user_agent) =0
		group by m.logdate
		) tmp
		group by logdate;
		"""
		
		cursor = self.hive.execute(sql%(self.curDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',-1,0,%s,%s,0)"
					self.db.execute(sql%(createtime,cnt,pnt))
		
		#计算移动端总PV、UV
		
		sql = """\
		select logdate,sum(pv),sum(uv) from (
		select logdate,count(concat(device_id,uuid)) pv,count(distinct(concat(device_id,uuid))) uv
		from log_php_app_log where logdate='%s' and appid in (2,4,6) and function_id in ('topic.getlist','topic.getgroupindexdata')
		group by logdate
		union all
		select m.logdate,count(m.remote_addr) pv,count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) uv
		from m_haodou_com m
		inner join hd_haodou_center_%s.grouptopic gt on gtifurl(m.path)=gt.topicid
		where gt.status=1 and m.logdate='%s' and gaidfua(m.http_user_agent) <> 0
		group by m.logdate
		)tmp
		group by logdate;
		"""
		
		cursor = self.hive.execute(sql%(self.curDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',-1,1,%s,%s,0)"
					self.db.execute(sql%(createtime,cnt,pnt))
					
					
		#栏目下话题PV、UV
		
		sql = "delete from bi_group_analysis where createtime='%s' and type =1"
		self.db.execute(sql%self.curDate)
		
		#web
		sql = """\
		select logdate,cateid,sum(pv),sum(uv) from (
		select g.logdate,gt.cateid,count(g.remote_addr) pv ,count(distinct(concat(g.remote_addr,'-',g.http_user_agent))) uv 
		from group_haodou_com g
		inner join hd_haodou_center_%s.grouptopic gt on gtifurl(g.path)=gt.topicid
		where gt.cateid in (5,6,8,12,11,10,23,27,17) and gt.status=1 and g.logdate='%s'
		group by g.logdate,gt.cateid
		union all 
		select m.logdate,gt.cateid,count(m.remote_addr) pv,count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) uv 
		from m_haodou_com m
		inner join hd_haodou_center_%s.grouptopic gt on gtifurl(m.path)=gt.topicid
		where gt.cateid in (5,6,8,12,11,10,23,27,17) and gt.status=1 and m.logdate='%s'
		and gaidfua(m.http_user_agent) = 0
		group by m.logdate,gt.cateid
		) tmp
		group by logdate,cateid;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					groupid = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',%s,0,%s,%s,1)"
					self.db.execute(sql%(createtime,groupid,cnt,pnt))
		
		#app
		sql = """\
		select m.logdate,gt.cateid,gaidfua(m.http_user_agent),count(m.remote_addr),count(distinct(concat(m.remote_addr,'-',m.http_user_agent)))
		from m_haodou_com m
		inner join hd_haodou_center_%s.grouptopic gt on gtifurl(m.path)=gt.topicid
		where gt.cateid in (5,6,8,12,11,10,23,27,17) and gt.status=1 and m.logdate='%s'
		and gaidfua(m.http_user_agent) <> 0
		group by m.logdate,gt.cateid,gaidfua(m.http_user_agent);
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					groupid = int(row[1])
					platform = int(row[2])
					cnt = int(row[3])
					pnt = int(row[4])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,%s,1)"
					self.db.execute(sql%(createtime,groupid,platform,cnt,pnt))
					
		
		#计算总PV、UV		
		sql = """\
		select logdate,sum(pv),sum(uv) from (
		select g.logdate,count(g.remote_addr) pv ,count(distinct(concat(g.remote_addr,'-',g.http_user_agent))) uv 
		from group_haodou_com g
		inner join hd_haodou_center_%s.grouptopic gt on gtifurl(g.path)=gt.topicid
		where gt.cateid in (5,6,8,12,11,10,23,27,17) and gt.status=1 and g.logdate='%s'
		group by g.logdate
		union all 
		select m.logdate,count(m.remote_addr) pv,count(distinct(concat(m.remote_addr,'-',m.http_user_agent))) uv 
		from m_haodou_com m
		inner join hd_haodou_center_%s.grouptopic gt on gtifurl(m.path)=gt.topicid
		where gt.cateid in (5,6,8,12,11,10,23,27,17) and gt.status=1 and m.logdate='%s'
		group by m.logdate
		) tmp
		group by logdate;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',-1,-1,%s,%s,1)"
					self.db.execute(sql%(createtime,cnt,pnt))
		
		#话题发布
		
		sql = "delete from bi_group_analysis where createtime='%s' and type =2"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(createtime),cateid,count(topicid),count(distinct(userid))
		from hd_haodou_center_%s.grouptopic
		where cateid in (5,6,8,12,11,10,23,27,17) and status=1 and to_date(createtime)='%s'
		group by to_date(createtime),cateid;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					groupid = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',%s,-1,%s,%s,2)"
					self.db.execute(sql%(createtime,groupid,cnt,pnt))
		
		#做美食大组
		sql = """\
		select to_date(createtime),count(topicid),count(distinct(userid))
		from hd_haodou_center_%s.grouptopic
		where cateid in (5,6,8) and status=1 and to_date(createtime)='%s'
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
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',-1,1,%s,%s,2)"
					self.db.execute(sql%(createtime,cnt,pnt))
		
		
		#爱生活大组
		sql = """\
		select to_date(createtime),count(topicid),count(distinct(userid))
		from hd_haodou_center_%s.grouptopic
		where cateid in (12,11,10,23,27) and status=1 and to_date(createtime)='%s'
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
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',-1,2,%s,%s,2)"
					self.db.execute(sql%(createtime,cnt,pnt))
		
		
		#计算总PV、UV
		
		sql = """\
		select to_date(createtime),count(topicid),count(distinct(userid))
		from hd_haodou_center_%s.grouptopic
		where cateid in (5,6,8,12,11,10,23,27,17) and status=1 and to_date(createtime)='%s'
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
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',-1,0,%s,%s,2)"
					self.db.execute(sql%(createtime,cnt,pnt))
		
		#话题评论
		sql = "delete from bi_group_analysis where createtime='%s' and type =3"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(c.createtime),g.cateid,c.platform,count(distinct(c.commentId)),count(distinct(c.userid)) 
		from hd_haodou_comment_%s.comment c
		inner join hd_haodou_center_%s.grouptopic g on g.topicid=c.itemid
		where to_date(c.createtime) = '%s' and c.status=1 and c.type=6
		group by to_date(c.createtime),g.cateid,c.platform;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					groupid = int(row[1])
					platform = int(row[2])
					cnt = int(row[3])
					pnt = int(row[4])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,%s,3)"
					self.db.execute(sql%(createtime,groupid,platform,cnt,pnt))
		
		
		#做美食大组
		sql = """\
		select to_date(c.createtime),count(distinct(c.commentId)),count(distinct(c.userid)) 
		from hd_haodou_comment_%s.comment c
		inner join hd_haodou_center_%s.grouptopic g on g.topicid=c.itemid
		where to_date(c.createtime) = '%s' and c.status=1 and c.type=6 and g.cateid in (5,6,8)
		group by to_date(c.createtime);
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',-1,0,%s,%s,3)"
					self.db.execute(sql%(createtime,cnt,pnt))
					
		
		#爱生活大组
		sql = """\
		select to_date(c.createtime),count(distinct(c.commentId)),count(distinct(c.userid)) 
		from hd_haodou_comment_%s.comment c
		inner join hd_haodou_center_%s.grouptopic g on g.topicid=c.itemid
		where to_date(c.createtime) = '%s' and c.status=1 and c.type=6 and g.cateid in (12,11,10,23,27)
		group by to_date(c.createtime);
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',-1,1,%s,%s,3)"
					self.db.execute(sql%(createtime,cnt,pnt))
		
		
		#计算总PV、UV
		
		sql = """\
		select to_date(c.createtime),count(distinct(c.commentId)),count(distinct(c.userid)) 
		from hd_haodou_comment_%s.comment c
		inner join hd_haodou_center_%s.grouptopic g on g.topicid=c.itemid
		where to_date(c.createtime) = '%s' and c.status=1 and c.type=6 and g.cateid in (5,6,8,12,11,10,23,27,17)
		group by to_date(c.createtime);
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',-1,-1,%s,%s,3)"
					self.db.execute(sql%(createtime,cnt,pnt))
		
		#话题收藏数
		sql = "delete from bi_group_analysis where createtime='%s' and type =4"
		self.db.execute(sql%self.curDate)
		
		#web
		sql = """\
		select w.logdate,g.cateid,count(w.remote_addr),count(distinct(concat(w.remote_addr,'-',w.http_user_agent)))
		from group_haodou_com w
		inner join hd_haodou_center_""" + self.nowDate + """.grouptopic g on gffp(w.path)=g.topicid
		where w.logdate='""" + self.curDate + """' and g.status=1
		and w.path like '/topic.php?do=fav%type=add'
		group by w.logdate,g.cateid;
		"""
		
		cursor = self.hive.execute(sql)
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					groupid = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',%s,0,%s,%s,4)"
					self.db.execute(sql%(createtime,groupid,cnt,pnt))
		
		#app
		sql = """\
		select l.logdate,g.cateid,l.appid,count(concat(l.device_id,l.uuid)),count(distinct(concat(l.device_id,l.uuid))) 
		from log_php_app_log l
		inner join hd_haodou_center_%s.grouptopic g on gffp(l.parameter_desc)=g.topicid
		where l.logdate='%s' and l.appid in (2,4,6) and l.function_id ='favorite.add'
		and g.status=1
		group by l.logdate,g.cateid,l.appid;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					groupid = int(row[1])
					platform = int(row[2])
					cnt = int(row[3])
					pnt = int(row[4])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,%s,4)"
					self.db.execute(sql%(createtime,groupid,platform,cnt,pnt))
		
		#计算总PV、UV
		
		sql = """\
		select logdate,sum(pv),sum(uv) from (
		select w.logdate,count(w.remote_addr) pv,count(distinct(concat(w.remote_addr,'-',w.http_user_agent))) uv
		from group_haodou_com w
		inner join hd_haodou_center_""" + self.nowDate + """.grouptopic g on gffp(w.path)=g.topicid
		where w.logdate='""" + self.curDate + """' and g.status=1 and g.cateid in (5,6,8,12,11,10,23,27,17)
		and w.path like '/topic.php?do=fav%type=add'
		group by w.logdate
		union all
		select l.logdate,count(concat(l.device_id,l.uuid)) pv,count(distinct(concat(l.device_id,l.uuid))) uv
		from log_php_app_log l
		inner join hd_haodou_center_""" + self.nowDate + """.grouptopic g on gffp(l.parameter_desc)=g.topicid
		where l.logdate='""" + self.curDate + """' and l.appid in (2,4,6) and l.function_id ='favorite.add' 
		and g.cateid in (5,6,8,12,11,10,23,27,17) and g.status=1
		group by l.logdate
		) tmp
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
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',-1,-1,%s,%s,4)"
					self.db.execute(sql%(createtime,cnt,pnt))
		
		
		#话题分享数
		sql = "delete from bi_group_analysis where createtime='%s' and type =5"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select to_date(f.createtime),g.cateid,count(f.feedid),count(distinct(f.userid))
		from hd_haodou_center_%s.userfeed f
		inner join hd_haodou_center_%s.grouptopic g on g.topicid=f.itemid
		where to_date(f.createtime) = '%s' and f.type = 403 and g.status=1
		group by to_date(f.createtime),g.cateid;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					groupid = int(row[1])
					cnt = int(row[2])
					pnt = int(row[3])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',%s,-1,%s,%s,5)"
					self.db.execute(sql%(createtime,groupid,cnt,pnt))
		
		#计算总PV、UV
		
		sql = """\
		select to_date(f.createtime),count(f.feedid),count(distinct(f.userid))
		from hd_haodou_center_%s.userfeed f
		inner join hd_haodou_center_%s.grouptopic g on g.topicid=f.itemid
		where to_date(f.createtime) = '%s' and f.type = 403 and g.status=1 and g.cateid in (5,6,8,12,11,10,23,27,17)
		group by to_date(f.createtime);
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					cnt = int(row[1])
					pnt = int(row[2])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,total_person,type) VALUES ('%s',-1,0,%s,%s,5)"
					self.db.execute(sql%(createtime,cnt,pnt))
		
		
		
	#优质话题数
	def qualityTopic(self):
		
		sql = "delete from bi_group_analysis where createtime='%s' and type =6"
		self.db.execute(sql%self.curDate)
		
		#做美食推荐
		sql = """\
		select to_date(createtime),cateid,count(topicid)
		from hd_haodou_center_%s.grouptopic
		where to_date(createtime) = '%s' and status =1 and recommend =1 and cateid in (5,6,8)
		group by to_date(createtime),cateid;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					groupid = int(row[1])
					cnt = int(row[2])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,type) VALUES ('%s',%s,0,%s,6)"
					self.db.execute(sql%(createtime,groupid,cnt))
		
		#做美食精华
		sql = """\
		select to_date(createtime),cateid,count(topicid)
		from hd_haodou_center_%s.grouptopic
		where to_date(createtime) = '%s' and status =1 and digest=1 and cateid in (5,6,8)
		group by to_date(createtime),cateid;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					groupid = int(row[1])
					cnt = int(row[2])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,type) VALUES ('%s',%s,1,%s,6)"
					self.db.execute(sql%(createtime,groupid,cnt))
		
		#做美食热门
		sql = """\
		select createtime,cateid,count(topicid) from(
		select to_date(c.createtime) as createtime,g.cateid,g.topicid,count(c.commentId) comment, 0 as viewcount
		from hd_haodou_comment_%s.comment c
		inner join hd_haodou_center_%s.grouptopic g on g.topicid=c.itemid
		where to_date(c.createtime) = '%s' and c.status=1 and c.type=6 and g.cateid in (5,6,8)
		group by to_date(c.createtime),g.cateid,g.topicid having comment >= 15
		union all
		select g.logdate as createtime,gt.cateid,gt.topicid,0 as comment, count(g.remote_addr) viewcount
		from hd_haodou_center_%s.grouptopic gt
		inner join group_haodou_com g on gtifurl(g.path)=gt.topicid
		inner join m_haodou_com m on gtifurl(m.path)=gt.topicid
		where g.logdate='%s' and m.logdate='%s'
		and gt.status=1 and gt.cateid in (5,6,8)
		group by g.logdate,gt.cateid,gt.topicid having viewcount >= 150
		) tmp
		group by createtime,cateid;
		"""
		
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate,self.nowDate,self.curDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					groupid = int(row[1])
					cnt = int(row[2])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,type) VALUES ('%s',%s,2,%s,6)"
					self.db.execute(sql%(createtime,groupid,cnt))
		
		#爱生活推荐
		sql = """\
		select to_date(createtime),cateid,count(topicid)
		from hd_haodou_center_%s.grouptopic
		where to_date(createtime) = '%s' and status =1 and recommend =1 and cateid in (12,11,10,23,27)
		group by to_date(createtime),cateid;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					groupid = int(row[1])
					cnt = int(row[2])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,type) VALUES ('%s',%s,3,%s,6)"
					self.db.execute(sql%(createtime,groupid,cnt))
		
		#爱生活精华
		sql = """\
		select to_date(createtime),cateid,count(topicid)
		from hd_haodou_center_%s.grouptopic
		where to_date(createtime) = '%s' and status =1 and digest=1 and cateid in (12,11,10,23,27)
		group by to_date(createtime),cateid;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					groupid = int(row[1])
					cnt = int(row[2])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,type) VALUES ('%s',%s,4,%s,6)"
					self.db.execute(sql%(createtime,groupid,cnt))
		
		#爱生活(摄影天地 游山玩水 亲子乐园 豆有所好)热门
		sql = """\
		select createtime,cateid,count(topicid) from(
		select to_date(c.createtime) as createtime,g.cateid,g.topicid,count(c.commentId) comment, 0 as viewcount
		from hd_haodou_comment_%s.comment c
		inner join hd_haodou_center_%s.grouptopic g on g.topicid=c.itemid
		where to_date(c.createtime) = '%s' and c.status=1 and c.type=6 and g.cateid in (11,10,23,27)
		group by to_date(c.createtime),g.cateid,g.topicid having comment >= 15
		union all
		select g.logdate as createtime,gt.cateid,gt.topicid,0 as comment, count(g.remote_addr) viewcount
		from hd_haodou_center_%s.grouptopic gt
		inner join group_haodou_com g on gtifurl(g.path)=gt.topicid
		inner join m_haodou_com m on gtifurl(m.path)=gt.topicid
		where g.logdate='%s' and m.logdate='%s'
		and gt.status=1 and gt.cateid in (11,10,23,27)
		group by g.logdate,gt.cateid,gt.topicid having viewcount >= 150
		) tmp
		group by createtime,cateid;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate,self.nowDate,self.curDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					groupid = int(row[1])
					cnt = int(row[2])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,type) VALUES ('%s',%s,5,%s,6)"
					self.db.execute(sql%(createtime,groupid,cnt))
		
		#爱生活(好好生活)热门
		sql = """\
		select createtime,cateid,count(topicid) from(
		select to_date(c.createtime) as createtime,g.cateid,g.topicid,count(c.commentId) comment, 0 as viewcount
		from hd_haodou_comment_%s.comment c
		inner join hd_haodou_center_%s.grouptopic g on g.topicid=c.itemid
		where to_date(c.createtime) = '%s' and c.status=1 and c.type=6 and g.cateid =12
		group by to_date(c.createtime),g.cateid,g.topicid having comment >= 20
		union all
		select g.logdate as createtime,gt.cateid,gt.topicid,0 as comment, count(g.remote_addr) viewcount
		from hd_haodou_center_%s.grouptopic gt
		inner join group_haodou_com g on gtifurl(g.path)=gt.topicid
		inner join m_haodou_com m on gtifurl(m.path)=gt.topicid
		where g.logdate='%s' and m.logdate='%s'
		and gt.status=1 and gt.cateid =12
		group by g.logdate,gt.cateid,gt.topicid having viewcount >= 150
		) tmp
		group by createtime,cateid;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate,self.nowDate,self.curDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					createtime = str(row[0])
					groupid = int(row[1])
					cnt = int(row[2])
					
					sql = "INSERT INTO bi_group_analysis (createtime,groupid,platform,total,type) VALUES ('%s',%s,5,%s,6)"
					self.db.execute(sql%(createtime,groupid,cnt))
	
	#每日活跃用户
	def activeUser(self):	
		
		sql = "delete from bi_group_active_user_summary where createtime='%s'"
		self.db.execute(sql%self.curDate)
		
		
		sql = """\
		select userid,username,sum(photo),sum(recipe),sum(diary),sum(topic),sum(recommend),sum(digest),sum(comment) from (
		select p.userid,u.username,count(p.id) photo, 0 as recipe, 0 as diary, 0 as topic, 0 as recommend, 0 as digest, 0 as comment
		from hd_haodou_photo_%s.photo p
		left outer join hd_haodou_passport_%s.user u on u.userid=p.userid
		where p.status in (1,2) and to_date(p.createtime)='%s'
		group by p.userid,u.username
		union all
		select r.userid,u.username,0 as photo, count(r.recipeid) recipe, 0 as diary, 0 as topic, 0 as recommend, 0 as digest, 0 as comment
		from hd_haodou_recipe_%s.recipe r
		left outer join hd_haodou_passport_%s.user u on u.userid=r.userid
		where r.status in (0,10) and to_date(r.createtime)='%s'
		group by r.userid,u.username
		union all
		select d.userid,u.username,0 as photo, 0 as recipe, count(fooddiaryid) diary, 0 as topic, 0 as recommend, 0 as digest, 0 as comment
		from hd_haodou_center_%s.userdiary d
		left outer join hd_haodou_passport_%s.user u on u.userid=d.userid
		where d.status in (1,2) and to_date(d.createtime)='%s'
		group by d.userid,u.username
		union all
		select g.userid,u.username,0 as photo, 0 as recipe, 0 as diary, count(topicid) topic, 0 as recommend, 0 as digest, 0 as comment
		from hd_haodou_center_%s.grouptopic g
		left outer join hd_haodou_passport_%s.user u on u.userid=g.userid
		where g.status in (1,2) and to_date(g.createtime)='%s'
		group by g.userid,u.username
		union all
		select g.userid,u.username,0 as photo, 0 as recipe, 0 as diary, 0 as topic, count(topicid) recommend, 0 as digest, 0 as comment
		from hd_haodou_center_%s.grouptopic g
		left outer join hd_haodou_passport_%s.user u on u.userid=g.userid
		where g.status in (1,2) and to_date(g.createtime)='%s' and g.recommend=1
		group by g.userid,u.username
		union all
		select g.userid,u.username,0 as photo, 0 as recipe, 0 as diary, 0 as topic, 0 as recommend, count(topicid) digest, 0 as comment
		from hd_haodou_center_%s.grouptopic g
		left outer join hd_haodou_passport_%s.user u on u.userid=g.userid
		where g.status in (1,2) and to_date(g.createtime)='%s' and g.digest=1
		group by g.userid,u.username
		union all
		select c.userid,u.username,0 as photo, 0 as recipe, 0 as diary, 0 as topic, 0 as recommend, 0 as digest, count(c.commentid) comment
		from hd_haodou_comment_%s.comment c
		left outer join hd_haodou_passport_%s.user u on u.userid=c.userid
		where c.status =1 and c.type=6 and to_date(c.createtime)='%s'
		group by c.userid,u.username
		) tmp
		group by userid,username;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate,self.nowDate,self.nowDate,self.curDate,self.nowDate,self.nowDate,self.curDate,self.nowDate,self.nowDate,self.curDate,self.nowDate,self.nowDate,self.curDate,self.nowDate,self.nowDate,self.curDate,self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					userid = int(row[0])
					username = str(row[1])
					photo = int(row[2])
					recipe = int(row[3])
					diary = int(row[4])
					topic = int(row[5])
					recommend = int(row[6])
					digest = int(row[7])
					comment = int(row[8])
					
					sql = "INSERT INTO bi_group_active_user_summary (userid,username,photo,recipe,diary,topic,recommend,digest,comment,createtime) VALUES (%s,'%s',%s,%s,%s,%s,%s,%s,%s,'%s')"
					self.db.execute(sql%(userid,username,photo,recipe,diary,topic,recommend,digest,comment,self.curDate))
			
	#优质话题详情
	def qualityTopicSummary(self):
		
		sql = "delete from bi_group_quality_topic where date(createtime)='%s'"
		self.db.execute(sql%self.curDate)
		
		
		#5种情况
		
		#热门、非推荐、非精华 0
		#热门、推荐 1
		#热门、精华 2
		#推荐、非热门 3
		#精华、非热门 4
		
		#1.找出所有的话题id
		#推荐和精华
		sql = """\
		select topicid,type from (
		select topicid, 0 as type from hd_haodou_center_%s.grouptopic
		where to_date(createtime) = '%s' and status =1 and digest=1
		union all
		select topicid, 1 as type from hd_haodou_center_%s.grouptopic
		where to_date(createtime) = '%s' and status =1 and recommend=1
		) tmp;
		"""
		
		topics = {}
		
		cursor = self.hive.execute(sql%(self.nowDate,self.curDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				if len(row) > 1:
					topicid = int(row[0])
					if topicid not in topics:
						topics[topicid] = {}
						topics[topicid]["topicid"] = topicid
						topics[topicid]["type"] = int(row[1])
						
		
		alltopics = {}
		
		#所有热门
		sql = """\
		select g.topicid,count(c.commentId) comment from hd_haodou_comment_%s.comment c
		inner join hd_haodou_center_%s.grouptopic g on g.topicid=c.itemid
		where to_date(c.createtime) = '%s' and c.status=1 and c.type=6 and g.status=1
		group by g.topicid having comment >=15;
		"""
		
		
		#2.按类型筛选出id
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				topicid = int(row[0])
				if topicid not in topics:
					#热门、非推荐、非精华 0
					if topicid not in alltopics:
						alltopics[topicid] = {}
						alltopics[topicid]["type"] = 0
				else:
					#热门、精华
					if topics[topicid]["type"] == 0:
						if topicid not in alltopics:
							alltopics[topicid] = {}
							alltopics[topicid]["type"] = 2
					#热门、推荐 		
					elif topics[topicid]["type"] == 1:
						if topicid not in alltopics:
							alltopics[topicid] = {}
							alltopics[topicid]["type"] = 1
		
		#删除 同时属于 热门、精华和热门、推荐的id
		for topicid in alltopics.keys():
			if topicid in topics:
				del topics[topicid]
			
		for topicid in topics.keys():
			#精华、非热门 4
			if topicid not in alltopics and topics[topicid]["type"] == 0:
				alltopics[topicid] = {}
				alltopics[topicid]["type"] = 4
			#推荐、非热门 3	
			elif topicid not in alltopics and topics[topicid]["type"] == 1:
				alltopics[topicid] = {}
				alltopics[topicid]["type"] = 3
		
		topicstr = ""
		for topicid in alltopics.keys():
			topicstr += str(topicid) + ","

		if "" != topicstr:
			topicstr = topicstr[0:len(topicstr)-1]
		
		#3.计算浏览量和回评量
				
		sql = """\
		select createtime,'###',topicid,'###',cateid,'###',userid,'###',username,'###',title,'###',sum(pv),'###',sum(comment) from (
		select g.createtime,g.topicid,g.cateid,g.userid,u.username,g.title,0 as pv, count(c.commentId) comment
		from hd_haodou_comment_%s.comment c
		inner join hd_haodou_center_%s.grouptopic g on g.topicid=c.itemid
		inner join hd_haodou_passport_%s.user u on u.userid=g.userid
		where to_date(c.createtime) = '%s' and c.status=1 and c.type=6 and g.topicid in (%s) and g.status=1 and to_date(g.createtime) = '%s'
		group by g.createtime,g.topicid,g.cateid,g.userid,u.username,g.title
		union all
		select g.createtime,g.topicid,g.cateid,g.userid,u.username,g.title,count(m.path) pv, 0 as comment
		from hd_haodou_center_%s.grouptopic g
		inner join logs.group_haodou_com m on gtifurl(m.path)=g.topicid
		inner join hd_haodou_passport_%s.user u on u.userid=g.userid
		where m.logdate='%s' and g.topicid in (%s) and g.status=1 and to_date(g.createtime) = '%s'
		group by g.createtime,g.topicid,g.cateid,g.userid,u.username,g.title
		) tmp
		group by createtime,topicid,cateid,userid,username,title;	
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.nowDate,self.curDate,topicstr,self.curDate,self.nowDate,self.nowDate,self.curDate,topicstr,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"###",res)
				if len(row) > 1:
					createtime = str(row[0]).strip()
					topicid = int(row[1])
					groupid = int(row[2])
					userid = int(row[3])
					username = str(row[4]).strip()
					title = str(row[5]).strip()
					viewcount = int(row[6])
					comment = int(row[7])
					
					
					sql = "INSERT INTO bi_group_quality_topic (createtime,topicid,cateid,userid,username,title,viewcount,comment,type) VALUES ('%s',%s,%s,%s,'%s','%s',%s,%s,%s)"
					self.db.execute(sql%(createtime,topicid,groupid,userid,username,title,viewcount,comment,alltopics[topicid]["type"]))
		
		
		
	
	#每日话题分享
	def topicShare(self):
		
		sql = "delete from bi_group_share_topic where date(createtime)='%s'"
		self.db.execute(sql%self.curDate)
		
		topicstr = ""
		sql = """\
		select g.topicid from hd_haodou_center_%s.userfeed f
		inner join hd_haodou_center_%s.grouptopic g on g.topicid=f.itemid
		where to_date(f.createtime) = '%s' and to_date(g.createtime)='%s' and f.type =403 and g.status=1;
		"""
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate,self.curDate))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				topicid = str(row[0])
				topicstr += topicid + ","
		
			if "" != topicstr:
				topicstr = topicstr[0:len(topicstr)-1]
		
		sql = """\
		select createtime,'###',topicid,'###',userid,'###',username,'###',title,'###',sum(share),'###',sum(sp),'###',sum(pv),'###',sum(comment) from(
		select g.createtime,g.topicid,g.userid,u.username,g.title,count(f.feedid) share,count(distinct(f.userid)) sp,0 as pv, 0 as comment
		from hd_haodou_center_%s.userfeed f
		inner join hd_haodou_center_%s.grouptopic g on g.topicid=f.itemid
		inner join hd_haodou_passport_%s.user u on u.userid=g.userid
		where to_date(f.createtime) = '%s' and to_date(g.createtime)='%s' and f.type =403 and g.status=1 and g.topicid in (%s)
		group by g.createtime,g.topicid,g.userid,u.username,g.title
		union all
		select g.createtime,g.topicid,g.userid,u.username,g.title,0 as share, 0 as sp, count(m.path) pv, 0 as comment
		from hd_haodou_center_%s.grouptopic g
		inner join logs.group_haodou_com m on gtifurl(m.path)=g.topicid
		inner join hd_haodou_passport_%s.user u on u.userid=g.userid
		where m.logdate='%s' and g.status=1 and to_date(g.createtime) = '%s' and g.topicid in (%s)
		group by g.createtime,g.topicid,g.userid,u.username,g.title
		union all
		select g.createtime,g.topicid,g.userid,u.username,g.title,0 as share, 0 as sp,0 as pv, count(c.commentId) comment
		from hd_haodou_comment_%s.comment c
		inner join hd_haodou_center_%s.grouptopic g on g.topicid=c.itemid
		inner join hd_haodou_passport_%s.user u on u.userid=g.userid
		where to_date(c.createtime) = '%s' and c.status=1 and c.type=6 and g.status=1 and to_date(g.createtime) = '%s' and c.replyid=0 and g.topicid in (%s)
		group by g.createtime,g.topicid,g.userid,u.username,g.title
		) tmp
		group by createtime,topicid,userid,username,title;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.nowDate,self.curDate,self.curDate,topicstr,self.nowDate,self.nowDate,self.curDate,self.curDate,topicstr,self.nowDate,self.nowDate,self.nowDate,self.curDate,self.curDate,topicstr))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"###",res)
				if len(row) > 1:
					createtime = str(row[0]).strip()
					topicid = int(row[1])
					userid = int(row[2])
					username = str(row[3]).strip()
					title = str(row[4]).strip()
					share = int(row[5])
					sp = int(row[6])
					viewcount = int(row[7])
					comment = int(row[8])
					
					
					sql = "INSERT INTO bi_group_share_topic (createtime,topicid,userid,username,title,share,share_person,viewcount,comment) VALUES ('%s',%s,%s,'%s','%s',%s,%s,%s,%s)"
					self.db.execute(sql%(createtime,topicid,userid,username,title,share,sp,viewcount,comment))
	
	
	#每日话题收藏
	def topicFavorite(self):
		
		sql = "delete from bi_group_favorite_topic where date(createtime)='%s'"
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select topicid,sum(pv) from (
		select g.topicid,count(w.remote_addr) pv
		from group_haodou_com w
		inner join hd_haodou_center_"""+self.nowDate+""".grouptopic g on gffp(w.path)=g.topicid
		where w.logdate='"""+self.curDate+"""' and g.status=1 and to_date(g.createtime)='"""+self.curDate+"""'
		and w.path like '/topic.php?do=fav%type=add'
		group by g.topicid
		union all	
		select g.topicid,count(concat(l.device_id,l.uuid)) pv
		from log_php_app_log l
		inner join hd_haodou_center_"""+self.nowDate+""".grouptopic g on gffp(l.parameter_desc)=g.topicid
		where l.logdate='"""+self.curDate+"""' and l.appid in (2,4,6) and l.function_id ='favorite.add' and g.status=1 and to_date(g.createtime)='"""+self.curDate+"""'
		group by g.topicid
		) tmp
		group by topicid;
		"""
		
		
		topicstr = ""
		topics = {}
		cursor = self.hive.execute(sql)
		if cursor is not None:
			for res in cursor:
				row = re.split(r"\s+",res)
				topicid = int(row[0])
				if topicid not in topics:
					topics[topicid] = {}
					topics[topicid]["favorite"] = int(row[1])
				topicstr += str(topicid) + ","
		
			if "" != topicstr:
				topicstr = topicstr[0:len(topicstr)-1]
					
			
		sql = """\
		select userid,'###',username,'###',createtime,'###',topicid,'###',title,'###',sum(pv),'###',sum(comment) from (
		select g.userid,u.username,g.createtime,g.topicid,g.title,count(m.path) pv, 0 as comment
		from hd_haodou_center_%s.grouptopic g
		inner join logs.group_haodou_com m on gtifurl(m.path)=g.topicid
		inner join hd_haodou_passport_%s.user u on u.userid=g.userid
		where m.logdate='%s' and g.status=1 and to_date(g.createtime) = '%s' and g.topicid in (%s)
		group by g.userid,u.username,g.createtime,g.topicid,g.title
		union all
		select g.userid,u.username,g.createtime,g.topicid,g.title,0 as pv, count(c.commentId) comment
		from hd_haodou_comment_%s.comment c
		inner join hd_haodou_center_%s.grouptopic g on g.topicid=c.itemid
		inner join hd_haodou_passport_%s.user u on u.userid=g.userid
		where to_date(c.createtime) = '%s' and c.status=1 and c.type=6 and g.status=1 and to_date(g.createtime) = '%s' and c.replyid=0 and g.topicid in (%s)
		group by g.userid,u.username,g.createtime,g.topicid,g.title
		) tmp
		group by userid,username,createtime,topicid,title;
		"""		
		
		cursor = self.hive.execute(sql%(self.nowDate,self.nowDate,self.curDate,self.curDate,topicstr,self.nowDate,self.nowDate,self.nowDate,self.curDate,self.curDate,topicstr))
		if cursor is not None:
			for res in cursor:
				row = re.split(r"###",res)
				if len(row) > 1:
					userid = int(row[0])
					username = str(row[1]).strip()
					createtime = str(row[2])
					topicid = int(row[3])
					title = str(row[4]).strip()
					viewcount = int(row[5])
					comment = int(row[6])
					
					sql = "INSERT INTO bi_group_favorite_topic (userid,username,createtime,topicid,title,viewcount,comment,favorite) VALUES (%s,'%s','%s',%s,'%s',%s,%s,%s)"
					self.db.execute(sql%(userid,username,createtime,topicid,title,viewcount,comment,topics[topicid]["favorite"]))
		
		
	def groupData(self):
		
		sql = "delete from bi_group_user_data where date(createtime)='%s' "
		self.db.execute(sql%self.curDate)
		
		sql = """\
		select tmp.userid,'#',p.username,'#',u.birthday,'#',p.logintime,'#',sum(topiccomment),'#',sum(topic),'#',sum(maincomment),'#',sum(comment),'#',sum(recommend),'#',sum(digest),'#',
		sum(kitchen),'#',sum(healthy),'#',sum(collection),'#',sum(live),'#',sum(photograph),'#',sum(travell),'#',sum(children),'#',sum(hobby),'#',sum(public) from(
		select u.userid,count(distinct(c.itemid)) topiccomment, 0 as topic,0 as maincomment,0 as comment,0 as recommend,0 as digest,0 as kitchen, 0 as healthy,0 as collection,0 as live,
		0 as photograph,0 as travell,0 as children,0 as hobby,0 as public
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_comment_"""+self.nowDate+""".comment c on u.userid=c.userid
		where c.status=1 and c.type=6 and to_date(c.createtime)='"""+self.curDate+"""' and length(html2text(c.content)) > 10
		group by u.userid
		union all 
		select u.userid,0 as topiccomment,0 as topic, count(c.commentid) maincomment,0 as comment,0 as recommend,0 as digest,0 as kitchen, 0 as healthy,0 as collection,0 as live,
		0 as photograph,0 as travell,0 as children,0 as hobby,0 as public
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_comment_"""+self.nowDate+""".comment c on c.userid=u.userid
		where c.status=1 and c.type=6 and to_date(c.createtime)='"""+self.curDate+"""' and c.replyid=0 and length(html2text(c.content)) > 10
		group by u.userid
		union all 
		select u.userid,0 as topiccomment,0 as topic, 0 as maincomment,count(c.commentid) comment,0 as recommend,0 as digest,0 as kitchen, 0 as healthy,0 as collection,0 as live,
		0 as photograph,0 as travell,0 as children,0 as hobby,0 as public
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_comment_"""+self.nowDate+""".comment c on c.userid=u.userid
		where c.status=1 and c.type=6 and to_date(c.createtime)='"""+self.curDate+"""' and length(html2text(c.content)) > 10
		group by u.userid
		union all
		select u.userid,0 as topiccomment,count(g.topicid) topic, 0 as maincomment,0 as comment,0 as recommend,0 as digest,0 as kitchen, 0 as healthy,0 as collection,0 as live,
		0 as photograph,0 as travell,0 as children,0 as hobby,0 as public
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".grouptopic g on g.userid=u.userid
		where g.status=1 and to_date(g.createtime)='"""+self.curDate+"""'
		group by u.userid
		union all
		select u.userid,0 as topiccomment,0 as topic, 0 as maincomment,0 as comment,count(g.topicid) recommend,0 as digest,0 as kitchen, 0 as healthy,0 as collection,0 as live,
		0 as photograph,0 as travell,0 as children,0 as hobby,0 as public
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".grouptopic g on g.userid=u.userid
		where g.status=1 and to_date(g.createtime)='"""+self.curDate+"""' and g.recommend =1
		group by u.userid
		union all
		select u.userid,0 as topiccomment,0 as topic, 0 as maincomment,0 as comment,0 as recommend,count(g.topicid) digest,0 as kitchen, 0 as healthy,0 as collection,0 as live,
		0 as photograph,0 as travell,0 as children,0 as hobby,0 as public
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".grouptopic g on g.userid=u.userid
		where g.status=1 and to_date(g.createtime)='"""+self.curDate+"""' and g.digest=1
		group by u.userid
		union all
		select u.userid,0 as topiccomment,0 as topic, 0 as maincomment,0 as comment,0 as recommend,0 as digest, count(g.topicid) kitchen, 0 as healthy,0 as collection,0 as live,
		0 as photograph,0 as travell,0 as children,0 as hobby,0 as public
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".grouptopic g on g.userid=u.userid
		where g.status=1 and to_date(g.createtime)='"""+self.curDate+"""' and g.cateid =5
		group by u.userid
		union all
		select u.userid,0 as topiccomment,0 as topic, 0 as maincomment,0 as comment,0 as recommend,0 as digest, 0 as kitchen,count(g.topicid) healthy,0 as collection,0 as live,
		0 as photograph,0 as travell,0 as children,0 as hobby,0 as public
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".grouptopic g on g.userid=u.userid
		where g.status=1 and to_date(g.createtime)='"""+self.curDate+"""' and g.cateid =6
		group by u.userid
		union all
		select u.userid,0 as topiccomment,0 as topic, 0 as maincomment,0 as comment,0 as recommend,0 as digest, 0 as kitchen,0 as healthy, count(g.topicid) collection,0 as live,
		0 as photograph,0 as travell,0 as children,0 as hobby,0 as public
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".grouptopic g on g.userid=u.userid
		where g.status=1 and to_date(g.createtime)='"""+self.curDate+"""' and g.cateid =8
		group by u.userid
		union all
		select u.userid,0 as topiccomment,0 as topic, 0 as maincomment,0 as comment,0 as recommend,0 as digest, 0 as kitchen,0 as healthy,0 as collection, count(g.topicid) live,
		0 as photograph,0 as travell,0 as children,0 as hobby,0 as public
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".grouptopic g on g.userid=u.userid
		where g.status=1 and to_date(g.createtime)='"""+self.curDate+"""' and g.cateid =12
		group by u.userid
		union all
		select u.userid,0 as topiccomment,0 as topic, 0 as maincomment,0 as comment,0 as recommend,0 as digest, 0 as kitchen,0 as healthy,0 as collection,0 as live, 
		count(g.topicid) photograph,0 as travell,0 as children,0 as hobby,0 as public
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".grouptopic g on g.userid=u.userid
		where g.status=1 and to_date(g.createtime)='"""+self.curDate+"""' and g.cateid =11
		group by u.userid
		union all
		select u.userid,0 as topiccomment,0 as topic, 0 as maincomment,0 as comment,0 as recommend,0 as digest, 0 as kitchen,0 as healthy,0 as collection,0 as live, 
		0 as photograph,count(g.topicid) travell,0 as children,0 as hobby,0 as public
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".grouptopic g on g.userid=u.userid
		where g.status=1 and to_date(g.createtime)='"""+self.curDate+"""' and g.cateid =10
		group by u.userid
		union all
		select u.userid,0 as topiccomment,0 as topic, 0 as maincomment,0 as comment,0 as recommend,0 as digest, 0 as kitchen,0 as healthy,0 as collection,0 as live, 
		0 as photograph,0 as travell,count(g.topicid) children,0 as hobby,0 as public
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".grouptopic g on g.userid=u.userid
		where g.status=1 and to_date(g.createtime)='"""+self.curDate+"""' and g.cateid =23
		group by u.userid
		union all
		select u.userid,0 as topiccomment,0 as topic, 0 as maincomment,0 as comment,0 as recommend,0 as digest, 0 as kitchen,0 as healthy,0 as collection,0 as live, 
		0 as photograph,0 as travell,0 as children,count(g.topicid) hobby,0 as public
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".grouptopic g on g.userid=u.userid
		where g.status=1 and to_date(g.createtime)='"""+self.curDate+"""' and g.cateid =27
		group by u.userid
		union all
		select u.userid,0 as topiccomment,0 as topic, 0 as maincomment,0 as comment,0 as recommend,0 as digest, 0 as kitchen,0 as healthy,0 as collection,0 as live, 
		0 as photograph,0 as travell,0 as children,0 as hobby,count(g.topicid) public
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".grouptopic g on g.userid=u.userid
		where g.status=1 and to_date(g.createtime)='"""+self.curDate+"""' and g.cateid =17
		group by u.userid
		) tmp 
		inner join hd_haodou_passport_"""+self.nowDate+""".user p on p.userid=tmp.userid
		left outer join hd_recipe_user_"""+self.nowDate+""".user u on u.userid=tmp.userid
		group by tmp.userid,p.username,u.birthday,p.logintime;
		"""
		
		useridstr = ""
		userids = {}
		datas = {}
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"#",cols)
			if len(row) > 1:
				userid = int(row[0].strip())
				if userid not in datas:
					datas[userid] = {}
					datas[userid]["username"] = str(row[1].strip())
					datas[userid]["birthday"] = str(row[2].strip())
					datas[userid]["logintime"] = str(row[3].strip())
					datas[userid]["topiccomment"] = int(row[4].strip())
					datas[userid]["topic"] = int(row[5].strip())
					datas[userid]["maincomment"] = int(row[6].strip())
					datas[userid]["comment"] = int(row[7].strip())
					datas[userid]["recommend"] = int(row[8].strip())
					datas[userid]["digest"] = int(row[9].strip())
					datas[userid]["kitchen"] = int(row[10].strip())
					datas[userid]["healthy"] = int(row[11].strip())
					datas[userid]["collection"] = int(row[12].strip())
					datas[userid]["live"] = int(row[13].strip())
					datas[userid]["photograph"] = int(row[14].strip())
					datas[userid]["travell"] = int(row[15].strip())
					datas[userid]["children"] = int(row[16].strip())
					datas[userid]["hobby"] = int(row[17].strip())
					datas[userid]["public"] = int(row[18].strip())
					datas[userid]["type"] = -1
					
					userids[userid] = userid
					
					useridstr += str(userid) + ","
		
		if "" != useridstr:
			useridstr = useridstr[0:len(useridstr)-1]
		
		print len(userids)
					
		self.findMinMaxTime(useridstr)
		
		#给用户分类 4类进行筛选
		#类型为 操作性、内容型、潜力型  待开发型四类用户
		#一 每天登陆手机端和网站端，并在其两方都有动作的用户（发布、评论、分享、收藏、发私信）：操作核心用户
		sql = """\
		select userid,sum(topic) topic,sum(comment) comment,sum(share) share,sum(favorite) favorite,sum(message) message from (
		select u.userid,count(g.topicid) topic, 0 as comment,0 as share,0 as favorite,0 as message
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".grouptopic g on g.userid=u.userid
		where g.status=1 and to_date(g.createtime)='"""+self.curDate+"""' and u.userid in ("""+useridstr+""") 
		group by u.userid having topic > 0
		union all
		select u.userid,0 as topic, count(c.commentid) comment,0 as share,0 as favorite,0 as message
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_comment_"""+self.nowDate+""".comment c on c.userid=u.userid
		where c.status=1 and c.type=6 and to_date(c.createtime)='"""+self.curDate+"""' and length(html2text(c.content)) > 10 and u.userid in ("""+useridstr+""")
		group by u.userid having comment > 0
		union all
		select u.userid,0 as topic, 0 as comment,count(f.feedid) share,0 as favorite,0 as message
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".userfeed f on f.userid=u.userid
		where to_date(f.createtime) = '"""+self.curDate+"""' and f.type =403 and u.userid in ("""+useridstr+""")
		group by u.userid having share > 0
		union all
		select userid,0 as topic, 0 as comment,0 as share,sum(favorite) favorite,0 as message from (
		select u.userid,count(w.remote_addr) favorite
		from group_haodou_com w
		inner join hd_haodou_passport_"""+self.nowDate+""".user u on u.userid=w.user_id
		where w.logdate='"""+self.curDate+"""' and w.path like '/topic.php?do=fav%type=add' and u.userid in ("""+useridstr+""")
		group by u.userid
		union all	
		select u.userid,count(concat(l.device_id,l.uuid)) favorite
		from log_php_app_log l
		inner join hd_haodou_passport_"""+self.nowDate+""".user u on u.userid=l.userid
		where l.logdate='"""+self.curDate+"""' and l.appid in (2,4,6) and l.function_id ='favorite.add' and u.userid in ("""+useridstr+""")
		group by u.userid
		) tmp
		group by userid having favorite > 0
		union all
		select userid,0 as topic, 0 as comment,0 as share,0 as favorite,count(m.replyid) message 
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".usermessagereply m on m.userid=u.userid and u.userid in ("""+useridstr+""")
		where to_date(m.createtime) = '"""+self.curDate+"""'
		group by u.userid having message > 0
		) tmp
		group by userid having topic > 0 and comment > 0 and share > 0 and favorite > 0 and message > 0;
		"""
		
		
		cursor = self.hive.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 1:
				userid = int(row[0].strip())
			
				if userid in userids:
					
					if userid in datas:
						datas[userid]["type"] = 0
						print userid
						del userids[userid]
		
		useridstr = ""
		
		for userid in userids:
			useridstr += str(userid) + ","
		
		print len(userids)
		
		if "" != useridstr:
			useridstr = useridstr[0:len(useridstr)-1]
				
		#二 每天登陆手机端或者网站端，在其中的一方有动作的用户（发布、评论、分享、收藏、发私信）：操作准核心用户
		
		sql = """\
		select userid,sum(topic) topic,sum(comment) comment,sum(share) share,sum(favorite) favorite,sum(message) message from (
		select u.userid,count(g.topicid) topic, 0 as comment,0 as share,0 as favorite,0 as message
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".grouptopic g on g.userid=u.userid
		where g.status=1 and to_date(g.createtime)='"""+self.curDate+"""' and u.userid in ("""+useridstr+""") 
		group by u.userid
		union all
		select u.userid,0 as topic, count(c.commentid) comment,0 as share,0 as favorite,0 as message
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_comment_"""+self.nowDate+""".comment c on c.userid=u.userid
		where c.status=1 and c.type=6 and to_date(c.createtime)='"""+self.curDate+"""' and length(html2text(c.content)) > 10 and u.userid in ("""+useridstr+""")
		group by u.userid
		union all
		select u.userid,0 as topic, 0 as comment,count(f.feedid) share,0 as favorite,0 as message
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".userfeed f on f.userid=u.userid
		where to_date(f.createtime) = '"""+self.curDate+"""' and f.type =403 and u.userid in ("""+useridstr+""")
		group by u.userid
		union all
		select userid,0 as topic, 0 as comment,0 as share,sum(favorite) favorite,0 as message from (
		select u.userid,count(w.remote_addr) favorite
		from group_haodou_com w
		inner join hd_haodou_passport_"""+self.nowDate+""".user u on u.userid=w.user_id
		where w.logdate='"""+self.curDate+"""' and w.path like '/topic.php?do=fav%type=add' and u.userid in ("""+useridstr+""")
		group by u.userid
		union all	
		select u.userid,count(concat(l.device_id,l.uuid)) favorite
		from log_php_app_log l
		inner join hd_haodou_passport_"""+self.nowDate+""".user u on u.userid=l.userid
		where l.logdate='"""+self.curDate+"""' and l.appid in (2,4,6) and l.function_id ='favorite.add' and u.userid in ("""+useridstr+""")
		group by u.userid
		) tmp
		group by userid
		union all
		select u.userid,0 as topic, 0 as comment,0 as share,0 as favorite,count(m.replyid) message 
		from hd_haodou_passport_"""+self.nowDate+""".user u
		inner join hd_haodou_center_"""+self.nowDate+""".usermessagereply m on m.userid=u.userid and u.userid in ("""+useridstr+""")
		where to_date(m.createtime) = '"""+self.curDate+"""'
		group by u.userid
		) tmp
		group by userid having (topic > 0 or comment > 0 or share > 0 or favorite > 0 or message > 0);
		"""
		
		if useridstr != "":
			cursor = self.hive.execute(sql)
			for cols in cursor:
				row = re.split(r"\s+",cols)
				if len(row) > 1:
					userid = int(row[0].strip())
				
					if userid in userids:
						
						if userid in datas:
							datas[userid]["type"] = 1
							print userid
							del userids[userid]

		useridstr = ""
		
		for userid in userids:
			useridstr += str(userid) + ","
			
		print len(userids)
			
		if "" != useridstr:
			useridstr = useridstr[0:len(useridstr)-1]	
		
		#三 每天发布的内容（带有推荐 精华）标签的用户，可以查询到用户ID和当天发布的内容链接  ：内容核心用户
 
		sql = """\
		set hivevar:date=%s;
		set hivevar:curdate=%s;
		set hivevar:userids=%s;
		select userid,sum(recommend) recommend,sum(digest) digest from (
		select u.userid,count(g.topicid) recommend,0 as digest
		from hd_haodou_passport_\${date}.user u
		inner join hd_haodou_center_\${date}.grouptopic g on g.userid=u.userid
		where g.status=1 and to_date(g.createtime)='\${curdate}' and g.recommend =1 and g.userid in (\${userids})
		group by u.userid
		union all
		select u.userid,0 as recommend,count(g.topicid) digest
		from hd_haodou_passport_\${date}.user u
		inner join hd_haodou_center_\${date}.grouptopic g on g.userid=u.userid
		where g.status=1 and to_date(g.createtime)='\${curdate}' and g.digest=1 and g.userid in (\${userids})
		group by u.userid
		) tmp
		group by userid having (recommend > 1 or digest > 1);
		"""
		
		if useridstr != "":
			cursor = self.hive.execute(sql%(self.nowDate,self.curDate,useridstr))
			for cols in cursor:
				row = re.split(r"\s+",cols)
				if len(row) > 1:
					userid = int(row[0].strip())
				
					if userid in userids:
						
						if userid in datas:
							datas[userid]["type"] = 2
							print userid
							del userids[userid]
		
		useridstr = ""
		
		for userid in userids:
			useridstr += str(userid) + ","
			
		print len(userids)
		
		if "" != useridstr:
			useridstr = useridstr[0:len(useridstr)-1]
		
		#四  每天登陆网站端 或者 每天登陆手机端的用户（只有签到或者浏览的动作）  待开发型用户
		
		#签到
		#浏览
		sql = """\
		select user_id,sum(pv) pv from(
		select user_id,count(remote_addr) pv from www_haodou_com
		where user_id <> 0 and logdate='"""+self.curDate+"""' and user_id in ("""+useridstr+""")
		group by user_id having pv > 0
		union all
		select user_id,count(remote_addr) pv from group_haodou_com
		where user_id <> 0 and logdate='"""+self.curDate+"""' and user_id in ("""+useridstr+""")
		group by user_id having pv > 0
		union all
		select user_id,count(remote_addr) pv from m_haodou_com
		where user_id <> 0 and logdate='"""+self.curDate+"""' and user_id in ("""+useridstr+""")
		group by user_id having pv > 0
		union all
		select user_id,count(remote_addr) pv from wo_haodou_com
		where user_id <> 0 and logdate='"""+self.curDate+"""' and user_id in ("""+useridstr+""")
		group by user_id having pv > 0
		union all
		select userid user_id,count(concat(device_id,uuid)) pv from log_php_app_log
		where userid <> 0 and logdate='"""+self.curDate+"""' and userid in ("""+useridstr+""")
		group by userid having pv > 0
		union all
		select user_id,count(remote_addr) pv from wo_haodou_com 
		where path like '/user/sign.php?do=Sign%' and logdate='"""+self.curDate+"""'
		and user_id in ("""+useridstr+""")
		group by user_id having pv > 0
		union all
		select userid user_id,count(concat(device_id,uuid)) pv from log_php_app_log where function_id='account.checkin' and logdate='"""+self.curDate+"""'
		and userid in ("""+useridstr+""")
		group by userid having pv > 0
		) tmp
		group by user_id having pv > 0;
		"""		
		
		if useridstr != "":
			cursor = self.hive.execute(sql)
			for cols in cursor:
				row = re.split(r"\s+",cols)
				if len(row) > 1:
					userid = int(row[0].strip())
				
					if userid in userids:
						
						if userid in datas:
							datas[userid]["type"] = 3
							print userid
							del userids[userid]
				
		sql = """\
		insert into bi_group_user_data (`userid`,`username`,`birthday`,`topiccomment`,`topic`,
		`maincomment`,`comment`,`recommend`,`digest`,`kitchen`,`healthy`,`collection`,`live`,`photograph`,
		`travell`,`children`,`hobby`,`public`,`classes`,`logintime`,`topictime`,`remark`,`createtime`) values 
		(%s,'%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'%s','%s','','%s');
		"""
		for userid in datas:
			
			username = datas[userid]["username"]
			birthday = datas[userid]["birthday"]
			logintime = datas[userid]["logintime"]
			topiccomment = datas[userid]["topiccomment"]
			topic = datas[userid]["topic"]
			maincomment = datas[userid]["maincomment"]
			comment = datas[userid]["comment"]
			recommend = datas[userid]["recommend"]
			digest = datas[userid]["digest"]
			kitchen = datas[userid]["kitchen"]
			healthy = datas[userid]["healthy"]
			collection = datas[userid]["collection"]
			live = datas[userid]["live"]
			photograph = datas[userid]["photograph"]
			travell = datas[userid]["travell"]
			children = datas[userid]["children"]
			hobby = datas[userid]["hobby"]
			public = datas[userid]["public"]
			classes = datas[userid]["type"]
			
			topictime = ''
			if userid in self.usersTime:
				topictime = self.usersTime[userid]
			
				
			self.db.execute(sql%(userid,username,birthday,topiccomment,topic,maincomment,comment,recommend,digest,kitchen,healthy,collection,live,photograph,travell,children,hobby,public,classes,logintime,logintime,self.curDate))	
	
	def findMinMaxTime(self, usersid):
	
		sql = """\
		select userid,',',max(createtime) 
		from hd_haodou_center_%s.grouptopic 
		where userid in (%s)
		group by userid;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,usersid))
		for cols in cursor:
			row = re.split(",",cols)
			userid = int(row[0].strip())
			maxdate = str(row[1].strip())
			
			if userid not in self.usersTime:
				self.usersTime[userid] = maxdate
		
if __name__ == '__main__':
	
	usage = "usage: %prog [options] arg1 arg2"
	parser = OptionParser(usage = usage, version="%prog 0.1")
	parser.add_option("-d", "--delay", help = 'Delay a number day', dest = "delay")
	(options, args) = parser.parse_args()
	
	if len(sys.argv)<=1:
		parser.print_help()
		sys.exit(2)

	delay = options.delay or 1
	
	delay = int(delay) 
	
	path  = os.path.dirname(os.path.abspath(__file__))
	
	obj = GroupMobileWebDataSummary(path, delay)
	obj.start()
