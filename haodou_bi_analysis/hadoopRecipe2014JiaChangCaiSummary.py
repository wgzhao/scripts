# -*- coding: UTF-8 -*- 
#coding = utf-8
#菜谱2014国民家常菜数据收集统计

from optparse import OptionParser
import commands
from time import ctime
from MailUtil import *
from XLSWriter import XLSWriter
from sharkDB import *
from timeUtils import *
import traceback
from  datetime  import  *
import time
import sys
import os
import re



class Recipe2014JiaChangCaiSummary:

	def __init__(self, delay=1):
		self.delay = delay
		self.shark = SharkDB()
		dt = datetime.now()
		self._datetime = dt - timedelta(days = delay)
		self.curDate = self._datetime.strftime("%Y-%m-%d")
		self.tu = TimeUtils()
		self.shortDate = dt.strftime("%Y%m%d")
	
	#【小组活动】“国民家常菜”活动数据需求
	def start(self):
				
		t = time.mktime(self._datetime.timetuple())
		#算上周一
		startWeekDate = time.strftime('%Y-%m-%d',time.localtime(self.tu.get_week_begin(t,-1)))
		#计算上周末
		endWeekDate = (datetime.fromtimestamp(self.tu.get_week_begin(t,-1)) + timedelta(days = 6)).strftime('%Y-%m-%d')
		
		#关于283485话题的基本数据
		
		sql = "select logdate,count(1),count(distinct(concat(remote_addr,'-',http_user_agent))) from logs.http_group_haodou_com \
		where logdate between '" + startWeekDate  + "' and '" + endWeekDate + "' and path like '/topic-283485%' group by logdate order by logdate desc; "
		
		cursor = self.shark.execute(sql)
		print "关于283485话题的 总PV UV数"
		
		for cols in cursor:
			row = re.split(r"\s+",cols)
			logdate = str(row[0])
			pv = str(row[1])
			uv = str(row[2])
			print logdate,',',pv,',',uv
		
		sql = """select to_date(createtime) createtime,count(1),count(distinct(c.userid)) from hd_haodou_comment_%s.comment c \
		where c.itemid=283485 and c.type=6 and c.status in (1,2) 
		and createtime between '%s 00:00:00' and '%s 23:59:59'
		group by to_date(createtime)
		order by createtime desc;"""
		
		cursor = self.shark.execute(sql%(self.shortDate,startWeekDate,endWeekDate))
		print "关于283485话题的 回复数 回复人数"
		for cols in cursor:
			row = re.split(r"\s+",cols)
			logdate = str(row[0])
			ct = str(row[1])
			pct = str(row[2])
			print logdate,',',ct,',',pct
			
		#关于283485话题的分类PV UV		
		
		sql = "select logdate,count(1),count(distinct(concat(remote_addr,'-',http_user_agent))) from logs.http_group_haodou_com where logdate between '" + startWeekDate + "' and '" + endWeekDate + "' and path like '%/topic-283485%' \
		and referer like '%haodou.com%' group by logdate order by logdate desc;"
		
		cursor = self.shark.execute(sql)
		print "关于283485话题的 网站端 PV UV数"
		
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 2:
				logdate = str(row[0])
				pv = str(row[1])
				uv = str(row[2])
				print logdate,',',pv,',',uv
		
		sql = "select logdate,count(1),count(distinct(concat(remote_addr,'-',http_user_agent))) from logs.http_group_haodou_com where logdate between '" + startWeekDate + "' and '" + endWeekDate + "' and path like '%/topic-283485%' \
		and referer like '%t.qq.com%' group by logdate order by logdate desc;"
		
		cursor = self.shark.execute(sql)
		print "关于283485话题的 腾讯微博 PV UV数"
		
		
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 2:
				logdate = str(row[0])
				pv = str(row[1])
				uv = str(row[2])
				print logdate,',',pv,',',uv
		
		sql = "select logdate,count(1),count(distinct(concat(remote_addr,'-',http_user_agent))) from logs.http_group_haodou_com where logdate between '" + startWeekDate + "' and '" + endWeekDate + "' and path like '%/topic-283485%' \
		and referer like '%weibo%' group by logdate order by logdate desc;"
		
		cursor = self.shark.execute(sql)
		print "关于283485话题的 新浪微博 PV UV数"
		
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 2:
				logdate = str(row[0])
				pv = str(row[1])
				uv = str(row[2])
				print logdate,',',pv,',',uv
		
		sql = "select logdate,count(1),count(distinct(concat(remote_addr,'-',http_user_agent))) from logs.http_group_haodou_com where logdate between '" + startWeekDate + "' and '" + endWeekDate + "' and path like '%/topic-283485%' \
		and (referer like '%qzs.qq.com%' or referer like '%qzone%') group by logdate order by logdate desc;"
		
		cursor = self.shark.execute(sql)
		print "关于283485话题的 QQ空间 PV UV数"
		
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 2:
				logdate = str(row[0])
				pv = str(row[1])
				uv = str(row[2])
				print logdate,',',pv,',',uv
			
		sql = "select logdate,count(1),count(distinct(concat(remote_addr,'-',http_user_agent))) from logs.http_m_haodou_com where logdate between '" + startWeekDate + "' and '" + endWeekDate + "' and path like '%view.php?id=283485%' \
		and referer like '%mp.weixin.qq.com%' group by logdate order by logdate desc;"
		
		cursor = self.shark.execute(sql)
		print "关于283485话题的 微信 PV UV数"
		
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 2:
				logdate = str(row[0])
				pv = str(row[1])
				uv = str(row[2])
				print logdate,',',pv,',',uv
			
		sql = "select logdate,count(1),count(distinct(concat(remote_addr,'-',http_user_agent))) from logs.http_m_haodou_com where logdate between '" + startWeekDate + "' and '" + endWeekDate + "' and path like '%view.php?id=283485%' \
		and referer not like '%mp.weixin.qq.com%' group by logdate order by logdate desc;"
		
		cursor = self.shark.execute(sql)
		print "关于283485话题的 移动端 PV UV数"
		
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 2:
				logdate = str(row[0])
				pv = str(row[1])
				uv = str(row[2])
				print logdate,',',pv,',',uv
		
	def run(self):
		
		t = time.mktime(self._datetime.timetuple())
		#算上周一
		startWeekDate = time.strftime('%Y-%m-%d',time.localtime(self.tu.get_week_begin(t,-1)))
		#计算上周末
		endWeekDate = (datetime.fromtimestamp(self.tu.get_week_begin(t,-1)) + timedelta(days = 6)).strftime('%Y-%m-%d')
		
		#关于“国民家常菜”话题的基本数据
		
		topics = []
		
		sql = "select concat('/topic-',c.topicid,'%') from hd_haodou_center_" + self.shortDate + ".grouptopic c \
		where c.createtime between '" + startWeekDate + " 00:00:00' and '" + endWeekDate + " 23:59:59' and c.status in (1,2) and c.title like '%国民家常菜%'; "
		cursor = self.shark.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			topics.append(row[0])
		
		topic_list = ""
		
		for i in range(0,len(topics),1):
			if i == 0:
				topic_list += "path like '" + topics[i] + "' "
			else:
				topic_list += "or path like '" + topics[i] + "' "
		
		sql = """select logdate,count(1),count(distinct(concat(remote_addr,'-',http_user_agent))) from logs.http_group_haodou_com where logdate between '%s' and '%s' and 
		(
		%s
		)
		group by logdate order by logdate desc; 
		"""
		cursor = self.shark.execute(sql%(startWeekDate,endWeekDate,topic_list))
		print "关于“国民家常菜”话题的 总PV UV数"
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 2:
				logdate = str(row[0])
				pv = str(row[1])
				uv = str(row[2])
				print logdate,',',pv,',',uv
				
				
		sql = "select to_date(c.createtime) createtime,count(c.topicid),count(distinct(c.userid)) from hd_haodou_center_" + self.shortDate + ".grouptopic c \
		where c.createtime between '" + startWeekDate + " 00:00:00' and '" + endWeekDate + " 23:59:59' and c.status in (1,2) \
		and c.title like '%国民家常菜%' \
		group by to_date(c.createtime) \
		order by createtime desc;"
		
		cursor = self.shark.execute(sql)
		print "标题包含“国民家常菜” 发帖数 发帖人数"
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 2:
				logdate = str(row[0])
				ct = str(row[1])
				pct = str(row[2])
				print logdate,',',ct,',',pct
		
		sql = "select to_date(a.createtime) createtime,count(distinct(a.commentId)),count(distinct(a.userid)) from hd_haodou_center_" + self.shortDate + ".grouptopic c \
		inner join hd_haodou_comment_" + self.shortDate + ".comment a on a.itemid=c.topicid \
		where c.createtime between '" + startWeekDate + " 00:00:00' and '" + endWeekDate + " 23:59:59' and c.status in (1,2) \
		and a.status=1 and a.type=6 and a.createtime between '" + startWeekDate + " 00:00:00' and '" + endWeekDate + " 23:59:59' \
		and c.title like '%国民家常菜%' \
		group by to_date(a.createtime) \
		order by createtime desc;"
		
		cursor = self.shark.execute(sql)
		print "标题包含“国民家常菜”发帖 回复数 回复人数"
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 2:
				logdate = str(row[0])
				ct = str(row[1])
				pct = str(row[2])
				print logdate,',',ct,',',pct
		
		#关于“国民家常菜”话题的分类PV UV
		
		sql = "select logdate,count(1),count(distinct(concat(remote_addr,'-',http_user_agent))) from logs.http_group_haodou_com where logdate between '" + startWeekDate + "' and '" + endWeekDate + "' and \
		( \
		" + topic_list + " \
		) \
		and referer like '%haodou.com%' \
		group by logdate order by logdate desc; "
		
		cursor = self.shark.execute(sql)
		print "关于“国民家常菜”话题的 网站端 PV UV数"
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 2:
				logdate = str(row[0])
				pv = str(row[1])
				uv = str(row[2])
				print logdate,',',pv,',',uv
				
		sql = "select logdate,count(1),count(distinct(concat(remote_addr,'-',http_user_agent))) from logs.http_group_haodou_com where logdate between '" + startWeekDate + "' and '" + endWeekDate + "' and \
		( \
		" + topic_list + " \
		) \
		and referer like '%t.qq.com%' \
		group by logdate order by logdate desc; "
		
		cursor = self.shark.execute(sql)
		print "关于“国民家常菜”话题的 腾讯微博 PV UV数"
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 2:
				logdate = str(row[0])
				pv = str(row[1])
				uv = str(row[2])
				print logdate,',',pv,',',uv
				
		sql = "select logdate,count(1),count(distinct(concat(remote_addr,'-',http_user_agent))) from logs.http_group_haodou_com where logdate between '" + startWeekDate + "' and '" + endWeekDate + "' and \
		( \
		" + topic_list + " \
		) \
		and referer like '%weibo%' \
		group by logdate order by logdate desc; "
		
		cursor = self.shark.execute(sql)
		print "关于“国民家常菜”话题的 新浪微博 PV UV数"
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 2:
				logdate = str(row[0])
				pv = str(row[1])
				uv = str(row[2])
				print logdate,',',pv,',',uv
				
		sql = "select logdate,count(1),count(distinct(concat(remote_addr,'-',http_user_agent))) from logs.http_group_haodou_com where logdate between '" + startWeekDate + "' and '" + endWeekDate + "' and \
		( \
		" + topic_list + " \
		) \
		and (referer like '%qzs.qq.com%' or referer like '%qzone%') \
		group by logdate order by logdate desc; "
		
		cursor = self.shark.execute(sql)
		print "关于“国民家常菜”话题的 QQ空间 PV UV数"
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 2:
				logdate = str(row[0])
				pv = str(row[1])
				uv = str(row[2])
				print logdate,',',pv,',',uv
		
		
		
		topics = []
		
		sql = "select concat('%view.php?id=',c.topicid,'%') from hd_haodou_center_" + self.shortDate + ".grouptopic c \
		where c.createtime between '" + startWeekDate + " 00:00:00' and '" + endWeekDate + " 23:59:59' and c.status in (1,2) and c.title like '%国民家常菜%'; "
		cursor = self.shark.execute(sql)
		for cols in cursor:
			row = re.split(r"\s+",cols)
			topics.append(row[0])
		
		topic_list = ""
		
		for i in range(0,len(topics),1):
			if i == 0:
				topic_list += "path like '" + topics[i] + "' "
			else:
				topic_list += "or path like '" + topics[i] + "' "
				
		sql = "select logdate,count(1),count(distinct(concat(remote_addr,'-',http_user_agent))) from logs.http_m_haodou_com where logdate between '" + startWeekDate + "' and '" + endWeekDate + "' and \
		( \
		" + topic_list + " \
		) \
		and referer like '%mp.weixin.qq.com%' \
		group by logdate order by logdate desc;"
		
		cursor = self.shark.execute(sql)
		print "关于“国民家常菜”话题的 微信 PV UV数"
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 2:
				logdate = str(row[0])
				pv = str(row[1])
				uv = str(row[2])
				print logdate,',',pv,',',uv
		
		sql = "select logdate,count(1),count(distinct(concat(remote_addr,'-',http_user_agent))) from logs.http_m_haodou_com where logdate between '" + startWeekDate + "' and '" + endWeekDate + "' and \
		( \
		" + topic_list + " \
		) \
		and referer not like '%mp.weixin.qq.com%' \
		group by logdate order by logdate desc;"
		
		cursor = self.shark.execute(sql)
		print "关于“国民家常菜”话题的 移动端 PV UV数"
		for cols in cursor:
			row = re.split(r"\s+",cols)
			if len(row) > 2:
				logdate = str(row[0])
				pv = str(row[1])
				uv = str(row[2])
				print logdate,',',pv,',',uv
		
		
				
		
if __name__ == '__main__':
	
	usage = "usage: %prog [options] arg1 arg2"
	parser = OptionParser(usage = usage, version="%prog 0.1")
	parser.add_option("-d", "--delay", help = 'Delay a number day', dest = "delay")
	(options, args) = parser.parse_args()

	delay = options.delay or 1
	
	delay = int(delay)
	
	rjccs = Recipe2014JiaChangCaiSummary(delay)
	rjccs.start()
	rjccs.run()
