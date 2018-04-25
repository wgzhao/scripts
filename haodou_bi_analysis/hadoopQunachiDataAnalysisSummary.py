# -*- coding: UTF-8 -*- 
#coding = utf-8
#去哪吃数据分析

from optparse import OptionParser
from XLSWriter import XLSWriter
from timeUtils import *
from datetime import *
from MailUtil import *
from hiveDB import *
from sparkDB import *
from time import ctime
import commands
import MySQLdb
import traceback
import time
import sys
import os
import re

class DB:
	conn = None
	cursor = None
	
	def connect(self):
		self.conn = MySQLdb.connect (host='10.1.1.70',port=3308, user='bi',passwd='bi_haodou',db='haodou_pai',charset="utf8")
	
	
	def execute(self, sql):
		try:
			cursor = self.conn.cursor()
			cursor.execute(sql)
		except (AttributeError, MySQLdb.OperationalError):
			self.connect()
			cursor = self.conn.cursor()
			cursor.execute(sql)
		return cursor
	
class QunachiDataAnalysisSummary:
	
	def __init__(self, fpath, delay=1, testmodel=False):
		self.delay = delay
		dt = datetime.now()
		self.testModel = testmodel
		
		#设置根据delay时间倒减一天
		#方便取过去时间的数据测试
		self.nowDate = (dt - timedelta(days = self.delay - 1)).strftime("%Y%m%d")
		self._datetime = dt - timedelta(days = delay)
		self.curDate = self._datetime.strftime("%Y-%m-%d")
		self.file_path = fpath + "/" + self._datetime.strftime("%Y%m") + "/"
		self.tu = TimeUtils()
		self.db = DB()
		
		if os.path.isdir(self.file_path) is False:
			os.mkdir(self.file_path)
		
		t = time.mktime(self._datetime.timetuple())
		#算上月一号
		self.startMonthDate = time.strftime('%Y-%m-%d',time.localtime(self.tu.get_month_begin(t,-1)))
		#算上月末
		self.endMonthDate = (datetime.fromtimestamp(self.tu.get_month_begin(t)) - timedelta(days = 1)).strftime('%Y-%m-%d')
		
		self.testtime = "21"
		
		#时间到了21号（实际是22号）自动统计当月1号到20号的数据
		if self._datetime.strftime("%d") == self.testtime:
		
			self.startMonthDate = self._datetime.strftime("%Y-%m") + "-01"
		
			self.endMonthDate = self._datetime.strftime("%Y-%m") + "-20"
			
		#"--define date=%s --define mindate=%s --define maxdate=%s"%(self.nowDate,self.startMonthDate,self.endMonthDate)
		#self.spark = SparkDB()
		self.hive = HiveDB()
	
	def start(self):
			
		#美食地主数据统计
		self.foodMaster()
			
		#部分用户每月操作数据导出
		self.userAnalysis()
	
	def userAnalysis(self):
		
		t = time.mktime(self._datetime.timetuple())

		nowMonthDate = time.strftime('%Y-%m-%d',time.localtime(self.tu.get_month_begin(t)))
		if nowMonthDate == self.curDate or self.testModel==True:
			print "判断当前时间是否在每月一号"
			print "当前时间%s是在本月第一天"%self.curDate
			
			sql = """ \
			select userid,username,sum(ct) as ct,sum(ct1),sum(ct2) from(
			select st.userid,u.username,count(distinct(st.topicid)) as ct,0 as ct1, 0 as ct2
			from qnc_haodou_pai_""" + self.nowDate + """.shoptopic st
			left outer join hd_haodou_passport_""" + self.nowDate + """.user u on st.userid=u.userid
			where st.userid in (674318,2171713,2301227,29485) 
			and to_date(st.createtime) between '""" + self.startMonthDate + """' and '""" + self.endMonthDate + """' and st.status=1
			group by st.userid,u.username
			union all
			select c.userid,u.username,0 as ct,count(distinct(if(st.type=1,c.itemid,0))) as ct1 ,count(distinct(if(st.type=2,c.itemid,0))) as ct2
			from qnc_haodou_pai_""" + self.nowDate + """.shoptopic st
			inner join qnc_haodou_comment_""" + self.nowDate + """.comment c on c.itemid=st.topicid
			left outer join hd_haodou_passport_""" + self.nowDate + """.user u on c.userid=u.userid
			where c.userid in (674318,2171713,2301227,29485)
			and c.type=11 and c.replyid=0 and c.status=1 and st.status=1
			and to_date(c.createtime) between '""" + self.startMonthDate + """' and '""" + self.endMonthDate + """'
			and to_date(st.createtime) between '""" + self.startMonthDate + """' and '""" + self.endMonthDate + """'
			group by c.userid,u.username
			) tmp
			group by userid,username
			order by ct desc;		
			"""
			
			
			sql = """ \
			SELECT userid,
			CASE 
			WHEN userid=674318 THEN 'wlpxsj'
			WHEN userid=2171713 THEN '夏雪飞扬'
			WHEN userid=29485 THEN '小包子'
			WHEN userid=2301227 THEN 'floraoy'
			END AS username
			,SUM(ct) AS ct,SUM(ct1),SUM(ct2) FROM(
			SELECT st.userid,COUNT(DISTINCT(st.topicid)) AS ct,0 AS ct1, 0 AS ct2
			FROM haodou_pai.ShopTopic st
			WHERE st.userid IN (674318,2171713,2301227,29485) 
			AND DATE(st.createtime) BETWEEN '""" + self.startMonthDate + """' AND '""" + self.endMonthDate + """' AND st.status=1
			GROUP BY st.userid
			UNION ALL
			SELECT c.userid,0 AS ct,COUNT(DISTINCT(IF(st.type=1,c.itemid,0))) AS ct1 ,COUNT(DISTINCT(IF(st.type=2,c.itemid,0))) AS ct2
			FROM haodou_pai.ShopTopic st
			INNER JOIN haodou_comment.Comment c ON c.itemid=st.topicid
			WHERE c.userid IN (674318,2171713,2301227,29485)
			AND c.type=11 AND c.replyid=0 AND c.status=1 AND st.status=1
			AND DATE(c.createtime) BETWEEN '""" + self.startMonthDate + """' AND '""" + self.endMonthDate + """'
			AND DATE(st.createtime) BETWEEN '""" + self.startMonthDate + """' AND '""" + self.endMonthDate + """'
			GROUP BY c.userid
			) tmp
			GROUP BY userid,username
			ORDER BY ct DESC
			"""
			print sql
			
			
			month = time.strftime('%m',time.localtime(self.tu.get_month_begin(t)))	
			print "生成去哪吃部分用户%s月份操作数据"%(month)
			
			#写文件
			xlswriter = XLSWriter(self.file_path+"qunachispecialdataanalysis_"+self.curDate+".xls")
			xlswriter.writerow(["用户ID","用户昵称","发帖数","回复数"], sheet_name=u'数据详情')
			xlswriter.writerow(["","","","走街寻店","特色小吃"], sheet_name=u'数据详情')
			
			cursor = self.db.execute(sql)
			
			for row in cursor:
				userid = str(row[0])
				username = row[1]
				topic = int(row[2])
				street = int(row[3])
				foods = int(row[4])
	
				xlswriter.writerow([userid,username,topic,street,foods], sheet_name=u'数据详情')
			
			xlswriter.save()				
			
			_title = "去哪吃部分用户%s月份操作数据"%(month)
				
			ms = MailSender(self.file_path+"qunachispecialdataanalysis_"+self.curDate+".xls", _title + ".xls")
			ms.sendMail("zhaoweiguo@haodou.com", _title, "数据详情见附件")
			if self.testModel==False:
				ms.sendMail("zhouguo@haodou.com", _title, "数据详情见附件")
			
	def foodMaster(self):
		
		t = time.mktime(self._datetime.timetuple())

		nowMonthDate = time.strftime('%Y-%m-%d',time.localtime(self.tu.get_month_begin(t)))
		if nowMonthDate == self.curDate or self._datetime.strftime("%d") == self.testtime or self.testModel==True:
			if self._datetime.strftime("%d") == self.testtime:
				#时间到了21号（实际是22号）自动统计当月1号到20号的数据
				print "判断当前日期是否在本月21号"
				print "当前时间%s是在本月21号"%self.curDate
				
			if nowMonthDate == self.curDate or self.testModel==True:
				print "判断当前时间是否在每月一号"
				print "当前时间%s是在本月第一天"%self.curDate
			
			sql = """\
			select vip,userid,username,sum(ct) as ct,sum(lever2),sum(lever3),sum(lever4),sum(lever5) 
			from (
			select gfmt(uv.vip) as vip,uv.userid as userid,u.username as username,count(distinct(ps.shareid)) as ct,0 as lever2,0 as lever3,0 as lever4,0 as lever5
			from qnc_haodou_center_""" + self.nowDate + """.uservipinfo uv 
			inner join hd_haodou_passport_""" + self.nowDate + """.user u on uv.userid=u.userid
			inner join qnc_haodou_pai_""" + self.nowDate + """.paishare ps on ps.userid=uv.userid
			where ps.status=1 and to_date(ps.createtime) between '""" + self.startMonthDate + """' and '""" + self.endMonthDate + """'
			and (gfmt(uv.vip) like '美食地主V%' or gfmt(uv.vip) = '实习美食地主') and ps.status=1
			group by gfmt(uv.vip),uv.userid,u.username having vip is not null
			union all
			select gfmt(uv.vip) as vip,uv.userid as userid,u.username as username,0 as ct, sum(if(st.rate=2,1,0)) as lever2,sum(if(st.rate=3,1,0)) as lever3,sum(if(st.rate=4,1,0)) as lever4,sum(if(st.rate=5,1,0)) as lever5
			from qnc_haodou_center_""" + self.nowDate + """.uservipinfo uv 
			inner join qnc_haodou_pai_""" + self.nowDate + """.shoptopic st on st.userid=uv.userid
			inner join hd_haodou_passport_""" + self.nowDate + """.user u on uv.userid=u.userid
			where to_date(st.createtime) between '""" + self.startMonthDate + """' and '""" + self.endMonthDate + """'
			and (gfmt(uv.vip) like '美食地主V%' or gfmt(uv.vip) = '实习美食地主') and st.type=1 and st.status=1
			group by gfmt(uv.vip),uv.userid,u.username having vip is not null
			) tmp
			group by vip,userid,username
			order by ct desc;
			"""
			
			
			if self._datetime.strftime("%d") == self.testtime:
				print "生成去哪吃美食地主%s号到%s号考核数据"%(self.startMonthDate,self.endMonthDate)
				
			month = time.strftime('%m',time.localtime(self.tu.get_month_begin(t)))	
			if nowMonthDate == self.curDate or self.testModel==True:				
				print "生成去哪吃美食地主%s月份考核数据"%(month)
			
			
			#写文件
			xlswriter = XLSWriter(self.file_path+"qunachidataanalysissummary_"+self.curDate+".xls")
			xlswriter.writerow(["地主级别","用户ID","用户名","美食发布数","走街巡店二星级以上数","走街巡店三星级以上数","走街巡店四星级以上数","走街巡店五星级以上数"], sheet_name=u'数据详情')
			
			cursor = self.hive.execute(sql)
			for cols in cursor:
				row = re.split(r"\s+",cols)
				vip = str(row[0])
				userid = str(row[1])
				username = str(row[2])
				ct = int(row[3])
				lever2 = int(row[4])
				lever3 = int(row[5])
				lever4 = int(row[6])
				lever5 = int(row[7])
	
				xlswriter.writerow([vip,userid,username,ct,lever2,lever3,lever4,lever5], sheet_name=u'数据详情')
			
			xlswriter.save()				
			
			_title = ""
			if self._datetime.strftime("%d") == self.testtime:
				_title = "去哪吃美食地主%s号到%s号考核数据"%(self.startMonthDate,self.endMonthDate)
				
			if nowMonthDate == self.curDate or self.testModel==True:
				_title = "去哪吃美食地主%s月份考核数据"%(month)
				
			ms = MailSender(self.file_path+"qunachidataanalysissummary_"+self.curDate+".xls", _title + ".xls")
			ms.sendMail("zhaoweiguo@haodou.com", _title, "数据详情见附件")
			if self.testModel==False:
				ms.sendMail("liuxiao@haodou.com", _title, "数据详情见附件")
			
			
if __name__ == '__main__':
	
	path  = os.path.dirname(os.path.abspath(__file__))
	
	usage = "usage: %prog [options] arg1 arg2"
	parser = OptionParser(usage = usage, version="%prog 0.1")
	parser.add_option("-d", "--delay", help = 'Delay a number day', dest = "delay")
	parser.add_option("-t", "--test", help = 'Open to the test mode', action="store_true", default=False, dest="test")
	(options, args) = parser.parse_args()
	
	if len(sys.argv)<=1:
		parser.print_help()
		sys.exit(2)
	
	delay = options.delay or 1
	
	delay = int(delay)
	
	rmpds = QunachiDataAnalysisSummary(path, delay, options.test)
	rmpds.start()
