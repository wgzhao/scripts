# -*- coding: UTF-8 -*- 
#coding = utf-8
#小组手机端/网站端相关数据需求（周为单位）

from optparse import OptionParser
import commands
from time import ctime
from MailUtil import *
from XLSWriter import XLSWriter
from sparkDB import *
from hiveDB import *
from timeUtils import *
import traceback
from  datetime  import  *
import time
import sys
import os
import re

do_food_group = '(31,32,33,6,8)'
love_life_group = '(34,35,23,29,28,30,38)'

#菜谱小组移动端数据统计
class RecipeGroupMobileWebDataSummary:

	def __init__(self, fpath, delay=1, testmodel=False):
		self.delay = delay
		dt = datetime.now()
		#设置根据delay时间倒减一天
		#方便取过去时间的数据测试
		self.nowDate = (dt - timedelta(days = self.delay - 1)).strftime("%Y%m%d")
		
		self._datetime = dt - timedelta(days = delay)
		self.curDate = self._datetime.strftime("%Y-%m-%d")
		self.tu = TimeUtils()
		
		self.testModel = testmodel
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
				
		self.hive = HiveDB("--define curdate=%s"%self.curDate)
		#self.spark = SparkDB()
		
		self.file_path = fpath + "/" + self._datetime.strftime("%Y%m") + "/"
		
		if os.path.isdir(self.file_path) is False:
			os.mkdir(self.file_path)
	
	#小组手机端和网站端周度数据需要进行导出并定期进行邮件发放
	def start(self):
		
		t = time.mktime(self._datetime.timetuple())
		
		#算本月一号
		monthFirstDay = time.strftime('%Y-%m-%d',time.localtime(self.tu.get_month_begin(t)))
			
		#算本周一		
		weekFirstDay = time.strftime('%Y-%m-%d',time.localtime(self.tu.get_week_begin(t)))
		if monthFirstDay == self.curDate or weekFirstDay == self.curDate or self.testModel==True:
			
			print "判断当前时间是否在每月一号"
			print "判断当前时间是否在每周一"
			if monthFirstDay == self.curDate:
				print "当前时间%s是在本月第一天"%self.curDate
			if weekFirstDay == self.curDate or self.testModel==True:
				#判断如果还未到自然周第一日则不做计算
				print "当前时间%s是在本周一"%self.curDate
			
			sql = """\
            set hive.support.sql11.reserved.keywords=false;
			select username,userid,sum(feeds+favorites),sum(feeds),sum(favorites),sum(mreplay+wrepaly),sum(mreplay),sum(wrepaly),sum(foods+lives),sum(foods),sum(lives),sum(recommend),sum(digest),sum(lovelifedigest),sum(foodsdigest),sum(loveliferecommend),sum(foodsrecommend) from(
			select u.username,f.userid,count(f.feedid) feeds, 0 as favorites, 0 as mreplay, 0 as wrepaly, 0 as foods, 0 as lives, 0 as recommend, 0 as digest, 0 as lovelifedigest, 0 as foodsdigest, 0 as loveliferecommend, 0 as foodsrecommend 
			from haodou_center_{date}.userfeed f
			left outer join haodou_passport_{date}.user u on f.userid=u.userid
			where to_date(f.createtime) between '{mind}' and '{maxd}'
			and f.channel in(3,4,5) and f.type in (110,111,112,113,303,403)
			group by u.username,f.userid
			union all
			select u.username,g.userid,0 as feeds, sum(g.favoritecount) favorites, 0 as mreplay, 0 as wrepaly, 0 as foods, 0 as lives, 0 as recommend, 0 as digest, 0 as lovelifedigest, 0 as foodsdigest, 0 as loveliferecommend, 0 as foodsrecommend 
			from haodou_center_{date}.grouptopic g
			left outer join haodou_passport_{date}.user u on g.userid=u.userid
			where to_date(g.createtime) between '{mind}' and '{maxd}'
			and g.status=1
			group by u.username,g.userid
			union all
			select u.username,c.userid,0 as feeds, 0 as favorites, count(distinct(c.itemid)) mreplay, 0 as wrepaly, 0 as foods, 0 as lives, 0 as recommend, 0 as digest, 0 as lovelifedigest, 0 as foodsdigest, 0 as loveliferecommend, 0 as foodsrecommend 
			from haodou_comment_{date}.comment c
			left outer join haodou_passport_{date}.user u on c.userid=u.userid
			where to_date(c.createtime) between '{mind}' and '{maxd}'
			and c.status=1 and c.type=6 and length(html2text(c.content))>=3 and c.platform in(1,2,3)
			group by u.username,c.userid
			union all
			select u.username,c.userid,0 as feeds, 0 as favorites, 0 as mreplay, count(distinct(c.itemid)) wrepaly, 0 as foods, 0 as lives, 0 as recommend, 0 as digest, 0 as lovelifedigest, 0 as foodsdigest, 0 as loveliferecommend, 0 as foodsrecommend 
			from haodou_comment_{date}.comment c
			left outer join haodou_passport_{date}.user u on c.userid=u.userid
			where to_date(c.createtime) between '{mind}' and '{maxd}'
			and c.status=1 and c.type=6 and length(html2text(c.content))>=10 and c.platform =0
			group by u.username,c.userid
			union all
			select u.username,g.userid, 0 as feeds, 0 as favorites, 0 as mreplay,0 as wrepaly, if(g.cateid in %(do_food_group)s, count(g.topicid), 0) foods, if(g.cateid in %(love_life_group)s, count(g.topicid), 0) lives, 0 as recommend, 0 as digest, 0 as lovelifedigest, 0 as foodsdigest, 0 as loveliferecommend, 0 as foodsrecommend 
			from haodou_center_{date}.grouptopic g
			left outer join haodou_passport_{date}.user u on g.userid=u.userid
			where to_date(g.createtime) between '{mind}' and '{maxd}' and g.status=1
			group by u.username,g.userid,g.cateid
			union all
			select u.username,g.userid, 0 as feeds, 0 as favorites, 0 as mreplay,0 as wrepaly, 0 as foods, 0 as lives, if(g.recommend=1, count(g.topicid), 0) as recommend, if(g.digest=1, count(g.topicid), 0) as digest, 0 as lovelifedigest, 0 as foodsdigest, 0 as loveliferecommend, 0 as foodsrecommend 
			from haodou_center_{date}.grouptopic g
			left outer join haodou_passport_{date}.user u on g.userid=u.userid
			where g.status=1
			and to_date(g.createtime) between '{mind}' and '{maxd}'
			group by u.username,g.userid,g.recommend,g.digest
			union all
			select u.username,g.userid, 0 as feeds, 0 as favorites, 0 as mreplay,0 as wrepaly, 0 as foods, 0 as lives, 0 as recommend, 0 as digest, if(g.digest=1 and g.cateid in %(love_life_group)s,count(g.topicid),0) lovelifedigest, if(g.digest=1 and g.cateid in %(do_food_group)s,count(g.topicid),0) foodsdigest, if(g.recommend=1 and g.cateid in %(love_life_group)s,count(g.topicid),0) loveliferecommend, if(g.recommend=1 and g.cateid in %(do_food_group)s,count(g.topicid),0) foodsrecommend 
			from haodou_center_{date}.grouptopic g
			left outer join haodou_passport_{date}.user u on g.userid=u.userid
			where g.status=1 and to_date(g.createtime) between '{mind}' and '{maxd}'
			group by u.username,g.userid,g.digest,g.cateid,g.recommend
			) temp
			group by username,userid;
			""" % {"do_food_group": do_food_group, "love_life_group": love_life_group}
			#having (foods > 0 or lives > 0 or recommend > 0 or digest > 0 or lovelifedigest > 0 or foodsdigest > 0 or loveliferecommend > 0 or foodsrecommend > 0)
			
			cursor = None
			
			#写文件
			xlswriter = XLSWriter(self.file_path+"recipegroupmobilewebdatasummary_"+self.curDate+".xls")
			
			if monthFirstDay == self.curDate:
				cursql = sql.replace("{date}", self.nowDate);
				cursql = cursql.replace("{mind}", self.startMonthDate);
				cursql = cursql.replace("{maxd}", self.endMonthDate);
				cursor = self.hive.execute(cursql)
				print "%s %s 小组手机端网站端数据月度需求"%(self.startMonthDate,self.endMonthDate)
				
				xlswriter.writerow(["用户名","用户ID","收藏总次数","豆圈","收藏话题次数","总回复话题数","手机端回复话题数","网站端回复话题数","发布话题","做美食","爱生活","推荐","精华","爱生活精华话题数","做美食精华话题数","爱生活被推荐话题数","做美食被推荐话题数","第一次发布时间","最近发布话题时间"], sheet_name=u'月度数据详情')
				
				
				userids = ""
				
				for cols in cursor:
					row = re.split(r"\s+",cols)
					userid = str(row[1])
					if "" != userid:
						userids += userid + ","
				
				if "" != userids:
					userids = userids[0:len(userids)-1]	
				
				self.findMinMaxTime(userids)		
				
				for cols in cursor:
					row = re.split(r"\s+",cols)
					username = str(row[0])
					userid = int(row[1])
					fcnt = int(row[2])
					feeds = int(row[3])
					ftcnt = int(row[4])
					trcnt = int(row[5])
					mrcnt = int(row[6])
					wrcnt = int(row[7])
					tcnt = int(row[8])
					foods = int(row[9])
					lives = int(row[10])
					rcd = int(row[11])
					digt = int(row[12])
					ashjh = int(row[13])
					zmsjh = int(row[14])
					ashtj = int(row[15])
					zmstj = int(row[16])
					mindate = ""
					maxdate = ""
					if userid in self.usersTime:
						mindate = self.usersTime[userid]["min"]
						maxdate = self.usersTime[userid]["max"]
					
					xlswriter.writerow([username,userid,fcnt,feeds,ftcnt,trcnt,mrcnt,wrcnt,tcnt,foods,lives,rcd,digt,ashjh,zmsjh,ashtj,zmstj,mindate,maxdate], sheet_name=u'月度数据详情')
					
					
					
			if weekFirstDay == self.curDate or self.testModel==True:
				cursql = sql.replace("{date}", self.nowDate);
				cursql = cursql.replace("{mind}", self.startWeekDate);
				cursql = cursql.replace("{maxd}", self.endWeekDate);
				cursor = self.hive.execute(cursql)
				print "%s %s 小组手机端网站端数据周度需,"%(self.startWeekDate,self.endWeekDate)
				
				xlswriter.writerow(["用户名","用户ID","收藏总次数","豆圈","收藏话题次数","总回复话题数","手机端回复话题数","网站端回复话题数","发布话题","做美食","爱生活","推荐","精华","爱生活精华话题数","做美食精华话题数","爱生活被推荐话题数","做美食被推荐话题数","第一次发布时间","最近发布话题时间"], sheet_name=u'周度数据详情')
			
				userids = ""
				
				for cols in cursor:
					row = re.split(r"\s+",cols)
					userid = str(row[1])
					if "" != userid:
						userids += userid + ","
				
				if "" != userids:
					userids = userids[0:len(userids)-1]	
				
				self.findMinMaxTime(userids)		
				
				for cols in cursor:
					row = re.split(r"\s+",cols)
					username = str(row[0])
					userid = int(row[1])
					fcnt = int(row[2])
					feeds = int(row[3])
					ftcnt = int(row[4])
					trcnt = int(row[5])
					mrcnt = int(row[6])
					wrcnt = int(row[7])
					tcnt = int(row[8])
					foods = int(row[9])
					lives = int(row[10])
					rcd = int(row[11])
					digt = int(row[12])
					ashjh = int(row[13])
					zmsjh = int(row[14])
					ashtj = int(row[15])
					zmstj = int(row[16])
					mindate = ""
					maxdate = ""
					if userid in self.usersTime:
						mindate = self.usersTime[userid]["min"]
						maxdate = self.usersTime[userid]["max"]
					
					xlswriter.writerow([username,userid,fcnt,feeds,ftcnt,trcnt,mrcnt,wrcnt,tcnt,foods,lives,rcd,digt,ashjh,zmsjh,ashtj,zmstj,mindate,maxdate], sheet_name=u'周度数据详情')
			
			
			xlswriter.save()
			
			#os.popen("echo \"email body \" |mail -a " + self.file_path+"recipegroupmobilewebdatasummary_"+self.curDate+".xls" + " -s \"subject\" zhaoweiguo@haodou.com")
			
			if monthFirstDay == self.curDate:
				fn = "%s %s 小组手机端网站端数据月度需求"%(self.startMonthDate,self.endMonthDate)
				ms = MailSender(self.file_path+"recipegroupmobilewebdatasummary_"+self.curDate+".xls", fn + ".xls")
				ms.sendMail("zhaoweiguo@haodou.com", "%s %s 小组手机端/网站端相关数据需求"%(self.startMonthDate,self.endMonthDate), "月度数据详情")
				ms.sendMail("lifangxing@haodou.com", "%s %s 小组手机端/网站端相关数据需求"%(self.startMonthDate,self.endMonthDate), "月度数据详情")
				
				if self.testModel == False:
					ms.sendMail("mohonghua@haodou.com", "%s %s 小组手机端/网站端相关数据需求"%(self.startMonthDate,self.endMonthDate), "月度数据详情")
					
			if weekFirstDay == self.curDate or self.testModel==True:
				fn = "%s %s 小组手机端网站端数据周度需求"%(self.startWeekDate,self.endWeekDate)
				ms = MailSender(self.file_path+"recipegroupmobilewebdatasummary_"+self.curDate+".xls", fn + ".xls")
				ms.sendMail("zhaoweiguo@haodou.com", "%s %s 小组手机端/网站端相关数据需求"%(self.startWeekDate,self.endWeekDate), "周度数据详情")
				
				if self.testModel == False:
					ms.sendMail("mohonghua@haodou.com", "%s %s 小组手机端/网站端相关数据需求"%(self.startWeekDate,self.endWeekDate), "周度数据详情")
		
		else:
			print "当前时间 %s 未到周一"%self.curDate
			print "执行其他任务"
			
			#self.dailyGroupTopicDataSummary()
			
			
	def findMinMaxTime(self, usersid):
		
		sql = """\
		select userid,',',min(createtime),',',max(createtime) 
		from haodou_center_%s.grouptopic 
		where userid in (%s)
		group by userid;
		"""
		
		cursor = self.hive.execute(sql%(self.nowDate,usersid))
		for cols in cursor:
			row = re.split(",",cols)
			userid = int(row[0].strip())
			mindate = str(row[1].strip())
			maxdate = str(row[2].strip())
			
			if userid not in self.usersTime:
				self.usersTime[userid] = {}
			
				self.usersTime[userid]["min"] = mindate	
				self.usersTime[userid]["max"] = maxdate
			
	
		
if __name__ == '__main__':
	
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
	
	path  = os.path.dirname(os.path.abspath(__file__))
	
	obj = RecipeGroupMobileWebDataSummary(path, delay, options.test)
	obj.start()
	#obj.eightFloor()
