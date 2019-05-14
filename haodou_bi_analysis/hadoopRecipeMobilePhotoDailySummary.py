# -*- coding: UTF-8 -*- 
#coding = utf-8
#每天导一份，直至8月1日
#手机客户端发布早餐、午餐、晚餐、夜宵四个标签的作品数

from optparse import OptionParser
import commands
from time import ctime
from MailUtil import *
from XLSWriter import XLSWriter
from hiveDB import *
import traceback
from  datetime  import  *
import time
import sys
import os
import re

class RecipeMobilePhotoDailySummary:
	
	def __init__(self, fpath, delay=1):
		self.delay = delay
		self.hive = HiveDB()
		dt = datetime.now()
		self._datetime = dt - timedelta(days = delay)
		self.curDate = self._datetime.strftime("%Y-%m-%d")
		self.file_path = fpath + "/" + self._datetime.strftime("%Y%m") + "/"
		
		if os.path.isdir(self.file_path) is False:
			os.mkdir(self.file_path)
		
		self.shortDate = dt.strftime("%Y%m%d")
		
	def start(self):
		
		foodtime = {'1':'早餐','2':'中餐','3':'晚餐','10':'夜宵'}
		
		#写文件
		xlswriter = XLSWriter(self.file_path + "recipemobilephotodailysummary_" + self.curDate + ".xls")
		
		xlswriter.writerow(["日期","类别","数量"], sheet_name=u'数据详情')
		
		sql = """\
		select to_date(p.createtime) createtime, p.topicid,count(p.id) from hd_haodou_photo_%s.photo p
		where to_date(p.createtime) = '%s'
		and p.\`From\` in (1,2,3)
		and p.topicid in (1,2,3,10) 
		group by to_date(p.createtime),p.topicid
		order by to_date(createtime);
		"""
		cursor = self.hive.execute(sql%(self.shortDate,self.curDate))
		
		
		for cols in cursor:
			row = re.split(r"\s+",cols)
			_time = row[0]
			_type = foodtime[row[1]]
			_count  = row[2]
			
			xlswriter.writerow([_time,_type,_count], sheet_name=u'数据详情')
		
		xlswriter.save()
		
		ms = MailSender(self.file_path + "recipemobilephotodailysummary_" + self.curDate + ".xls", ("%s手机客户端发布早餐、午餐、晚餐、夜宵四个标签的作品数"%self.curDate) + ".xls")
		#ms.sendMail("tanglin@haodou.com", "%s手机客户端发布四个标签的作品数"%self.curDate, "数据产生时间%s"%datetime.now().strftime("%Y-%m-%d"))
		ms.sendMail("zhaoweiguo@haodou.com", "%s手机客户端发布四个标签的作品数"%self.curDate, "数据产生时间%s"%datetime.now().strftime("%Y-%m-%d"))
		
if __name__ == '__main__':
	
	path  = os.path.dirname(os.path.abspath(__file__))
	
	usage = "usage: %prog [options] arg1 arg2"
	parser = OptionParser(usage = usage, version="%prog 0.1")
	parser.add_option("-d", "--delay", help = 'Delay a number day', dest = "delay")
	(options, args) = parser.parse_args()
	
	if len(sys.argv)<=1:
		parser.print_help()
		sys.exit(2)
	
	delay = options.delay or 1
	
	delay = int(delay)
	
	rmpds = RecipeMobilePhotoDailySummary(path,delay)
	rmpds.start()
