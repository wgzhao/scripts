# -*- coding: UTF-8 -*- 
#coding = utf-8
#菜谱2014国民家常菜抽奖数据收集统计

from optparse import OptionParser
import commands
from time import ctime
from MailUtil import *
from XLSWriter import XLSWriter
from sharkDB import *
import traceback
from  datetime  import  *
import time
import sys
import os
import re

class Recipe2014JiaChangCaiAward:
	
	def __init__(self, fpath, delay=1):
		self.delay = delay
		self.shark = SharkDB()
		dt = datetime.now()
		self._datetime = dt - timedelta(days = delay)
		self.curDate = self._datetime.strftime("%Y-%m-%d")
		self.file_path = fpath + "/" + self._datetime.strftime("%Y%m") + "/"
		
		if os.path.isdir(self.file_path) is False:
			os.mkdir(self.file_path)
		
		self.shortDate = dt.strftime("%Y%m%d")
		
	def start(self):
		
		#写文件
		xlswriter = XLSWriter(self.file_path + "recipe2014jiachangcaisummary_" + self.curDate + ".xls")
		
		xlswriter.writerow(["日期","用户ID","用户昵称","奖品","豆窝地址"], sheet_name=u'所有中奖用户抽奖信息')
		
		
		sql = """\
		select ua.userid,if(u.username is null or u.username = 'null' or u.username = 'NULL','-',u.username),ca.name,concat('http://wo.haodou.com/',ua.userid,'/') from rcp_haodou_act_%s.2014jiachangcaiuseraward ua 
		inner join rcp_haodou_act_%s.2014jiachangcaiaward ca on ua.awardid=ca.awardid 
		left outer join hd_haodou_passport_%s.user u on ua.userid=u.userid 
		where ua.createtime between '%s 00:00:00' and '%s 23:59:59' and ua.userid<>0; 
		"""
		
		cursor = self.shark.execute(sql%(self.shortDate,self.shortDate,self.shortDate,self.curDate,self.curDate))
		for cols in cursor:
			row = re.split(r"\s+",cols)
			userid = str(row[0])
			username = str(row[1])
			award  = str(row[2])
			web  = str(row[3])
			
			xlswriter.writerow([self.curDate,userid,username,award,web], sheet_name=u'所有中奖用户抽奖信息')
		
		xlswriter.save()
		
		#统计中奖情况
		sql = """\
		select name,sum(ct) ct from(
		select ca.name,0 ct from rcp_haodou_act_%s.2014jiachangcaiaward ca
		union all
		select ca.name,count(ua.userid) ct from rcp_haodou_act_%s.2014jiachangcaiaward ca
		left outer join rcp_haodou_act_%s.2014jiachangcaiuseraward ua on ua.awardid=ca.awardid
		where ua.createtime between '%s 00:00:00' and '%s 23:59:59'
		group by ca.name
		) tmp
		group by name
		order by ct;
		"""
		
		tds = ""
		
		cursor = self.shark.execute(sql%(self.shortDate,self.shortDate,self.shortDate,self.curDate,self.curDate))
		for cols in cursor:
			row = re.split(r"\s+",cols)
			award = str(row[0])
			count = str(row[1])
			tds += "<tr>\n"
			tds += "<td align='center' valign='middle' style='border: 1px solid #C7D8EA;line-height: 16px; font-size: 14px;'>" + award + "</td>\n"
			tds += "<td align='center' valign='middle' style='border: 1px solid #C7D8EA;line-height: 16px; font-size: 14px;'>" + count + "</td>\n"
			tds += "</tr>"
			
		html = """\
		<html>
				<head></head>
				<body>
				<table cellspacing='0' cellpadding='0' width='400px' style='border: 1px solid #C7D8EA;'>
				<tr>
					<td colspan='2' align='center' style='border: 1px solid #C7D8EA;background: rgb(14,86,173); color: white;line-height: 20px;'><b>国民家常菜%s抽奖基本情况</b></td>
				</tr>
				<tr>
					<td style='border: 1px solid #C7D8EA;'><p style='color:#000099;' align='center'>奖品类型</p></td>
					<td style='border: 1px solid #C7D8EA;'><p style='color:#000099;' align='center'>中奖人数</p></td>
				</tr>
				%s
				</table>
				</body>
		</html>
		"""
		
		
		ms = MailSender(self.file_path + "recipe2014jiachangcaisummary_" + self.curDate + ".xls", ("国民家常菜%s抽奖信息"%self.curDate) + ".xls")
		ms.sendMail("jiangmengdie@haodou.com", "国民家常菜%s抽奖信息"%self.curDate, html%(self.curDate,tds))
		ms.sendMail("tanglin@haodou.com", "国民家常菜%s抽奖信息"%self.curDate, html%(self.curDate,tds))
		ms.sendMail("zhaoweiguo@haodou.com", "国民家常菜%s抽奖信息"%self.curDate, html%(self.curDate,tds))
	
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
	
	rjcca = Recipe2014JiaChangCaiAward(path, delay)
	rjcca.start()
