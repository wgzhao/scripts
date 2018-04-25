# -*- coding: UTF-8 -*-
#coding = utf-8

#每日新增applog日志入库
#每日新增统计表的分区

from optparse import OptionParser
from datetime import *
from hiveDB import *
from hashlib import md5
from MailUtil import *
import traceback
import  time
import commands
import os
import sys
import re



def start(path, today,notd=False,close=False,applog=True,resp=True):

	curDate = (nowday - timedelta(days = today)).strftime("%Y-%m-%d")
	_curDate = (nowday - timedelta(days = today)).strftime("%Y%m%d")

	while checkMD5Exists(path):
		print "MD5文件未生成，继续等待"
		time.sleep(60)

	print "当前新增日志时间是：" + curDate

	if notd == False:

		if close == False:

			downloadApplog(applog, resp, _curDate, '-q')

		else:
			downloadApplog(applog, resp, _curDate)

		moveToHDFS(curDate, _curDate)
		
		msg = checkData(path)
		errorMail.sendMail("zhaoweiguo@haodou.com", curDate+"日志实时入库监控", msg)
		errorMail.sendMail("zhaoweiguo@haodou.com", curDate+"日志实时入库监控", msg)

	#os.popen('hdfs dfs -chmod -R 777 /user/yarn/logs/*')

	

	
	#对所有入库的日志文件每天做一次检查
	#如果出现入库异常则发邮件通知

	#在每天1点开始
	#print "在每天8点开始"
	#nowTime = datetime.now()
	#secDeliy = datetime(nowTime.year,nowTime.month,nowTime.day,8,0,0) - nowTime

	#time.sleep(secDeliy.seconds)

def calMD5ForFile(file):
  #m = md5()
  #a_file = open(file, 'rb')
  #m.update(a_file.read())
  #a_file.close()
  #return m.hexdigest()
  
  status, ret = commands.getstatusoutput("md5sum " + file)
  data = re.split(r"\s+",ret.strip())
  return str(data[0])
  
  
def checkMD5Exists(path):
	os.popen("rm -f " + path + "/md5.txt")
	
	os.popen('wget -q -P '+path+' http://download.hoto.cn/Mobile_Bi/md5.txt')
	
	if os.path.exists(path + "/md5.txt"):
		return False
	else:
		return True	
	

def checkData(path):
	
	wst = "日志入库"
	
	for line in open(path + "/md5.txt"):
		print line,
		data = re.split(r"\s+",line.strip())
		value = data[0]
		data[1] = data[1].replace("/data/www/download_web/Mobile_Bi/","")
		#将来自服务器的MD5值临时先存起来
		md5data[value] = data[1]
		fileName = path+'/'+data[1]
		print fileName
		md5Value = calMD5ForFile(fileName)
		if md5Value == value:
			wst += "<font color=\'blue\'>" + str(data[1]) + " - 正常</font><br/>\n"
		else:
			wst += "<font color=\'red\'>" + str(data[1]) + " 经校验，此文件下载失败,\n原始文件MD5值:" + value+"\n"
			wst += "下载文件的MD5值:"+md5Value+"\n"
			wst += "请联系运维解决此问题。"
			wst += "</font><br/>\n"	
	
	return wst

def downloadApplog(applog, resp, _curDate, _close=''):
	if applog == True:
		print '开始下载applog...'

		#os.popen('wget ' + _close + ' -P '+path+' http://download.hoto.cn/Mobile_Bi/'+prefix+_curDate+suffix)

		os.popen('axel -n 10 -s 3145728  ' + _close + ' -o '+path+' http://download.hoto.cn/Mobile_Bi/'+prefix+_curDate+suffix + ' 2>> ' + path + '/dailyUpdateLog.log')

		os.popen("axel -n 10 -s 3145728  " + _close + " -o "+path+" http://download.hoto.cn/tomcat_Log/tomcat206_"+_curDate+".tar.lzo 2>> " + path + '/dailyUpdateLog.log')
		os.popen("axel -n 10 -s 3145728  " + _close + " -o "+path+" http://download.hoto.cn/tomcat_Log/tomcat207_"+_curDate+".tar.lzo 2>> " + path + '/dailyUpdateLog.log')

		print 'applog下载结束'

	if resp == True:
		print '开始下载app response log...'

		"""
		os.popen('wget ' + _close + ' -P '+path+' http://download.hoto.cn/Mobile_Bi/'+prefix+_curDate+'002_40.tar.lzo')
		os.popen('wget ' + _close + ' -P '+path+' http://download.hoto.cn/Mobile_Bi/'+prefix+_curDate+'002_41.tar.lzo')
		os.popen('wget ' + _close + ' -P '+path+' http://download.hoto.cn/Mobile_Bi/'+prefix+_curDate+'002_50.tar.lzo')
		os.popen('wget ' + _close + ' -P '+path+' http://download.hoto.cn/Mobile_Bi/'+prefix+_curDate+'002_51.tar.lzo')
		"""

		os.popen('axel -n 10 -s 3145728  ' + _close + ' -o '+path+' http://download.hoto.cn/Mobile_Bi/'+prefix+_curDate+'002_40.tar.lzo 2>> ' + path + '/dailyUpdateLog.log')
		os.popen('axel -n 10 -s 3145728  ' + _close + ' -o '+path+' http://download.hoto.cn/Mobile_Bi/'+prefix+_curDate+'002_41.tar.lzo 2>> ' + path + '/dailyUpdateLog.log')
		os.popen('axel -n 10 -s 3145728  ' + _close + ' -o '+path+' http://download.hoto.cn/Mobile_Bi/'+prefix+_curDate+'002_50.tar.lzo 2>> ' + path + '/dailyUpdateLog.log')
		os.popen('axel -n 10 -s 3145728  ' + _close + ' -o '+path+' http://download.hoto.cn/Mobile_Bi/'+prefix+_curDate+'002_51.tar.lzo 2>> ' + path + '/dailyUpdateLog.log')

		print 'resp下载结束'

def moveToHDFS(curDate,_curDate):
	
	os.popen('hdfs dfs -rm -f /user/yarn/logs/source-log.php.CDA39907/'+curDate+'/'+prefix+_curDate+suffix)

	os.popen('hdfs dfs -moveFromLocal '+path+'/'+prefix+_curDate+suffix+' /backup/CDA39907/'+curDate)

	os.popen('hdfs dfs -rm -f /user/yarn/logs/source-log.php.CDA39907.resp/'+curDate+'/*')

	os.popen('hdfs dfs -moveFromLocal '+path+'/'+prefix+_curDate+'002_40.tar.lzo /backup/CDA39907.resp/'+curDate)
	os.popen('hdfs dfs -moveFromLocal '+path+'/'+prefix+_curDate+'002_41.tar.lzo /backup/CDA39907.resp/'+curDate)
	os.popen('hdfs dfs -moveFromLocal '+path+'/'+prefix+_curDate+'002_50.tar.lzo /backup/CDA39907.resp/'+curDate)
	os.popen('hdfs dfs -moveFromLocal '+path+'/'+prefix+_curDate+'002_51.tar.lzo /backup/CDA39907.resp/'+curDate)

	os.popen('hdfs dfs -mkdir /user/yarn/logs/source-private_custom_log/'+curDate)
	os.popen('hdfs dfs -rm -f /user/yarn/logs/source-private_custom_log/'+curDate+'/*')
	os.popen('hdfs dfs -moveFromLocal '+path+'/tomcat206_'+_curDate+'.tar.lzo /backup/source-private_custom_log/'+curDate+'/')
	os.popen('hdfs dfs -moveFromLocal '+path+'/tomcat207_'+_curDate+'.tar.lzo /backup/source-private_custom_log/'+curDate+'/')

	#add lzo indexer
	#os.popen('hadoop jar /usr/lib/hadoop/lib/hadoop-lzo-0.6.0.jar com.hadoop.compression.lzo.LzoIndexer /user/yarn/logs/source-log.php.CDA39907/%s' % curDate)
	#os.popen('hadoop jar /usr/lib/hadoop/lib/hadoop-lzo-0.6.0.jar com.hadoop.compression.lzo.LzoIndexer /user/yarn/logs/source-log.php.CDA39907.resp/%s' % curDate)


path  = os.path.dirname(os.path.abspath(__file__))
hive = HiveDB()
errorMail = MailSender()
nowday = datetime.now()

md5data = {}

prefix = 'CDA39907'
suffix = '001.AVL.log.tar.lzo'

if __name__ == '__main__':

	usage = "usage: %prog [options] arg1 arg2"
	parser = OptionParser(usage = usage, version="%prog 0.1")
	parser.add_option("-d", "--days", help = 'Download the log back for a few days, default value is 1, use \',\' separated, like 10,1', dest = "days")
	parser.add_option("-n", "--notd", help = 'Do not download the log file, input value is False', action="store_true", default=False, dest="notd")
	parser.add_option("-c", "--close", help = 'Close Quiet mode, input value is False is quiet mode', action="store_true", default=False, dest="close")
	parser.add_option("-a", "--applog", help = 'Close download CDA39907 data, input value is True is download', action="store_false", default=True, dest="applog")
	parser.add_option("-r", "--resp", help = 'Close download CDA39907 response data, input value is True is download', action="store_false", default=True, dest="resp")
	
	#parser.add_option("-n", "--notd", help = 'Do not download the log file, input value is 0', dest = "notd")
	#parser.add_option("-c", "--close", help = 'Close Quiet mode, input value is 0 is quiet mode', dest = "close")
	#parser.add_option("-a", "--applog", help = 'Close download CDA39907 data, input value is 1 is download', dest = "applog")
	#parser.add_option("-r", "--resp", help = 'Close download CDA39907 response data, input value is 1 is download', dest = "resp")
	
	
	(options, args) = parser.parse_args()

	if len(sys.argv)<=1:
		parser.print_help()
		sys.exit(2)

	days = options.days or "1"
	delay = days.split(",")

	if len(delay) != 1:

		for today in range(int(delay[0]),int(delay[1])-1,-1):
			start(path,today,options.notd,options.close,options.applog,options.resp)

	else:
		start(path,int(delay[0]),options.notd,options.close,options.applog,options.resp)
