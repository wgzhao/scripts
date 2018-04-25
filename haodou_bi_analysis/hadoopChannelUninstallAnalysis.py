# -*- coding: UTF-8 -*- 
#coding = utf-8
#渠道卸载量统计

#from pyspark.java_gateway import launch_gateway
#from pyspark import SparkContext
#from pyspark.conf import SparkConf
from operator import add
from datetime import  *
import traceback
from urllib import *
from hiveDB import *
from sharkDB import *
import base64
import time
import sys
import rsa
import os
import re

class ChannelUninstallAnalysis:
	
	def __init__(self, delay=1, fpath=""):
		
		self.delay = delay
		
		dt = datetime.now()
		self._datetime = dt - timedelta(days = self.delay)
		self.curDate = self._datetime.strftime("%Y-%m-%d")
		
		self.path = fpath
		self.hive = HiveDB()
		#self.shark = SharkDB()
		
		with open(self.path + '/rsa-transform-key.txt' , 'rb') as privatefile:
			p = privatefile.read()
			self.privkey = rsa.PrivateKey.load_pkcs1(p)
		
		self.regex_url = r'^.*uninstall.php?(?P<url>.+?)&time.*'
		self.regex = r'^.*&encode=(?P<crypto>.+).*$'
		self.regex_channel = r'^.*channel=(?P<chl>.+?)&vc=.*$'

	
	#主方法
	def startExtract(self):
		channels = {}
		
		#res = self.shark.execute("select referer from m_haodou_com where referer like '%uninstall.php%' and logdate='"+self.curDate+"'")
		res = self.hive.execute("select referer from m_haodou_com where referer like '%uninstall.php%' and logdate='"+self.curDate+"'")
		for chunk in res:
			m = re.findall(self.regex_url, chunk)
			for (url) in m:
				url = unquote(url)
				m = re.findall(self.regex, url, re.S)
				try:
					for (crypto) in m:
						crypto = base64.b64decode(crypto)
						message = rsa.decrypt(crypto, self.privkey)
						
						m = re.findall(self.regex_channel, message)
						print message
						for (chl) in m:
							if chl not in channels:
								channels[chl] = 1
							else:	
								channels[chl] = channels[chl] + 1
				except:
					print chunk
					traceback.print_exc()
					continue
		return channels
		
"""
arg = sys.argv[1:]

if len(arg)<1:
	print >> sys.stderr, "Usage: hadoopChannelUninstallAnalysis <delay>"
	sys.exit(2)
	
delay = int(arg[0])

paths = os.popen("pwd").readlines()
path = paths[0].replace("\n","")
	
cua = ChannelUninstallAnalysis(delay, path)
#cua.startExtract()

t = cua.startExtract()

print len(t)




thegateway = launch_gateway()
		thegateway.launch_gateway(jarpath="/home/likunjian/py4j0.8.1.jar", javaopts=["-Xmx5120m"])
		
		theconf = SparkConf().set("spark.executor.memory", "100g").setAppName("ChannelUninstallAnalysis").setMaster("local[1]")
		sc = SparkContext(conf=theconf)
		
		counts = sc.parallelize(res) \
						.flatMap(lambda x: x.split('\n')) \
						.map(lambda x: self.parseChannel(x)) \
						.reduceByKey(add)
						
		output = counts.collect()
		for (word, count) in output:
			print "%s: %i" % (word, count)

"""
