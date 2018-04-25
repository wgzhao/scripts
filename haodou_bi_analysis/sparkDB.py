# -*- coding: UTF-8 -*-
#coding = utf-8
#spark 数据库连接器


from datetime import *
import re
import os


class SparkDB:
	def __init__(self, args=""):
		self.args = args


	def queryString(self, sql):
		path  = os.path.dirname(os.path.abspath(__file__))

		cmd = """ \
		SPARK_YARN_QUEUE=beta /opt/app/spark/bin/spark-sql --jars hdfs://hdcluster/udf/haodoubihiveudf.jar,hdfs://hdcluster/udf/bing.jar -i %s/hiverc %s -e "%s"
		"""

		print cmd%(path,self.args,sql)

		begin = datetime.now()
		#status, ret = commands.getstatusoutput(cmd%(path,self.args,sql))
		rt = os.popen(cmd%(path,self.args,sql))
		ret = rt.read()

		ret = ret.replace(r"--args is deprecated. Use --arg instead.","").replace(r"SET spark.sql.hive.version=0.13.1","").replace(r"SET mapreduce.job.queuename=alpha","").replace(r"mapreduce.job.queuename=alpha","").strip()
		end = datetime.now()
		print "查询消耗时间: " + str((end-begin).seconds)
		return ret

	def execute(self, sql):
		res = self.queryString(sql)
		if res is not None:
			return Result(res.split('\n'))

class Result:

	def __init__(self, rs):

		self.values = rs

	def fetchone(self):
		return re.split(r"\s+",self.values[0])

	def __len__(self):
		return len(self.values)

	def __getitem__(self, key):
		#如果键的类型或者值无效，列表值将会抛出错误
		return re.split(r"\s+",self.values[key])

	def __iter__(self):
		return iter(self.values)

	def tail(self):
		return self.values[1:]

	def first(self):
		return fetchone()

	def last(self):
		#返回末尾元素
		return re.split(r"\s+",self.values[-1])

	def take(self, n):
		#返回前n个元素
		return self.values[:n]
