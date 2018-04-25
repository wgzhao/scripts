# -*- coding: UTF-8 -*-
#coding = utf-8
#shark 数据库连接器


import commands
import re
import os

class HiveDB:

	def __init__(self, args=""):
		self.args = args

	def queryString(self, sql):

		path  = os.path.dirname(os.path.abspath(__file__))

		cmd = """\
		hive -i %s/hiverc %s --hiveconf mapreduce.job.queuename=alpha --hiveconf mapreduce.map.memory.mb=16384 --hiveconf mapreduce.reduce.memory.mb=16384 -e "%s" \
		"""
		print cmd%(path,self.args,sql)

		status, ret = commands.getstatusoutput(cmd%(path,self.args,sql))
		#print ret
		regex_time = "^[\s\S]*Time taken: (?P<time>[1-9]\d*\.\d*|0\.\d*[1-9]\d*|0?\.0+|0)"
		m = re.findall(regex_time, ret)
		for (time) in m:
			print "查询消耗时间: "+ time

		regex = "^[\s\S]*[Total MapReduce CPU Time Spent:|org.slf4j.impl.Log4jLoggerFactory].+?[\s\S]OK[\n](?P<res>[\s\S]*)Time taken:[\s\S]*$"
		m = re.findall(regex, ret)
		if not m:
			print ret
		for (res) in m:
			return res.strip('\n')

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

