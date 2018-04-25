#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
@ 计算自然周第一天、自然月第一天和每天的凌晨时间戳
@ author : jomin
@ email  : qkzhaoming@163.com
@ mtime  : 2012-09-01
"""
import time

class TimeUtils:

	def get_day_begin(self, ts = time.time(),N = 0):
		"""
		N为0时获取时间戳ts当天的起始时间戳，N为负数时前数N天，N为正数是后数N天
		"""
		return int(time.mktime(time.strptime(time.strftime('%Y-%m-%d',time.localtime(ts)),'%Y-%m-%d'))) + 86400*N

	def get_week_begin(self, ts = time.time(),N = 0):
		"""
		N为0时获取时间戳ts当周的开始时间戳，N为负数时前数N周，N为整数是后数N周，此函数将周一作为周的第一天
		"""
		w = int(time.strftime('%w',time.localtime(ts)))
		return self.get_day_begin(int(ts - (w-1)*86400)) + N*604800

	def get_month_begin(self, ts = time.time(),N = 0):
		"""
		N为0时获取时间戳ts当月的开始时间戳，N为负数前数N月，N为正数后数N月
		"""
		month_day = {1:31,3:31,4:30,5:31,6:30,7:31,8:31,9:30,10:31,11:30,12:31}
		cur_y,cur_m,cur_d = [int(x) for x in time.strftime('%Y-%m-%d',time.localtime(ts)).split('-')]
		if (cur_y%4 == 0 and cur_y%100 != 0) or cur_y%400 == 0:
			month_day[2] = 29
		else:
			month_day[2] = 28
			t = self.get_day_begin(ts) - (cur_d-1)*86400
			real_month = N + cur_m
		if real_month == cur_m:
			return t
		if N > 0:
			if real_month <= 12:
				for x in xrange(cur_m,real_month):
					t += month_day[x]*86400
			if real_month > 12:
				for x in xrange(cur_m,13):
					t += month_day[x]*86400
				t = self.get_month_begin(t,real_month - 13)
		if N < 0:
			if real_month >= 1:
				for x in xrange(real_month,cur_m):
					t -= month_day[x]*86400
			if real_month < 1:
				for x in xrange(1,cur_m):
					t -= month_day[x]*86400
				t -= month_day[12]*86400
				t = self.get_month_begin(t,real_month)
		return t

"""
if __name__ == "__main__":
	tu = TimeUtils()
	t = time.time()
	#算当天
	print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(tu.get_day_begin(t)))
	#算明天
	print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(tu.get_day_begin(t,1)))
	#算昨天
	print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(tu.get_day_begin(t,-1)))
	#算本周
	print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(tu.get_week_begin(t)))
	#算下一周
	print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(tu.get_week_begin(t,1)))
	#算上一周
	print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(tu.get_week_begin(t,-1)))
	#算当月
	print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(tu.get_month_begin(t)))
	#算下一月
	print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(tu.get_month_begin(t,1)))
	#算上一月
	print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(tu.get_month_begin(t,-1)))
"""
