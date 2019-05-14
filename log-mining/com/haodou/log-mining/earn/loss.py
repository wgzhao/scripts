#encoding=utf-8

import sys

from conf import *

YearFold=270*250.0

class Loss(object):
	def __init__(self,start):
		self.time=0
		self.start=start
		self.now=start
		self.min=start
		self.max=start
		self.maxTrack=0
		self.maxTrackAt=0
		self.lastEarnTime=0
		self.maxLossTime=0
		self.maxLossTimeAt=0
		self.win=0
		self.loss=0
		self.winE=0
		self.lossE=0

	def setTime(self,time):
		self.time=time
	
	def addTime(self,add=1):
		self.time+=add

	def addTrack(self):
		if self.now < self.min:
			self.min=self.now
		if self.now >= self.max:
			self.max=self.now
			self.lastEarnTime=self.time
		else:
			track=(self.max-self.now)/self.max
			if track > self.maxTrack:
				self.maxTrack=track
				self.maxTrackAt=self.time
			lossTime=self.time-self.lastEarnTime
			if lossTime > self.maxLossTime:
				self.maxLossTime=lossTime
				self.maxLossTimeAt=self.time
	
	def addE(self,e):
		self.now+=e
		self.addTrack()

	def addTrackE(self,te):
		self.now+=te
		self.addTrack()
		self.now-=te

	def addWin(self,e,conf):
		if e > 0:
			self.win+=1
			self.winE+=e*conf.HandSize
		else:
			self.loss+=1
			self.lossE+=e*conf.HandSize
	
	def confStr(self,conf):
		try:
			s="time:%d	now:%.4f min:%.4f max:%.4f\t"%(self.time,self.now,self.min,self.max)
			s+="超出值：%.2f\t%.2f\t%.6f\n"%(self.now-self.start,self.start,(self.now-self.start)/self.start)
			s+="win:%d\t%.4f\t%d\t%.4f\trate:%.4f\tmrate:%.4f\n"%(self.win,self.winE,self.loss,self.lossE,float(self.win)/(self.loss+self.win+1e-12),self.winE/(abs(self.lossE)+self.winE+1e-12))
			d=1.0
			if self.now < 0:
				d=-1.0
			self.rate=d*math.pow(abs(self.now/self.start),float(conf.YearFold)/(self.time+0.1))-1.0
			s+="增长率:%f\t最大下降:%.3f\t@%d\t增长下降比:%.4f\n"%(self.rate,self.maxTrack,self.maxTrackAt,self.rate/(self.maxTrack+1e-32))
			s+="最大下降周期:%d=%.4f年\t@%d"%(self.maxLossTime,self.maxLossTime/conf.YearFold,self.maxLossTimeAt)
			return s
		except:
			return ""
	
	def __str__(self):
		return self.confStr(basicConf)

