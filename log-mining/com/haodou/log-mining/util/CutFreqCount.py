#encoding=utf-8

import sys
import heapq
from utf8 import *

class CWC(object):
	def __init__(self,n=1000,adjustRate=0.2):
		self.wc={}
		self.sum=0
		self.n=n
		self.cutNum=n*(1.0+adjustRate)

	def cut(self):
		if len(self.wc) > self.cutNum:
			cuts=heapq.nsmallest(len(self.wc)-self.n,self.wc.items(),key=lambda e:e[1])
			for w,v in cuts:
				del self.wc[w]

	def add(self,w,v):
		if w not in self.wc:
			self.wc[w]=v
			self.cut()
		else:
			self.wc[w]+=v
		self.sum+=v

	def __str__(self):
		s="{"
		n=0
		for w in self.wc:
			if n== 0:
				n+=1
			else:
				s+=","
			s+=ustr(w)+":"+str(self.wc[w])
		s+="}"
		return s


if __name__=="__main__":
	cwc=CWC(n=20)
	import random
	for i in range(1000000):
		rr=random.random()
		if rr < 0.5:
			a=random.randrange(2)
		else:
			a=random.randrange(3,50,1)
		cwc.add(a,1)
	print cwc

