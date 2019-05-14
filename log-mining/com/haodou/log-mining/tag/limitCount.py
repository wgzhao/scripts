#encoding=utf-8

import sys
import random

LWC_BATCH=2
MIN_LIMIT=10
FREQ_DIV=3
MIN_FREQ=2

class LWC(object):
	def __init__(self,limit=100,parent=None):
		self.wc={}
		self.wlist=[None]*2*limit
		self.limit=limit
		self.fwc=None
	
	def addSize(self,size=100):
		if size <= 0:
			return
		self.wlist+=[None]*2*size
		self.limit+=size
		if self.fwc != None:
			self.fwc.addSize(self.limit/FREQ_DIV-self.fwc.limit)

	def getIndex(self,void=True):
		i=random.randrange(self.limit*2)
		nn=0
		while ((self.wlist[i] == None) ^ void):
			i=random.randrange(self.limit*2)
			nn+=1
			if nn > 100:
				#sys.stderr.write(str(self)+"\n")
				sys.stderr.write("limitCount.getIndex.nn > 100(hasParent=%rvoid=%r\t%d\t%d)\n"%((self.parent!=None),void,2*self.limit,len(self.wc)))
				return -1
		return i
	
	def getVoid(self):
		return self.getIndex(void=True)

	def inFreq(self,w):
		if self.fwc != None:
			if self.fwc.get(w) > 0:
				return True
		else:
			if w in self.wc and self.wc[w] >= MIN_FREQ:
				return True
		return False

	def get(self,w):
		if w in self.wc:
			return self.wc[w]
		return 0

	def getIndexV(self,fi):
		w=self.wlist[fi]
		if w == None:
			return 0
		else:
			return self.get(w)

	def minus(self,fi):
		if fi < 0:
			return
		w=self.wlist[fi]
		if w in self.wc:
			self.wc[w]-=1
			if self.wc[w] <= 0:
				del	self.wc[w]
				self.wlist[fi]=None

	def cut(self):
		if len(self.wc) <= self.limit:
			return
		if self.fwc==None and self.limit > MIN_LIMIT:
			self.fwc=LWC(limit=self.limit/FREQ_DIV,parent=self)
		for i in range(LWC_BATCH):
			fi=self.getIndex(void=False)
			self.minus(fi)
			if len(self.wc) < self.limit/2+1:
				break

	def add(self,w,c=1):
		if w == None:
			return
		self.cut()
		if w in self.wc:
			self.wc[w]+=c
			wv=self.wc[w]
			if self.fwc != None and wv >= MIN_FREQ:
				self.fwc.add(w)
		else:
			self.wc[w]=c
			vi=self.getVoid()
			if vi < 0:
				return
			self.wlist[vi]=w

	def __str__(self):
		s=str(self.wc)+"\t"+str(self.wlist)+"\n"
		if self.fwc != None:
			s+="\t"+str(self.fwc)
		return s

def test():
	wc=LWC(20)
	print wc
	for i in range(500):
		k=random.randrange(40)
		if random.random()<0.8:
			k=random.randrange(12)
		print "k",k
		wc.add(k)
		k1=random.randrange(40)
		print wc
		print "k1",k1,wc.get(k1)

if __name__=="__main__":
	test()



