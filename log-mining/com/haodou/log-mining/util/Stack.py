#encoding=utf-8

import sys

class Stack(object):
	def __init__(self):
		self.es=[]
		self.curIndex=0

	def add(self,e):
		if len(self.es) <= self.curIndex:
			self.es.append(e)
		else:
			self.es[self.curIndex]=e
		self.curIndex+=1
	
	def get(self):
		if self.curIndex <= 0:
			return (-1,None)
		return (self.curIndex-1,self.es[self.curIndex-1])

	def pop(self):
		index,e=self.get()
		if index >= 0:
			self.curIndex=index
		return index,e

class Queue(object):
	def __init__(self,MaxSize,init=None):
		self.init=init
		self.vs=[init]*MaxSize
		self.MaxSize=MaxSize
		self.index=0
		self.numOfE=0
	
	def raiseIndex(self):
		self.index+=1
		if self.numOfE < self.MaxSize:
			self.numOfE+=1

	def add(self,v):
		#print "add v:%f"%(v)
		self.vs[self.index%self.MaxSize]=v
		self.raiseIndex()

	def minusIndex(self):
		if self.numOfE > 0:
			self.numOfE-=1

	def pop(self):
		if self.numOfE <= 0:
			return self.init
		curIndex=(self.index-self.numOfE)%self.MaxSize
		v=self.vs[curIndex]
		self.vs[curIndex]=self.init
		self.numOfE-=1
		return v

	def getVs012(self,size):
		curIndex=self.index%self.MaxSize
		vs0= self.vs[curIndex-size]
		vs1= self.vs[curIndex-size+1]
		vs2= self.vs[curIndex-size+2]
		return (vs0,vs1,vs2)

	def getV_12(self):
		curIndex=self.index%self.MaxSize
		v_1=self.vs[curIndex-1]
		v_2=self.vs[curIndex-2]
		return (v_1,v_2)

	def v0(self):
		return self.vs[(self.index-1)%self.MaxSize]
	
	def v1(self):
		return self.vs[(self.index)%self.MaxSize]

	def eleIndexs(self):
		start=(self.index-self.numOfE)%self.MaxSize
		end=self.index%self.MaxSize
		if end <= start:
			start-=self.MaxSize
		return (start,end)

	def getI(self,i):
		return self.vs[i%self.MaxSize]

if __name__=="__main__":
	s=Stack()
	s.add(4)
	s.add(5)
	print s.get()
	print s.pop()
	print s.get()
	print s.pop()
	print s.pop()



