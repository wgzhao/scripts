#encoding=utf-8

import sys
from wave import *

class Wave(object):
	def __init__(self,vs):
		self.vs=vs
		self.getEx()

	def getEx(self):
		if len(self.vs) <= 0:
			return
		self.es=[]
		max=self.vs[0]
		min=self.vs[0]
		maxI=0
		minI=0
		lastDiv=0
		divSum=0
		sum=self.vs[0]
		for i in range(1,len(self.vs),1):
			v=self.vs[i]
			sum+=v
			if v > max:
				max=v
				maxI=i
			elif v < min:
				min=v
				minI=i
			div=self.vs[i]-self.vs[i-1]
			if div*lastDiv < 0:
				self.es.append(self.vs[i-1])
			lastDiv=div
			div=abs(div/v)*100
			if div > 1:
				div=1.0
			divSum+=div
		self.max=max
		self.maxI=maxI
		self.min=min
		self.minI=minI
		self.sum=sum
		self.avg=sum/len(self.vs)
		self.divSum=divSum

	def simpleStr(self):
		(a0,a1)=leastSquare(self.vs)
		s="%.2f\t%.2f\t%.2f\t%d\t%.2f\t%d\t%d\t%.2f\t%.4f"%(self.sum,self.avg,self.max,self.maxI,self.min,self.minI,len(self.es),a0,a1)
		return s

	def __str__(self):
		s=str(self.vs)
		if len(self.vs) <= 0:
			return s
		s+="\t%f\t%d\t%f\t%d"%(self.max,self.maxI,self.min,self.minI)
		s+="\t"+str(self.es)
		return s

def testWave(n):
	nn=0
	vs=[]
	lastA1=0
	k=0
	rk=0
	for line in sys.stdin:
		v=float(line.strip())
		vs.append(v)
		nn+=1
		if nn % n == 0:
			w=Wave(vs)
			#print w.simpleStr()
			#print len(w.es)
			(a0,a1)=leastSquare(vs)
			if a1*lastA1 > 0:
				rk+=1
			lastA1=a1
			k+=1
			vs=[]
			print "%d\t%.4f\t%.3f\t%.2f"%(len(w.es),abs(a1),w.divSum/len(w.es),w.divSum)
	#print k,rk,float(rk)/k

if __name__=="__main__":
	testWave(int(sys.argv[1]))

