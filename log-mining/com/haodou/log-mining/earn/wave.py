#encoding=utf-8

import sys
sys.path.append("./")
from conf import *
sys.path.append("../util/")
import Stack

class Count(object):
	def reset(self):
		self.num=0
		self.sum=0
		self.avg=0
		self.y2=0
		self.xy=0
		self.divSum=0
		self.run=0
		self.dev=0
		self.mAmpSum=0  #
		self.mAmp=0	#高于平均振幅的振幅均值
		self.aAmp=0  #平均振幅,是区间内最大波的振幅。在多个区间上求均值。
		self.ampSum=0 #
		#self.N=0
		self.max=0
		self.min=sys.float_info.max
		self.open=0
		self.close=0
	
	def __init__(self,size=100):
		self.size=size
		self.nx=(size + 1) * size / 2
		self.nx2=size * (size + 1) * (2*size + 1) / 6
		self.reset()
	
	def addV(self,v,backAdd=True):
		self.y2+=v*v
		if backAdd:
			self.xy+=v*self.num+v
			self.colse=v
		else:
			self.xy+=(v+self.sum)
			self.open=v
		if self.num == 0:
			self.close=v
			self.open=v
		self.num+=1
		self.sum+=v
		self.avg=self.sum/self.num
		if self.max < v:
			self.max=v
		if self.min > v:
			self.min=v


	#注意：从时间上来讲，如果backAdd是False，那么被merge的对象应该位于前面
	def merge(self,other,backAdd=True):
		self.y2+=other.y2
		if backAdd:
			self.xy+=(self.num*other.sum+other.xy)
			self.close=other.close
		else:
			#print "self.xy",self.xy,"other.xy",other.xy,"self.sum",self.sum
			self.xy+=(other.xy+other.num*self.sum)
			self.open=other.open
			#print "self.xy",self.xy
		if self.num == 0:
			self.close=other.close
			self.open=other.open
		self.num+=other.num
		self.sum+=other.sum
		self.avg=self.sum/self.num
		if self.max < other.max:
			self.max=other.max
		if self.min > other.min:
			self.min=other.min

	def __str__(self):
		s="num:%d,sum:%.3f,avg:%.3f,y2:%.3f,xy:%.3f,max:%.3f,min:%.3f,a1:%.6f"%(self.num,self.sum,self.avg,self.y2,self.xy,self.max,self.min,self.a1())
		return s
	
	def a1(self):
		if self.num <= 1:
			return 0
		if self.num < self.size:
			nx=(self.num + 1) * self.num / 2
			nx2=self.num * (self.num + 1) * (2*self.num + 1) / 6
			return (self.num*self.xy-nx*self.sum)/(self.num*nx2-nx*nx)
		return (self.size*self.xy - self.nx*self.sum) / (self.size*self.nx2-self.nx*self.nx)

class Base(object):
	def __init__(self,bases):
		self.MaxSize=0
		self.counts={}
		self.bases=sorted(bases,reverse=True)
		self.avgs={}
		for size in bases:
			self.avgs[size]=Count()
			msize=size*10+1
			if msize > self.MaxSize:
				self.MaxSize=msize
		for size in bases:
			self.counts[size]=Stack.Queue(self.MaxSize/size+1)
		self.vq=Stack.Queue(self.MaxSize)
		self.phases={}
		for size in self.bases:
			tsize=size/10
			min=tsize-1
			pbase=1
			for msize in self.bases:
				if size == msize:
					continue
				d=abs(msize-tsize)
				if d < min:
					min=d
					pbase=msize
			self.phases[size]=pbase

	def v0(self):
		return self.vq.v0()

	def combineCC(self,n,cc):
		cc.reset()
		if n > self.vq.index:
			n=self.vq.index
		#print "n",n
		rn=n
		r=0
		end=self.vq.index
		for size in self.bases:
			rest=end%size
			if rn < rest:
				continue
			countQueue=self.counts[size]
			k=(rn-rest)/size
			if rest == 0 and k == 0:
				continue
			e=-r/size
			m=(rest+k*size)
			end-=m
			rn-=m
			r+=m
			if rest > 0:
				s=e-k-1
			else:
				s=e-k
			#print "size",size,"rn",rn,"rest",rest,"s",s,"e",e,"countQueue.MaxSize",countQueue.MaxSize
			s=(s+countQueue.index)%countQueue.MaxSize
			e=(e+countQueue.index)%countQueue.MaxSize
			if e < s:
				s-=countQueue.MaxSize
			#print "size",size,"rn",rn,"rest",rest,"s-1",s-1,"e-1",e-1,"countQueue.get(e-1)",countQueue.getI(e-1)
			for i in range(e-1,s-1,-1):
				cc.merge(countQueue.getI(i),False)
			if rn == 0:
				break
		if rn > 0:
			if rn > end:
				rn=end
			end=end%self.vq.MaxSize
			#print "end-1",end-1,"end-rn-1",end-rn-1
			for i in range(end-1,end-rn-1,-1):
				cc.addV(self.vq.getI(i),False)
		return cc
	
	def combine(self,n):
		cc=Count(n)
		return self.combineCC(n,cc)

	#近似组合，提高计算速度
	def approximateCombine(self,n):
		pass

	def add(self,v):
		for size in self.avgs:
			count=self.avgs[size]
			if count.num < size:
				count.num+=1
			else:
				(vs0,vs1,vs2)=self.vq.getVs012(size)
				if vs0 == None:
					sys.stderr.write("num:%d\tindex:%d\tsize:%d\n"%(count.num,self.vq.index,size))
				else:
					count.sum-=vs0
			count.sum+=v
			count.avg=count.sum/count.num
		vindex=self.vq.index
		#print "vnum",vnum
		for size in self.counts:
			countQueue=self.counts[size]
			if vindex % size == 0:
				countQueue.add(Count(size))
			count=countQueue.v0()
			count.addV(v)
			if vindex > 0:
				(v_1,v_2)=self.vq.getV_12()
				div=(v-v_1)
				count.divSum+=abs(div)
				if vindex > 1:
					if div*(v_1-v_2) < 0:
						count.run+=1
		self.vq.add(v)
		
	#dual trust中的range值
	def rangeV(self,size,n):
		countQueue=self.counts[size]
		(start,end)=countQueue.eleIndexs()
		if start < end -n:
			start=end-n
		hh=0
		ll=sys.float_info.max
		hc=0
		lc=sys.float_info.max
		r=0
		mr=0
		for i in range(start,end):
			count=countQueue.getI(i)
			if count.max > hh:
				hh=count.max
			if count.min < ll:
				ll=count.min
			if count.close > hc:
				hc=count.close
			if count.close < lc:
				lc=count.close
			#print "close",count.close,"max",count.max,"min",count.min
			#print "hh",hh,"ll",ll,"hc",hc,"lc",lc
			if r < hh-lc:
				r=hh-lc
			if r < hc -ll:
				r=hc-ll
			if mr < count.max-count.close:
				mr=count.max-count.close
			if mr < count.close-count.min:
				mr=count.close-count.min
		return r,mr


class Phase(object):
	def __init__(self,sizes,bases=[5,20,100,500,2500]):
		self.counts={}
		self.curIndex=0
		MaxSize=0
		for size in bases:
			sizes.append(size)
		self.base=Base(bases)
		for size in sizes:
			self.counts[size]=Count(size)
			if size > MaxSize:
				MaxSize=size
		self.vs=Stack.Queue(MaxSize+1)
		
	def getSmallDiv(self,size):
		ssize=size/5
		if ssize not in self.counts:
			return 0
		return self.counts[ssize].mdiv

	def simpleAdd(self,v):
		for size in self.counts:
			count=self.counts[size]
			if count.num < count.size:
				count.num+=1
			else:
				count.xy-=count.sum
				(vs0,vs1,vs2)=self.vs.getVs012(size)
				count.sum-=vs0
				count.y2-=vs0*vs0
				count.divSum-=abs(vs1-vs0)
				if (vs2-vs1)*(vs1-vs0) < 0:
					count.run-=1
			count.sum+=v
			count.xy+=(v*count.num)
			count.y2+=v*v
			if count.num > 1:
				(v_1,v_2)=self.vs.getV_12()
				div=(v-v_1)
				count.divSum+=abs(div)
				if count.num > 2:
					if div*(v_1-v_2) < 0:
						count.run+=1
			count.avg=count.sum/count.num
		self.vs.add(v)
		
	def add(self,v):
		self.base.add(v)
		#self.updateSize()
	
	def updateSize(self):
		for size in self.counts:
			self.counts[size]=self.base.combineCC(size,self.counts[size])
			#print "size",size,"cc",self.counts[size],"\n"
	
	def divAvg(self,size):
		count=self.counts[size]
		#print "divSum",count.divSum,"num",count.num
		return count.divSum/count.num

	def dev(self,size):
		count=self.counts[size]
		s2=count.y2-count.sum/count.num*count.sum
		if count.num <= 1:
			return 0
		else:
			d=math.pow(s2/(count.num-1),0.5)
		return d

	def v0(self):
		v=self.base.v0()
		if v == None:
			return self.vs.v0()
		return v

	def sum(self,size):
		return self.counts[size].sum

	def avg(self,size):
		return self.counts[size].avg

	def max(self,size):
		return self.counts[size].max

	def min(self,size):
		return self.counts[size].min

	def xy(self,size):
		return self.counts[size].xy

	#a1=(n*nxy-nx*ny)/(n*nx2-nx*nx)
	##a1 = [n∑Xi Yi - （∑Xi ∑Yi)] / [n∑Xi2 - （∑Xi)2 )] 
	def a1(self,size):
		count=self.counts[size]
		#a1 = (size*count.xy - count.nx*count.sum) / (size*count.nx2-count.nx*count.nx)
		return count.a1()

	#a0=ny/n-a1*nx/n
	def a0(self,size):
		#return self.sums[size]/size-self.a1(size)*(size+1)/2
		return self.counts[size].sum/size-self.a1(size)*(size+1)/2

	def next(self,size):
		#return self.a0(size)+(size+1)*self.a1(size)
		return self.a0(size)+(size+1)*self.a1(size)

def testPhase():
	sizes=[5,33,78]
	phase=Phase(sizes)
	for i in range(1000):
		phase.add(1.0*i+10.0)
	for size in sizes:
		print size,phase.divSums[size],phase.runs[size],phase.divAvg(size),phase.divAvg1(size),phase.dev(size),phase.xy(size),phase.a1(size),phase.a0(size),phase.next(size),phase.sum(size),phase.avg(size),phase.v0(size)
	

def waveType(vs,d):
	maxI=0
	minI=0
	state=0
	ms=[]
	for i in range(1,len(vs),1):
		v=vs[i]
		#print i,v,"state",state,"minI",minI,"maxI",maxI
		if v > vs[maxI]:
			if v - vs[minI] > d*vs[minI]:
				print "max",i,v,vs[minI],d*v
				if state <= 0:
					ms.append(minI)
				minI=i
				print "minI",minI
				state=1
			maxI=i
		elif v < vs[minI]:
			if vs[maxI]-v > d*vs[maxI]:
				print "min",i,v,vs[maxI],d*vs[maxI]
				if state >= 0:
					ms.append(maxI)
				maxI=i
				state=-1
			minI=i
	print "state",state,"minI",minI,"maxI",maxI
	if state > 0:
		ms.append(maxI)
	elif state < 0:
		ms.append(minI)
	return ms

def testWaveType():
	vs=[]
	i=1
	for line in sys.stdin:
		vs.append(float(line.strip()))
		if i % 120 == 0:
			print vs
			ms=waveType(vs,0.005)
			print "------"
			for m in ms:
				print m,vs[m]
			vs=[]
		i+=1

def maxPhaseDiv(vs):
	max=0
	min=1000000
	for v in vs:
		if max < v:
			max=v
		if min > v:
			min=v
		div = max-min
	return div

def testPhaseDiv(size=120):
	vs=[]
	i=1
	sum=0
	y2=0
	k=0
	maxDiv=0
	ssum=0
	sk=0
	N=5
	ssize=size/N
	tavg=0
	tsum=0
	tk=0
	mk=0
	msum=0
	for line in sys.stdin:
		vs.append(float(line.strip()))
		if i % size == 0:
			#print vs
			div=maxPhaseDiv(vs)
			if div > maxDiv:
				maxDiv=div
			sum+=div
			tsum+=div
			y2+=div*div
			k+=1
			tk+=1
			tavg=tsum/tk
			if div > tavg:
				mk+=1
				msum+=div
			#print sum,y2,k
			for si in range(N):
				ssum+=maxPhaseDiv(vs[si*ssize:(si+1)*ssize])
			sk+=N
			if k % 100 == 0:
				savg=ssum/sk
				avg=sum/k
				dev=math.pow((y2-k*avg*avg)/(k-1),0.5)
				mavg=msum/mk
				print k,sum,avg,y2,dev,maxDiv,savg,avg/savg,mavg/tavg,mavg/savg
				sum=0
				y2=0
				k=0
				sk=0
				ssum=0
				maxDiv=0
			vs=[]
		i+=1
	avg=sum/k
	dev=math.pow((y2-k*avg*avg)/(k-1),0.5)
	savg=ssum/sk
	print k,sum,avg,y2,dev,maxDiv,savg,avg/savg
	mavg=msum/mk
	print msum,mk,mavg,tsum,tk,tavg,mavg/tavg,mavg/savg


def testBase():
	base=Base([5,25,125,625])
	phase=Phase([100,234,590,1800])
	for line in sys.stdin:
		v=float(line.strip().split()[0])
		base.add(v)
		phase.add(v)
	for size in base.counts:
		countQueue=base.counts[size]
		(start,end)=countQueue.eleIndexs()
		#print "size",size,"start",start,"end",end,"index",countQueue.index
		#for i in range(start,end,1):
		#	print countQueue.vs[i]
	ns=[100,234,590,1800]
	phase.updateSize()
	for n in ns:
		cc=base.combine(n)
		print n,cc,"a1",cc.a1()
		print n,phase.sum(n),phase.avg(n),phase.a1(n)
	print "rangev",phase.base.rangeV(100,10)
	print base.phases

if __name__=="__main__":
	if sys.argv[1] == "testPhase":
		testPhase()
	elif sys.argv[1] == "waveType":
		testWaveType()
	elif sys.argv[1] == "phaseDiv":
		size=120
		if len(sys.argv) > 2:
			size=int(sys.argv[2])
		testPhaseDiv(size)
	elif sys.argv[1] == "testBase":
		testBase()
	else:
		c=Count()
		c.size=5
		c1=copy.deepcopy(c)
		print c.size
		print c1.size
		c.sum=0.5
		c.__init__(c.size)
		print c.sum


