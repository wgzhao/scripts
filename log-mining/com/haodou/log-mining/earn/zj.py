#encoding=utf-8

import sys

from conf import *

class ZJ(object):
	def __init__(self,day,time,kai):
		self.day=day
		self.time=time
		self.kai=kai

	def fullTime(self):
		t=self.day*1440+self.time/100*60+self.time%100
		return t

def d2s(d):
	s=""
	items=sorted(d.items(),key=lambda e:e[0])
	for k,v in items:
		s+="\t"+str(k)+":"+str(v)
	return s

def parseLine(line):
	cols=line.strip().split(" ")
	day=int("".join(cols[0].split("/")))
	time=int("".join(cols[1].split(":")))
	kai=float(cols[2])
	zj=ZJ(day,time,kai)
	return zj

#波动映射到tag
def r2t(r):
	if r > NoBidFold*Rate/GangRate:return 1
	elif r < -NoBidFold*Rate/GangRate:return -1
	else:return 0

def getFea(f,weitFile=None):
	wf=None
	if weitFile != None:
		wf=open(weitFile,"w")
	last=None
	q = []
	nn=0
	ws={}
	fid={}
	fid["wn"]=len(fid)+1
	fid["div"]=len(fid)+1
	fid["avgDiv"]=len(fid)+1
	fid["dx"]=len(fid)+1
	for i in range(Size/PSize+1):
		fid["pcn_%d"%(i)]=len(fid)+1
	for i in range(TSize+1):
		fid["tcn_%d"%(i)]=len(fid)+1
	for line in f:
		nn+=1
		if nn <= 1:
			continue
		zj=parseLine(line)
		has=False
		if len(q) >= Size+MidSize:
			fs={}
			last=q[len(q)-MidSize]
			wt=(zj.kai-last.kai)/last.kai
			ct=r2t(wt)
			wn=1
			dx=0
			PC=0
			TC=0
			sum=0
			for i in range(0,len(q)-MidSize,1):
				if i %PSize == PSize-1: #分段波动
					t=(q[i+1].kai-q[i+1-PSize].kai)/40.0
					fi="PSize_%d"%(last.fullTime()-q[i].fullTime())
					if fi not in fid:
						fid[fi]=len(fid)+1
					fs[fid[fi]]=t
					if t > 0:
						PC+=1
				sum+=q[i].kai	
				t=(q[i+1].kai-q[i].kai)/20.0
				if t > 0:
					wn+=1
					dx+=t
				else:
					dx-=t
				if i >= len(q)-MidSize-TSize:
					if t > 0:
						TC+=1
				fi=last.fullTime()-q[i].fullTime()
				if fi not in fid:
					fid[fi]=len(fid)+1
				#fs[fid[fi]]=t
			avgDiv=last.kai-sum/Size
			fs[fid["avgDiv"]]=(last.kai-sum/Size)/5.0
			#fs[fid["pcn_%d"%(PC)]]=1.0
			#fs[fid["tcn_%d"%(TC)]]=1.0
			fs[fid["wn"]]=(wn/float(Size)-0.5)*math.pow(Size,0.5)
			fs[fid["div"]]=(last.kai-q[0].kai)/float(Size)
			#fs[fid["dx"]]=dx/float(Size)  #震动幅度
			s=d2s(fs)
			print "%d%s"%(ct,s)
			if wf != None:
				wf.write("%.6f%s\n"%(wt,s))
			del q[0]
		q.append(zj)
	if wf != None:
		wf.close()
	#for wn in ws:
	#	print wn,ws[wn]


if __name__=="__main__":
	if len(sys.argv) >=2:
		getFea(sys.stdin,sys.argv[1])
	else:
		getFea(sys.stdin)
	


