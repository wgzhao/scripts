#encoding=utf-8

import sys

sys.path.append("./")

from timeUtil import *
import math

def readR():
	rinfo={}  #菜谱质量,标题,Rate
	y=getY()
	m=getM()
	d=getD()
	#23      彩椒鸡片        2.91747757037   2009-05-31 16:26:13     2.0
	for line in open("quality.txt"):
		cols=line.strip().split("\t")
		rid=cols[0]
		title=cols[1]
		q=float(cols[2])
		t=1.0/(3.0+math.log(dayDiv(cols[3],y,m,d)+2.0))
		rate=cols[4]
		rinfo[rid]=(title,q*t,rate,t)
	return rinfo

def cover(dret,rinfo):
	maxRate=0.0
	max=-1
	maxR=None
	for r in dret:
		rate=float(rinfo[r][2])
		if rate > 4.0:
			rate=4.0
		if rate >= maxRate:
			v=dret[r]
			if v > max or rate > maxRate:
				max=v
				maxR=r
			if rate > maxRate:
				maxRate=rate
	return maxR

SampleNum=50

import random
import operator


def div(title,divers):
	all=1.0
	cn=0.0
	max=0
	for s in title:
		if s in divers:
			if divers[s]>max:
				max=divers[s]
			cn+=1.0
		else:
			all=0.0
	d=1.0/math.pow(1.1+0.15*cn+all,max)
	#sys.stdout.write(title.encode("utf-8")+"\t"+str(all)+"\t"+str(max)+"\t"+str(d)+"\n")
	return d


def sampleWithDiversity(rs,N,rinfo):
	if len(rs) <= 0:
		return rs
	m=max([rs[e] for e in rs])
	keys=[e for e in rs]
	values=[rs[e]/(m*1.1+1e-64) for e in rs]
	divers={}
	ret={}
	n=2*min(N,len(rs))
	for i in range(n):
		#print len(ret),i,n,N,len(rs)
		if len(ret) >= min(N,len(rs)):
			break
		k=random.randint(0,len(keys)-1)
		maxk=-1
		maxv=0
		for j in range(20):
			key=keys[k]
			if key in ret:
				continue
			title=rinfo[key][0].decode("utf-8")
			v=values[k]*div(title,divers)
			if maxv < v:
				maxv=v
				maxk=k
			if v < random.random():
				k=random.randint(0,len(keys)-1)
				continue
			else:
				maxk=k
				break
		if maxk != -1:
			key=keys[maxk]
			ret[key]=rs[key]
			title=rinfo[key][0].decode("utf-8")
			#print rinfo[key][0]
			for s in title:
				if s not in divers:
					divers[s]=1
				else:
					divers[s]+=1
	return ret
	 

def output(rs,lastU,rinfo):
	#lastU=lastU[0:4]
	dret=sampleWithDiversity(rs,SampleNum,rinfo)
	#print ""
	c=cover(dret,rinfo)
	if c != None:
		#print lastU+"\t"+c+"\t"+str(dret[c])+"\t"+rinfo[c][0]+"\t"+rinfo[c][2]
		lastU += "\t"+c
		del dret[c]
	sorted_ret = sorted(dret.iteritems(), key=operator.itemgetter(1),reverse=True)
	for (r,v) in sorted_ret:
		lastU += "\t"+r
	print lastU
	#for (r,v) in sorted_ret:
	#	print lastU+"\t"+r+"\t"+str(v)+"\t"+rinfo[r][0]+"\t"+rinfo[r][2]

def sample():
	rinfo=readR()
	lastU=""
	rs={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		u=cols[0]
		if lastU == "":
			lastU=u
		if lastU != u:
			output(rs,lastU,rinfo)
			lastU=u
			rs={}
		rid=cols[1]
		v=float(cols[2])
		if rid in rinfo:
			rs[rid]=v*rinfo[rid][1]
	if lastU != "":
		output(rs,lastU,rinfo)

def testReadR():
	rinfo=readR()
	for r in rinfo:
		print r,rinfo[r][1],rinfo[r][3],rinfo[r][4]

if __name__=="__main__":
	#testReadR()
	sample()


