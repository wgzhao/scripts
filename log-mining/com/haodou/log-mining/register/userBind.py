#encoding=utf-8

import sys
from userReg import *


def bind2(f):
	b2={}
	for line in f:
		cols=line.strip().split()
		u=cols[0]
		v=1
		if len(cols) == 3:
			v=2
		if u not in b2:
			b2[u]=v
		else:
			if b2[u] != v and b2[u] < 3:
				b2[u]+=v
	for u in b2:
		if b2[u] == 3:
			#print u
			pass
		elif b2[u] > 3:
			sys.stderr.write("error:"+u+"\t"+str(b2[u])+"\n")
	return b2

def readB2(f):
	b22={}
	for line in f:
		b22[line.strip()]=1
	return b22

def rate(b2,uds):
	ds={}
	for u in uds:
		i=0
		if u in b2:
			i=b2[u]
		for d in uds[u]:
			if d not in ds:
				ds[d]=[0,0,0,0]
			ds[d][i]+=1
			
	for d in ds:
		sum=1e-32
		for i in range(4):
			sum+=ds[d][i]
		print "%s\t%d\t%d\t%d\t%d\t%.4f\t%.4f"%(d,ds[d][0],ds[d][1],ds[d][2],ds[d][3],ds[d][0]/sum,ds[d][3]/(sum))
			
def regRate(f,b22,uds):
	ds={}
	for line in f:
		cols=line.strip().split("\t")
		(u,phone,reg)=cols
		if u not in b22 or (u not in uds):
			continue
		reg=reg[0:7]
		#if reg <= "2014-08":
		#	reg="<=2014-08"
		#else:
		#	reg=">=2014-09"
		for d in uds[u]:
			if d not in ds:
				ds[d]={}
			if reg not in ds[d]:
				ds[d][reg]=1
			else:
				ds[d][reg]+=1
	for d in ds:
		sum=1e-32
		for reg in ds[d]:
			sum+=ds[d][reg]
		for reg in ds[d]:
			print "%s\t%s\t%d\t%.4f"%(d,reg,ds[d][reg],ds[d][reg]/sum)

def refineTime(time):
	if time > "2014-08":
		return ">=2014-09"
	else:
		return "<=2014-08"

def Bind2():
	qs={}
	for line in open("/home/zhangzhonghui/data/userConnect.qunachi.txt"):
		cols=line.strip().split("\t")
		if len(cols) < 3:
			sys.stderr.write(line)
			continue
		(u,openid,time)=cols
		time=time[0:7]
		if u not in qs:
			qs[u]=["",time]
		else:
			if time > qs[u][1]:
				qs[u][1]=time
	for line in open("/home/zhangzhonghui/data/userConnect.recipe.txt"):
		cols=line.strip().split("\t")
		if len(cols) < 3:
			sys.stderr.write(line)
			continue
		(u,openid,time)=cols
		if u not in qs:
			continue
		time=time[0:7]
		if qs[u][0] == "" or qs[u][0] < time:
			qs[u][0]=time
	ts={}
	for u in qs:
		if qs[u][0] == "":
			continue
		t1=refineTime(qs[u][0])
		t2=refineTime(qs[u][1])
		t=t1+"\t"+t2
		if t not in ts:
			ts[t]=1
		else:
			ts[t]+=1
	sum=1e-32
	for t in ts:
		sum+=ts[t]
	for t in ts:
		print "%s\t%d\t%.3f"%(t,ts[t],ts[t]/sum)

if __name__=="__main__":
	b2=bind2(open("/home/zhangzhonghui/data/userOpenid.txt"))
	#b22=readB2(open("/home/zhangzhonghui/data/userOpenid.b2.txt"))
	uds=readUser(open("/home/zhangzhonghui/data/shqu.txt"))
	rate(b2,uds)
	#regRate(open("/home/zhangzhonghui/data/userPhone.txt"),b22,uds)
	#Bind2()

