#encoding=utf-8

import sys
import loss
from conf import *
sys.path.append("../util")
import DictUtil

def t1():
	last=""
	for line in sys.stdin:
		line=line.strip()
		if last != "":
			print last+"\t"+line
		last=line.split()[0]
	if last != "":
		print last+"\t"+line

def t2():
	ds={}
	S=True
	ln=0
	for line in sys.stdin:
		line=line.strip()
		if line.startswith("if"):
			if line.startswith("ifs"):
				d=int(line[3:-4])
				S=True
			else:
				d=int(line[2:-4])
				S=False
			if d not in ds:
				ds[d]=[0,0]
		elif line.startswith("增长率:"):
			p=line.find("\t")
			v=float(line[len("增长率:"):p])
			if v < 0:
				ln+=1
			if S:
				ds[d][1]=v
			else:
				ds[d][0]=v
	for d in ds:
		print "%d\t%f\t%f"%(d,ds[d][0],ds[d][1])
	sys.stderr.write("loss month:%d\n"%(ln))

def t3():
	sum=0
	n=0
	vs=[]
	for line in sys.stdin:
		if line.startswith("增长率:"):
			p=line.find("\t")
			try:
				v=float(line[len("增长率:"):p])
			except:
				continue
			if abs(v) > 100:
				#print line
				continue
			#print v
			sum+=v
			n+=1
			vs.append(v)
	#sys.stderr.write("sum:%f\tn:%d\tavg:%f\n"%(sum,n,sum/n))
	(sum,avg,std,s0)=DictUtil.statis(vs)
	sys.stderr.write("sum:%.4f    loss:%d,%.2f    avg:%.2f%%    std:%.4f    avg/std:%.4f\n"%(sum,s0,float(s0)/len(vs),avg*100,std,avg/std))

def t4():
	v=1.0
	curve=loss.Loss(1.0)
	for line in sys.stdin:
		curve.addTime(5500)
		curve.addE(float(line.strip())*curve.now)
		print curve.now
	print curve

def t5():
	for line in sys.stdin:
		cols=line.strip().split()
		print float(cols[2])-float(cols[1])

def t6():
	import math
	nvg=0
	t=0
	div=100
	for line in sys.stdin:
		v=float(line.strip())
		t+=1
		nvg+=v
		if t % div == 0:
			print "%.4f"%(math.log(nvg/div))
			nvg=0
	if t % div != 0:
		print "%.4f"%(math.log(nvg/(t%div)))

def t7(tag="增长下降比:"):
	vs=[]
	for line in sys.stdin:
		p=line.find(tag)
		if p >= 0:
			p1=line.find("=",p+len(tag))
			if p1 < 0:
				p1=line.find(" ",p+len(tag))
			if p1 < 0:
				p1=line.find("\t",p+len(tag))
			v=float(line[p+len(tag):p1])
			vs.append(v)
	(sum,avg,std,s0)=DictUtil.statis(vs)
	sys.stderr.write("sum:%.4f    loss:%d,%.2f    avg:%.2f%%    std:%.4f    avg/std:%.4f\n"%(sum,s0,float(s0)/len(vs),avg*100,std,avg/(std+1e-64)))

def t8(tag="超出值",Rate=0.0):
	import loss
	import random
	print tag
	curve=loss.Loss(1.0)
	conf=Conf()
	conf.HandSize=1.0
	for line in sys.stdin:
		p=line.find(tag)
		if p >= 0:
			p1=line.rfind("\t")
			try:
				v=float(line[p1+1:-1])
			except:
				continue
			#if random.random() < Rate:
			#	continue
			if abs(v) < 1e-6:
				continue
			print v	
			#if v > 0.3:
			#	print "line:",line
			#v=0.01*v
			#if curve.now < 0:
			#	continue
			curve.addTime()
			e=curve.now*v
			curve.addE(e)
			curve.addWin(v,conf)
	conf.YearFold=1.0
	print curve.confStr(conf)

def t9(day,file):
	tt=0
	#20141114,09:00
	lastTime=0
	lastHour=0
	lastV=""
	vs=[]
	sum=0
	#print file
	for line in open(file):
		if tt < 3:
			tt+=1
			continue
		if len(line) < 14:
			continue
		try:
			time=line[9:14]
			ts=time.split(":")
			hour=int(ts[0])
			minute=int(ts[1])
			time=int(ts[0])*100+int(ts[1])
			#if hour == 13 and lastHour == 11:
			#	lastTime=time-1
		except:
			continue
		if lastTime==0:
			lastTime=time
		if time != lastTime:
			#if time -lastTime > 5:
			#	#print "%d:%d->%d:%d"%(lastTime/60,lastTime%60,time/60,time%60)
			#	sum+=(time-lastTime)
			#	if sum > 100:
			#		return []
			cols=line.strip().split(",")
			if len(cols) >= 4:
				#for t in range(lastTime+1,time,1):
				#	vs.append(str(t)+"\t"+lastV)
				lastTime=time
				lastHour=hour
				try:
					lastV=float(cols[2].strip())
				except:
					sys.stderr.write(file+"\n")
					sys.stderr.write(cols[2]+"\n")
					sys.stderr.write(line+"\n")
					continue
				if lastV <= 0:
					sys.stderr.write(file+"\n")
					sys.stderr.write(cols[2]+"\n")
					sys.stderr.write(line+"\n")
					continue
				try:
					vol=int(cols[3].strip())
				except:
					sys.stderr.write(file+"\n")
					sys.stderr.write(cols[3]+"\n")
					sys.stderr.write(line+"\n")
					continue
				if vol < 5:
					continue
				vs.append(day+"\t"+str(time)+"\t"+cols[2]+"\t"+str(vol))
	return vs

def t10():
	dir="/home/zhangzhonghui/log-mining/com/haodou/log-mining/earn/dce/DCE/"
	files=getFiles(dir)
	wfs={}
	for file in files:
		day=file[len(dir)+1:len(dir)+9]
		month=file[-8:-4]
		p1=file.rfind("/")
		item=file[p1:-4]
		if True:
			vs=t9(day,file)
			if item not in wfs:
				wfs[item]=open("./modiData/"+item+".txt","w")
			lastP=-1
			for tp in vs:
				tps=tp.split("\t")
				time=tps[1]
				p=float(tps[2])
				vol=int(tps[3])
				if (p <= 0) or (lastP > 0 and (lastP > 1.2*p or p > 1.2*lastP)):
					sys.stderr.write(file+"\n")
					sys.stderr.write("day="+day+"\ttime="+time+"\tp="+str(p)+"\tlastP="+str(lastP)+"\n")
					break
				lastP=p
				wfs[item].write(tp+"\n")
	for item in wfs:
		wfs[item].close()

def t11():
	sa={}
	sb={}
	sc={}
	for line in sys.stdin:
		cols=line.strip().split()
		name=cols[0]
		day=cols[1]
		m=cols[2]
		v=float(cols[3])
		t=cols[4]
		if name not in sa:
			sa[name]={}
			sb[name]={}
			sc[name]={}
		if t not in sa[name]:
			sa[name][t]=[1e-12,0]
			sb[name][t]=[1e-12,0]
			sc[name][t]=[1e-12,0]
		if day < '20141230':
			sa[name][t][0]+=1
			sa[name][t][1]+=v
		elif day < '20150310':
			sb[name][t][0]+=1
			sb[name][t][1]+=v
		else:
			sc[name][t][0]+=1
			sc[name][t][1]+=v
	for name in sa:
		for t in sa[name]:
			va=sa[name][t][1]/(sa[name][t][0])
			vb=sb[name][t][1]/sb[name][t][0]
			vc=sc[name][t][1]/sc[name][t][0]
			print "%s\t%s\t%f\t%f\t%f"%(name,t,va,vb,vc)

if __name__=="__main__":
	if len(sys.argv) < 2:
		t3()
	elif sys.argv[1] == "t2":
		t2()
		#t1()
	elif sys.argv[1] == "t4":
		t4()
	elif sys.argv[1] == "t5":
		t5()
	elif sys.argv[1] == "t6":
		t6()
	elif sys.argv[1] == "t10":
		t10()
	elif sys.argv[1] == "t8":
		rate=0.0
		t="超出值"
		if len(sys.argv) >=3:
			try:
				rate=float(sys.argv[2])
			except:
				t=sys.argv[2]
		t8(tag=t,Rate=rate)
	elif sys.argv[1] == "t7":
		if len(sys.argv) >= 3:
			t7(sys.argv[2])
		else:
			t7()

	elif sys.argv[1] == "t9":
		t9(sys.argv[2])
	elif sys.argv[1] == "tt9":
		vs=t9(sys.argv[2])
		for v in vs:
			print v
	elif sys.argv[1] == "t11":
		t11()



