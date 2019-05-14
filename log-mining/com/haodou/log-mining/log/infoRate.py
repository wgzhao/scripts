#encoding=utf-8

import sys
import os

import readParaConf
names=readParaConf.readParaSetting(sys.argv[1])

dimension=sys.argv[2]

dirname="./"
if len(sys.argv) >= 4 and len(sys.argv[3]) > 0:
	dirname=sys.argv[3]
files=os.listdir(dirname)

fix="methodInfo"
if len(sys.argv) >= 5 and len(sys.argv[4]) > 0:
	fix=sys.argv[4]

ps={}
for file in files:
	if file.startswith(fix+".2014-"):
		month=int(file[-5:-3])
		day=int(file[-2:])
		day=month*100+day
		ps[day]={}
		for line in open(dirname+"/"+file):
			cols=line.strip().split("\t")
			if len(cols) < 2:
				continue
			m=cols[0]
			has=False
			for name in names:
				nn=name
				if name.endswith("(tot)"):
					nn=name[0:-5]
				if m.startswith(nn):
					has=True
			if not has:
				continue
			ps[day][m]=eval(cols[1])
			avgStr=ps[day][m]["avg"]
			if avgStr == "":
				avgStr="0"
				ps[day][m]["avg"]=float(avgStr)

def getv(dimension,tjs):
	dtj={}
	dtj["sum"]=0
	dtj["csum"]=0
	dtj["num"]=0
	for tj in tjs :
		dtj["sum"]+=tj["sum"]
		if "csum" not in tj:
			dtj["csum"]+=tj["sum"]
		else:
			dtj["csum"]+=tj["csum"]
		if "userNum" in tj:
			dtj["num"]+=tj["userNum"]
		else:
			dtj["num"]+=tj["num"]
	dtj["avg"]=dtj["sum"]/(dtj["num"]+1e-6)
	dtj["cavg"]=dtj["csum"]/(dtj["num"]+1e-6)
	return dtj[dimension]

s="日期"
for name in names:
	s+="\t"+name+"\t"+"比例"
s+="\t"+"总量"+"\t"+"增长率"
print s
lastsum=0
for i in range(1,1300):
	if i not in ps:
		continue
	ms=ps[i]
	if len(ms) > 0:
		s=""
		sum=0
		mtjs={}
		for m in names:
			tjs=[]
			if m.endswith("(all)") or m.endswith("(tot)"):
				tm=m[0:-5]
				for k in ms:
					if k.startswith(tm):
						tjs.append(ms[k])
				if m.endswith("(tot)"):
					sum+=getv(dimension,tjs)
			else:
				for k in ms:
					if k==m:
						tjs.append(ms[k])
				sum+=getv(dimension,tjs)
			mtjs[m]=tjs
		for m in names:
			v=getv(dimension,mtjs[m])
			s+="\t"+str(v)+"\t"+"%.3f"%(v/(sum+1e-16))
		print "2014-"+str(i/100)+"-"+str(i%100)+s+"\t"+str(sum)+"\t"+"%.3f"%(sum/(lastsum+1e-16)-1.0)
		lastsum=sum	

