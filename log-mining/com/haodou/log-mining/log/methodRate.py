#encoding=utf-8

import sys
import os

f=open(sys.argv[1])
names={}
for line in f:
	m=line.strip()
	if len(m) > 0:
		names[m]=1

files=os.listdir("./")
ps={}
for file in files:
	if file.startswith("mc.2014-07-"):
		day=int(file[-2:])
		ps[day]={}
		for line in open(file):
			cols=line.strip().split("\t")
			if len(cols) < 2:
				continue
			m=cols[0]
			has=False
			for name in names:
				if m.startswith(name):
					has=True
			if not has:
				continue
			ps[day][m]=int(cols[1])
s="日期"
for name in names:
	s+="\t"+name+"\t"+"比例"
s+="\t"+"总量"+"\t"+"增长率"
print s
lastsum=0
for i in range(10,40):
	if i not in ps:
		continue
	ms=ps[i]
	if len(ms) > 0:
		sum=0
		for m in ms:
			sum+=ms[m]
		s=""
		for m in names:
			n=0
			if m.endswith("(all)"):
				m=m[0:-5]
				for k in ms:
					if k.startswith(m):
						n+=ms[k]
			else:
				for k in ms:
					if k==m or (k.startswith(m) and k not in names):
						n+=ms[k]
			s+="\t"+str(n)+"\t"+"%.3f"%(n/(sum+1e-16))
		print "2014-07-"+str(i)+s+"\t"+str(sum)+"\t"+"%.3f"%(sum/(lastsum+1e-16)-1.0)
		lastsum=sum	

