#encoding=utf-8

import sys
import os

files=os.listdir("/home/zhangzhonghui/data/reg/")
files=sorted(files)
ds={}
vs={}
for file in files:
	if not file.startswith("regCount."):
		continue
	day=file[len("regCount."):]
	if day not in ds:
		ds[day]={}
	for line in open("/home/zhangzhonghui/data/reg/"+file):
		cols=line.strip().split()
		v=cols[0]
		n=int(cols[1])
		ds[day][v]=n
		if n > 1000:
			vs[v]=1

s="日期"
for v in vs:
	s+="\t"+v
print s

for day in ds:
	s=day
	for v in vs:
		n=0
		if v in ds[day]:
			n=ds[day][v]
		s+="\t%d"%(n)
	print s

