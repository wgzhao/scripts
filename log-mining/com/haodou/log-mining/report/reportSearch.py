#encoding=utf-8
import sys

ts={}
for line in sys.stdin:
	if line.startswith("##Card##_"):
		continue
	if line.startswith("ck4"):
		continue
	if line.startswith("v45_"):
		line=line[4:]
	cols=line.strip().split("\t")
	try:
		w=cols[0]
		s=int(cols[1])
		v=int(cols[2])
		if w not in ts:
			ts[w]=[0,0]
		else:
			ts[w][0]+=s
			ts[w][1]+=v
	except:
		sys.stderr.write(line)
		continue

print "搜索词\t搜索次数\t有点击次数"
for w in ts:
	if ts[w][0] >= 100:
		print "%s\t%d\t%d"%(w,ts[w][0],ts[w][1])


