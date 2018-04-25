#encoding=utf-8

import sys

sys.path.append("./")
sys.path.append("log")

import column

def readCates(file):
	tags={}
	for line in open(file):
		cols=line.strip().split("\t")
		if len(cols) < 2:
			continue
		tags[cols[1]]=cols[0]
	return tags

def countSearch(file):
	tags=readCates(file)
	for line in sys.stdin:
		if line.find("search.getlist") < 0:
			continue
		cols=line.strip().split("\t")
		if not column.valid(cols):
			continue
		scene=column.getValue(cols[column.PARA_ID],"scene")
		if scene == "t2":
			continue
		tagid=column.getValue(cols[column.PARA_ID],"tagid")
		if tagid != None and tagid != "" and scene == "t1":
			print tagid
			continue
		keyword=column.getValue(cols[column.PARA_ID],"keyword")
		if keyword != None and keyword in tags:
			print tags[keyword]

def reduce():
	lastTid=""
	n=0
	for line in sys.stdin:
		tid=line.strip()
		if lastTid == "":
			lastTid=tid
		if tid != lastTid:
			print "%s\t%d"%(lastTid,n)
			lastTid=tid
			n=0
		n+=1
	if lastTid != "":
		print "%s\t%d"%(lastTid,n)

if __name__=="__main__":
	if len(sys.argv) >= 2:
		if sys.argv[1] == "reduce":
			reduce()
		else:
			countSearch(sys.argv[1])
	else:
		countSearch("./util/cateidName.txt")


