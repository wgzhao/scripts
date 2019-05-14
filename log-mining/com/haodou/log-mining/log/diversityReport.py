#encoding=utf-8

import sys


dirname="./"
if len(sys.argv) >= 2:
	dirname=sys.argv[1]
fix="diversity.2014"
if len(sys.argv) >= 3:
	fix=sys.argv[2]
import os
def count():
	files=os.listdir(dirname)
	ps={}
	for file in files:
		if file.startswith(fix):
			month=int(file[-5:-3])
			day=int(file[-2:])
			day=month*100+day
			lastLine=""
			for line in open(dirname+"/"+file):
				lastLine=line.strip()
			ps[day]=eval(lastLine)
	sortList=[(k,ps[k]) for k in sorted(ps.keys())]
	return sortList

def printDays(sortList):
	for day,vs in sortList:
		s= "日期"
		for col in vs:
			s+= "\t"+col
		print s
		break
	for day,vs in sortList:
		s="2014-"+str(day/100)+"-"+str(day%100)
		for col in vs:
			s+="\t"+str(vs[col])
		print s

if __name__=="__main__":
	printDays(count())


