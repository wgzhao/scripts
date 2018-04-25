#encoding=utf-8

import sys

def formatOutput(lastU,us):
	s=lastU
	for u in us:
		s+="\t%s"%(u)
	print s

#
#事先排序
#
def format():
	lastU=""
	us=[]
	for line in sys.stdin:
		cols=line.strip().split("\t")
		u=cols[0]
		if u.endswith("L"):
			u=u[0:-1]
		u1=cols[1]
		if u1.endswith("L"):
			u1=u1[0:-1]
		if lastU == "":
			lastU=u
		if lastU != u:
			formatOutput(lastU,us)
			lastU=u
			us=[]
		us.append(u1)
	if lastU != "":
		formatOutput(lastU,us)

#
#
if __name__=="__main__":
	format()  #cat ../readDB/user.follow.txt | sort | python follow.py > userFollow.txt

