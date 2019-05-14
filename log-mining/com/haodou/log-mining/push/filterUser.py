#encoding=utf-8

import sys

def output(lastU,us,has):
	if has:
		for u in us:
			print u+"\t"+us[u]
		if len(us) == 0:  #因为原来的代码会把新用户忽略
			print lastU+"\t0"

def filter():
	lastU=""
	us={}
	has=False
	for line in sys.stdin:
		cols=line.strip().split("\t")
		u=cols[0]
		if lastU == "":
			lastU=u
		if lastU != u:
			output(lastU,us,has)
			lastU=u
			has=False
			us={}
		if len(cols) < 2:
			has=True
		else:
			us[u]=cols[1]
	if lastU != "":
		output(lastU,us,has)

if __name__=="__main__":
	filter()

