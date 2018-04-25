#encoding=utf-8

import sys
sys.path.append("./")
sys.path.append("../")
sys.path.append("../util")

from regSeq     import *
import column
import DictUtil

hits=readMethod(open("./hitMethod.txt"))

def distribute():
	vs={}
	ms={}
	cs={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < column.APP_LOG_COLUMNS:
			continue
		version=cols[column.VERSION_CID]
		channel=cols[column.MEDIA_CID]
		method=cols[column.METHOD_CID]
		uid=column.uid(cols)
		if uid == "":
			uid=None
		if method in regMs and uid == None:
			DictUtil.addOne(vs,version)
			DictUtil.addOne(ms,method)
			DictUtil.addOne(cs,channel)
	for v in vs:
		print "%s\t%d"%(v,vs[v])
	for m in ms:
		print "%s\t%d"%(m,ms[m])
	for c in cs:
		print "%s\t%d"%(c,cs[c])

if __name__=="__main__":
	distribute()




