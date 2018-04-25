import sys

sys.path.append("../")
sys.path.append("./")
sys.path.append("../abtest")

import column
import removeItem

def readKeys(f):
	keys={}
	for line in f:
		cols=line.strip().split()
		if len(cols) < 2:
			continue
		name=cols[0]
		head=cols[1]
		hs=head.split(".")
		keys[name]=hs
	return keys

def value(hs,a):
	if len(hs) == 0:
		return a
	for h in hs:
		if type(a) == dict and h in a:
			return value(hs[1:],a[h])
		elif type(a) == list:
			rlist=[]
			for ai in a:
				ret=value(hs,ai)
				if ret != None:
					rlist.append(ret)
			if len(rlist) > 1:
				return rlist
			elif len(rlist) == 1:
				return rlist[0]
	return None

def extract(f):
	keys=readKeys(open("responseItem.txt"))
	for line in f:
		cols=line.strip().split("\t")
		if len(cols) < column.APP_LOG_COLUMNS4+2:
			continue
		response=cols[-1]
		a=removeItem.parse(response)
		if a == None:
			continue
		u=cols[0]
		time=cols[1]
		method=cols[column.METHOD_CID+1]
		for	name in keys:
			v=value(keys[name],a)
			if v != None:
				print u+"\t"+time+"\t"+str(v)+"\t"+method

if __name__=="__main__":
	extract(sys.stdin)

