#encoding=utf-8

import sys
sys.path.append("./")
sys.path.append("../")
import column

def do(userFile):
	us={}
	vms={}
	for line in open(userFile):
		us[line.strip()]=1
	for line in sys.stdin:
		cols=line.strip().split("\t")
		uid=column.uid(cols)
		if uid == None or uid == "":
			continue
		if uid not in us:
			continue
		v=cols[column.VERSION_CID]
		v=column.intVersion(v)
		method=cols[column.METHOD_CID]
		if v not in vms:
			vms[v]={}
		if method not in vms[v]:
			vms[v][method]=1
		else:
			vms[v][method]+=1
	for v in vms:
		for method in vms[v]:
			print "%d\t%s\t%d"%(v,method,vms[v][method])

if __name__=="__main__":
	do(sys.argv[1])

