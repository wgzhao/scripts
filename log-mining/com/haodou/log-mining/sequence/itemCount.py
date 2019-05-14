#encoding=utf-8

import sys
sys.path.append("./")
sys.path.append("../")

import column
import sequence
import getActionItem
import hitItemName

def parseLine(line,type="appLog"):
	cols=line.strip().split("\t")
	if type == "userSort":
		(action,user)=sequence.getActionUser(cols,0)
		if action == None:
			return (None,None)
		if user == None:
			return (None,None)
		item=hitItemName.getPosName(action)
		return (item,user.name())
	elif type == "appLog":
		if not column.valid(cols):
			return (None,None)
		method=cols[column.METHOD_CID]
		para=cols[column.PARA_ID]
		version=column.intVersion(cols[column.VERSION_CID])
		item=getActionItem.getActionItem(method,para,version)
		name=hitItemName.getMethodName(method,para)
		offset=column.getValue(para,"offset")
		if offset == "0":
			name+=hitItemName.SplitSign+"0"
		item=hitItemName.packItem(name,item)
		uuid=column.uuid(cols)
		return (item,uuid)
	return (None,None)

def parse(f,type="appLog"):
	for line in f:
		(item,u)=parseLine(line,type)
		if item == None or u == None:
			continue
		print "%s\t%s"%(item,u)
		s=0
		p=item.rfind(hitItemName.SplitSign)
		while p > 0:
			item=item[0:p]
			print "%s\t%s"%(item,u)
			p=item.rfind(hitItemName.SplitSign)

def output(lastItem,us):
	cs={}
	n=0
	for u in us:
		c=us[u]
		n+=c
		if c not in cs:
			cs[c]=1
		else:
			cs[c]+=1
	print "%s\t%d\t%d\t%.4f\t%s"%(lastItem,n,len(us),float(n)/len(us),str(cs))

def count(f):
	lastItem=""
	us={}
	for line in f:
		cols=line.strip().split("\t")
		if len(cols) < 2:
			continue
		(item,u)=cols[0:2]
		if lastItem == "":
			lastItem=item
		if lastItem != item:
			output(lastItem,us)
			lastItem=item
			n=0
			un=0
			us={}
		if u not in us:
			us[u]=1
		else:
			us[u]+=1
	if lastItem != "":
		output(lastItem,us)

if __name__=="__main__":
	if sys.argv[1] == "parse":
		if len(sys.argv) >= 3:
			parse(sys.stdin,sys.argv[2])
		else:
			parse(sys.stdin)
	elif sys.argv[1] == "count":
		count(sys.stdin)

	
