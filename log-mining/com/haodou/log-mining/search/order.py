#encoding=utf-8

import sys
sys.path.append("./")
sys.path.append("../")
import column

orderMap={}
orderMap["0"]="默认"
orderMap["1"]="最新"
orderMap["2"]="推荐"
orderMap["3"]="热门"
orderMap[""]="[空]"

def count():
	orders={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < column.APP_LOG_COLUMNS:
			continue
		method=cols[column.METHOD_CID]
		if method != "search.getlistv3":
			continue
		para=cols[column.PARA_ID]
		offset=column.getValue(para,"offset")
		if offset != "0":
			continue
		order=column.getValue(para,"order")
		if order not in orders:
			orders[order]=1
		else:
			orders[order]+=1
	ns={}
	for order in orders:
		n=order
		if order in orderMap:
			n=orderMap[order]
		ns[n]=orders[order]
	for n in ns:
		print n+"\t"+str(ns[n])

def reduce():
	alls={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		order=cols[0]
		v=int(cols[1])
		if order not in alls:
			alls[order]=v
		else:
			alls[order]+=v
	for n in alls:
		print n+"\t"+str(alls[n])

if __name__=="__main__":
	if sys.argv[1] == "map":
		count()
	else:
		reduce()



