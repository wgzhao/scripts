#encoding=utf-8

#连接请求日志和响应日志

import sys

sys.path.append("./")
sys.path.append("../")
sys.path.append("../mlog")
sys.path.append("../abtest")

import column
import mlog
import random
import column2

def qid(f):
	lastQLine=""
	for line in f:
		line=line.strip()
		if line.startswith('{"status":'):
			q=column.getQid2(line,"request_id")
			if q != None and q != "":
				print q+"\t"+line
		elif line.startswith('{"remote_addr":"'):
			ip=mlog.getU(line)
			time=mlog.value(line,'"log_time":')
			if ip != "" and ip != None:
				print str(random.randint(0,99))+"\t"+ip+"\t"+time+"\t"+line.strip()
		else:
			cols=line.split("\t")
			if not column.valid(cols):
				continue
			q=column.getValue(cols[column.PARA_ID],"request_id")
			if q != None and q != "":
				print q+"\t"+line
			else:
				print str(random.randint(0,99))+"\t"+line

def outputQ(method,q,ps):
	if len(ps) == 1:
		print q+"\t"+ps[0]
	elif len(ps) == 0:
		print q
	else:
		sys.stderr.write("multi requestid q:"+q+"\n")
		for p in ps:
			sys.stderr.write("multi requestid p:"+p+"\n")
		if method in column2.FuncMap:
			for p in ps:
				ret=column2.FuncMap[method](p)
				if ret != None:
					print q+"\t"+p
					break
		else:
			print q+"\t"+"\t".join(ps)

def output(method,qs,ps):
	for q in qs:
		outputQ(method,q,ps)

def qidReduce(f):
	lastId=""
	method=""
	qs=[]
	ps=[]
	for line in f:
		cols=line.strip().split("\t")
		if len(cols) < 2:
			continue
		id=cols[0]
		if lastId == "":
			lastId=id
		if lastId != id or len(id) <= 2:
			output(method,qs,ps)
			lastId=id
			qs=[]
			ps=[]
			method=""
		if len(id) <= 2 or len(cols) > 2:
			#uid=column.ActionUser(cols[1:])
			if len(cols) > 4:
				cols[0]=cols[column.IP_CID+1]
				q="\t".join(cols)
				method=cols[column.METHOD_CID+1]
			else:
				q="\t".join(cols[1:])
			qs.append(q)
		else:
			p=cols[1]
			ps.append(p)
	if lastId != "":
		output(method,qs,ps)

if __name__=="__main__":
	if len(sys.argv) >= 2:
		if sys.argv[1] == "qid":
			qid(sys.stdin)
		elif sys.argv[1] == "qidReduce":
			qidReduce(sys.stdin)
	else:
		qidReduce(sys.stdin)


