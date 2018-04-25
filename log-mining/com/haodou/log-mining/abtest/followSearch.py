import sys

sys.path.append("./")
sys.path.append("../")

from column import *
import re

def starts(fms,method,para):
	for fm  in fms:
		ss=fm.split("@")
		m=ss[0]
		if method.startswith(m):
			if len(ss) <= 1:
				return True
			else:
				ok=True
				for i in range(1,len(ss),1):
					(a,b)=ss[i].split(":")
					v=getValue(para,a)
					if b == "boolfalse":
						if v != None and v != "":
							ok=False
							break
					elif b == "bool":
						if v == "" or v == None:
							ok=False
							break
					else:
						if b != v:
							ok=False
							break
				return ok
	return False

def count(f,methods,followMethod,followPara):
	fms=followMethod.split("#")
	us={}
	for line in f:
		cols=line.strip().split("\t")
		if len(cols) < APP_LOG_COLUMNS:
			continue
		version=bigVersion(cols[VERSION_CID])
		if version != "4":
			continue
		u=uuid(cols)
		if u == None or u == "" or u == "null":
			continue
		method=cols[METHOD_CID]
		if starts(fms,method,cols[PARA_ID]):
			#sys.stderr.write(method+"\n")
			parav=getValue(cols[PARA_ID],followPara)
			if u in us:
				p=us[u]
				print p[0]+"\t"+p[1]+"\t"+u+"\t"+p[2]+"\t"+parav
			else:
				print "VoidMethod\tVoidPara\t+"+u+"\tVoidRequestId\t"+parav
		elif method in methods:
			lastMethod=method
			paras=methods[method]
			lastPara=""
			requestId=getValue(cols[PARA_ID],"request_id")
			for para in paras:
				if len(para) == 0:
					continue
				parav=getValue(cols[PARA_ID],para)
				if parav != None and parav != "":
					if paras[para]=="bool":
						parav=para+"_ok"
					lastPara+="-"+parav
				else:
					lastPara+="-"
			if u in us:
				p=us[u]
				#print p[0]+"\t"+p[1]+"\t"+u+"\t"+p[2]
			us[u]=(lastMethod,lastPara,requestId)
			
if __name__=="__main__":
	sys.path.append("../log/")
	from readParaConf import *
	methods=readParaSetting(sys.argv[1])
	followMethod=sys.argv[2]
	followPara=sys.argv[3]
	count(sys.stdin,methods,followMethod,followPara)

