import sys

sys.path.append("./")
sys.path.append("../")

from column import *
import re

def starts(fms,method):
	for fm  in fms:
		if method.startswith(fm):
			return True
	return False

def count(f,methods,followMethod,followPara):
	fms=followMethod.split("#")
	us={}
	for line in f:
		cols=line.strip().split("\t")
		if len(cols) < APP_LOG_COLUMNS:
			continue
		v=bigVersion(cols[VERSION_CID])
		if v != "3" and v != "4":
			continue
		u=uuid(cols)
		if u == None or u == "" or u == "null":
			continue
		method=cols[METHOD_CID]
		if starts(fms,method):
			#sys.stderr.write(method+"\n")
			parav=getValue(cols[PARA_ID],followPara)
			if u in us:
				p=us[u]
				print p[0]+"\t"+p[1]+"\t"+u+"\t"+p[2]+"\t"+parav
		elif method in methods:
			lastMethod=method
			paras=methods[method]
			lastPara=""
			requestId=getValue(cols[PARA_ID],"request_id")
			lastQid=""
			for para in paras:
				if len(para) == 0:
					continue
				parav=getValue(cols[PARA_ID],para)
				if parav != None and parav != "":
					lastQid+="-"+parav
					if paras[para]=="bool":
						parav=para+"_ok"
					lastPara+="-"+parav
				else:
					lastPara+="-"
			#if u in us:
				#p=us[u]
				#print p[0]+"\t"+p[1]+"\t"+u+"\t"+p[2]
			if method.startswith("search.getlist"):
				requestId=u+lastQid
			print lastMethod+"\t"+lastPara+"\t"+u+"\t"+requestId
			us[u]=(lastMethod,lastPara,requestId)
			
if __name__=="__main__":
	sys.path.append("../log/")
	from readParaConf import *
	methods=readParaSetting(sys.argv[1])
	followMethod=sys.argv[2]
	followPara=sys.argv[3]
	count(sys.stdin,methods,followMethod,followPara)

