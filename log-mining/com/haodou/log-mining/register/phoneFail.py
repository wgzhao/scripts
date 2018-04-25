#encoding=utf-8

import sys
import re
sys.path.append("./")
sys.path.append("../")
import column

sys.path.append("../abtest")

from regSeq     import *
import column2


PositiveNumP=re.compile("[1-9][0-9]*")
def addFea(fea):
	if fea not in lms:
		lms[fea]=1
	else:
		lms[fea]+=1

def output(lastUuid,ms,regFirstDo,phone):
	if phone == "":
		return
	if len(phone) > 3:
		phone=phone[0:3]
	addFea(phone)
	if regFirstDo == "NO":
		addFea(phone+"_NO")
	
def count():
	lastUuid=""
	regFirstDo="NO"
	hasUid=""
	ms=[]
	phone=""
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < column.APP_LOG_COLUMNS:
			continue
		uuid=column.uuidOnly(cols[1:])
		if uuid == None:
			continue
		uid=column.uid(cols[1:])
		method=cols[column.METHOD_CID+1]
		resp=""
		if method in regMs and method in column2.FuncMap and len(cols) >= column.APP_LOG_COLUMNS4+1:
			resp=column2.FuncMap[method](cols[-1])
			if resp == None:
				resp = ""
		if lastUuid == "":
			lastUuid=uuid
		if lastUuid != uuid:
			output(lastUuid,ms,regFirstDo,phone)
			lastUuid=uuid
			regFirstDo="NO"
			ms=[]
			hasUid=""
			phone=""
		if method in regMs and hasUid == "":
			if method == "passport.bindconnectstatus" and PositiveNumP.match(resp):
				hasHit=True
				if len(ms) >= 1:
					regFirstDo="OK"
				uid=resp
				#sys.stderr.write("bind uid:"+uid+"\n")
			elif method == "passport.reg" and PositiveNumP.match(resp):
				hasHit=True
				if len(ms) >= 1:
					regFirstDo="OK"
				uid=resp
				#sys.stderr.write("reg uid:"+uid+"\n")
			else:
				if resp != "":
					ms.append(method+"("+resp+")")
				else:
					ms.append(method)
		elif len(ms) >= 1:
			if (regFirstDo=="NO" or regFirstDo=="OK"):
				if method in regUserMs:
					regFirstDo=method
				elif uid != "":
					regFirstDo="OK"
		if uid != "":
			hasUid=uid
		if method == "common.sendcode":
			phone=column.getValue(cols[column.PARA_ID+1],"phone")
			#sys.stderr.write(method+"\t"+cols[column.PARA_ID+1]+"\t"+phone+"\n")
	if lastUuid != "":
		output(lastUuid,ms,regFirstDo,phone)
	for m in lms:
		if m.endswith("_NO"):
			continue
		v=lms[m]
		nv=0
		if m+"_NO" in lms:
			nv=lms[m+"_NO"]
		print "%s\t%d\t%d\t%.4f"%(m,lms[m],nv,nv/(v+1e-32))

def reduce():
	ts={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		t=cols[0]
		v=int(cols[1])
		nv=int(cols[2])
		if t not in ts:
			ts[t]=[0,0]
		ts[t][0]+=v
		ts[t][1]+=nv
	for t in ts:
		v=ts[t][0]
		nv=ts[t][1]
		print "%s\t%d\t%d\t%.4f"%(t,v,nv,nv/(v+1e-32))


if __name__=="__main__":
	if sys.argv[1] == "map":
		count()
	else:
		reduce()



