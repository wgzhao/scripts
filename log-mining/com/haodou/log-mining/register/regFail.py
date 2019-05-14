#encoding=utf-8

import sys
import re
sys.path.append("./")
sys.path.append("../")
import column

sys.path.append("../abtest")

from regSeq	import *
import column2

hits=readMethod(open("./hitMethod.txt"))

def count():
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < column.APP_LOG_COLUMNS:
			continue
		uuid=column.uuidOnly(cols[1:])
		if uuid == None:
			continue
		time=cols[1]
		method=cols[column.METHOD_CID+1]
		uid=column.uid(cols[1:])
		response=""
		if method in regMs and method in column2.FuncMap and len(cols) >= column.APP_LOG_COLUMNS4+1:
			response=column2.FuncMap[method](cols[-1])
			if response == None:
				response=""
			#print method
		print "%s\t%s\t%s\t%s\t%s"%(uuid,time,uid,method,response)

def addFea(fea):
	if fea not in lms:
		lms[fea]=1
	else:
		lms[fea]+=1

def outputMc(lastUuid,ms,regFirstDo,hasUid,hasHit):
	#print lastUuid+"\t"+regFirstDo
	if len(ms) == 0:
		return
	s=lastUuid
	phoneNum=0
	bindNum=0
	regNum=0
	for m in ms:
		if m.startswith("common.sendcode"):
			phoneNum+=1
		elif m.startswith("passport.bindconnectstatus"):
			bindNum+=1
		elif m.startswith("passport.reg"):
			regNum+=1
		s+="_"+m
	mc={}
	for m in ms:
		if not m.startswith("common.sendcode"):
			p=m.find('(')
			if p > 0 and m.find("message") < 0:
				m=m[0:p]
		if m not in mc:
			mc[m]=1
		else:
			mc[m]+=1
	if phoneNum >= 2:
		phoneNum=2
	regOk=True
	if regFirstDo == "NO":
		regOk=False
	for m in mc:
		if mc[m] > 2:
			mc[m]=2
		addFea("%s_%d"%(m,mc[m]))
		if regNum > 0 and m.startswith("common.sendcode"):
			continue
		addFea("%s_%d_注册成功(%r)"%(m,mc[m],regOk))
	addFea("注册成功(%r)"%(regOk))
	addFea("手机绑定次数(%r)"%(phoneNum))
	addFea("手机绑定次数(%r)_有后续点击(%r)"%(phoneNum,hasHit))
	addFea("手机绑定次数(%r)_注册成功(%r)"%(phoneNum,regOk))
	addFea("注册后调用第一个接口(%r)"%(regFirstDo))
	if bindNum >= 2:
		bindNum=2
	if regNum >= 2:
		regNum=2
	addFea("调用绑定接口(%r)次"%(bindNum))
	addFea("调用绑定接口(%r)次_注册成功(%r)"%(bindNum,regOk))
	addFea("reg接口(%r)次"%(regNum))
	addFea("reg接口(%r)次_注册成功(%r)"%(regNum,regOk))
	if bindNum > 0:
		if phoneNum == 0:
			addFea("调用绑定接口(%r)次_没有手机绑定"%(bindNum))
			addFea("调用绑定接口(%r)次_没有手机绑定_有后续点击(%r)"%(bindNum,hasHit))
			if regNum == 0:
				addFea("调用绑定接口(%r)次_没有手机绑定_注册成功(%r)"%(bindNum,regOk))
			addFea("调用绑定接口(%r)次_没有手机绑定_reg接口次数(%r)"%(bindNum,regNum))
	s+="_"+regFirstDo
	if hasUid != "":
		s+="("+hasUid+")"
	#print s

PositiveNumP=re.compile("[1-9][0-9]*")

def outputUser(lastUuid,ms,regFirstDo,hasUid,hasHit):
	if len(ms) == 0:
		return
	if regFirstDo == "NO":
		print lastUuid+"\tNO"
	else:
		print lastUuid+"\tYES"

def output(lastUuid,ms,regFirstDo,hasUid,hasHit):
	if sys.argv[1] == "reduce":
		outputMc(lastUuid,ms,regFirstDo,hasUid,hasHit)
	elif sys.argv[1] == "user":
		outputUser(lastUuid,ms,regFirstDo,hasUid,hasHit)

def reduce():
	maxUid=0
	for line in open("maxUid.txt"):
		maxUid=int(line.strip())
	sys.stderr.write("%d\n"%(maxUid))
	lastUuid=""
	regFirstDo="NO"
	hasHit=False
	hasUid=""
	ms=[]
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 4:
			continue
		uuid=cols[0]
		uid=cols[2]
		method=cols[3]
		resp=""
		if len(cols) >= 5:
			resp=cols[4]
		if lastUuid == "":
			lastUuid=uuid
		if lastUuid != uuid:
			output(lastUuid,ms,regFirstDo,hasUid,hasHit)
			lastUuid=uuid
			regFirstDo="NO"
			ms=[]
			hasHit=False
			hasUid=""
		if method in regMs and hasUid == "":
			if method == "passport.loginbyconnect":
				if PositiveNumP.match(resp):
					if int(resp) > maxUid:
						uid=resp
						regFirstDo="OK"
						ms.append(method)
				else:
					ms.append(method+"("+resp+")")
			elif method == "passport.bindconnectstatus" and PositiveNumP.match(resp):
				hasHit=True
				if int(resp) > maxUid:
					regFirstDo="OK"
					uid=resp
					if resp != "":
						ms.append(method+"("+resp+")")
					else:
						ms.append(method)
				#sys.stderr.write("bind uid:"+uid+"\n")
			elif method == "passport.reg" and PositiveNumP.match(resp):
				hasHit=True
				regFirstDo="OK"
				uid=resp
				#sys.stderr.write("reg uid:"+uid+"\n")
				if resp != "":
					ms.append(method+"("+resp+")")
				else:
					ms.append(method)
			else:
				if resp != "":
					ms.append(method+"("+resp+")")
				else:
					ms.append(method)
		elif len(ms) >= 1: #前面已经有注册方法
			if (regFirstDo=="NO" or regFirstDo=="OK"):
				if method in regUserMs:
					regFirstDo=method
				elif uid != "":
					regFirstDo="OK"
			if method in hits:
				hasHit=True
		if uid != "":
			hasUid=uid

	if lastUuid != "":
		output(lastUuid,ms,regFirstDo,hasUid,hasHit)
	if sys.argv[1] == "reduce":
		for m in lms:
			print "%s\t%d"%(m,lms[m])

if __name__=="__main__":
	if sys.argv[1] == "map":
		count()
	elif sys.argv[1] == "reduce" or sys.argv[1] == "user":
		reduce()

