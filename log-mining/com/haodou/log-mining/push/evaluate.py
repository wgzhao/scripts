#encoding=utf-8

import sys
sys.path.append("../util")
import columnUtil
import PushName
import itemBank

from readLog import *

def readUserPolicy(f,targetId):
	ps={}
	for line in f:
		cols=line.strip().split("\t")
		if len(cols) < 3:
			continue
		u=cols[0]
		id=cols[1]
		if id == "":
			id=targetId
		policy=cols[2]
		ps[u]=(id,policy)
	return ps
		
class EvaluateRet(object):
	def __init__(self,policy):
		self.policy=policy
		self.userNum=0
		self.receiveNum=0
		self.viewNum=0
		self.rms={}
		self.vms={}

	def __str__(self):
		rrate=self.receiveNum/(self.userNum+1e-1)
		vrate=self.viewNum/(self.receiveNum+1e-1)
		return "%s\t%d\t%d\t%.3f\t%d\t%.3f"%(self.policy,self.userNum,self.receiveNum,rrate,self.viewNum,vrate)

class EvBank(object):
	def __init__(self,ps,rs,targetItem):
		self.bank={}
		self.ps=ps
		self.rs=rs
		self.targetItem=targetItem
	
	def getE(self,policy):
		if policy not in self.bank:
			self.bank[policy]=EvaluateRet(policy)
			self.bank[policy].userNum=self.getUserNum(policy)
			print policy,self.getUserNum(policy)
		return self.bank[policy]
	
	def inGroup(self,user,policy):
		if user not in self.ps:
			sys.stderr.write("no user in policy for "+user+"\n")
			return False
		(id,upolicy)=self.ps[user]
		p=policy.find("_")
		if p > 0:
			policy=policy[0:p]
		if policy == upolicy:
			return True
		return False

	def getUserNum(self,policy):
		num=0
		for user in self.ps:
			if self.inGroup(user,policy):
				num+=1
		return num

	def addReceive(self,user,rid,appid,day):
		#print "addReceive"
		if user not in self.ps and rid != self.targetItem:
			#sys.stderr.write("no user in policy for "+user+"\n")
			return
		if user not in self.ps:
			id=rid
			if id != self.targetItem:
				raise Exception("rid not equal targetItem!!")
			policy="other"
		else:
			(id,policy)=self.ps[user]
		if id != rid and id+"_"+day != rid:
			#sys.stderr.write("the id("+id+")in policy"+"not equal receive id (" +rid+")for"+user+"\n")
			return
		policy+="_"+appid
		ret=self.getE(policy)
		ret.receiveNum+=1
		if id not in ret.rms:
			ret.rms[id]=1
		else:
			ret.rms[id]+=1

	def addView(self,user,id,appid,day):
		#print "addView"
		if user not in self.ps and id != self.targetItem:
			#sys.stderr.write("no user in policy for "+user+"\n")
			return
		if user not in self.ps:
			pid=id
			if pid != self.targetItem:
				raise Exception("vid not equal targetItem!!")
			policy="other"
		else:
			(pid,policy)=self.ps[user]
		if id != pid and pid+"_"+day != id:
			sys.stderr.write("the id("+pid+") in policy "+"not equal view id (" +id+") for "+user+"\n")
			return
		if user not in self.rs:
			sys.stderr.write(user +" in view but not in receive!\n")
			#return
			policy+="_notInRs"
		else:
			has=False
			for rid,rappid in self.rs[user]:
				if id == rid:
					has=True
					break
			if not has:
				policy+="_notInRsUser"
				sys.stderr.write(id+" for user("+user +") in view but not in receive: "+str(self.rs[user])+"!\n")
				#return
		policy+="_"+appid
		ret=self.getE(policy)
		ret.viewNum+=1
		if id not in ret.vms:
			ret.vms[id]=1
		else:
			ret.vms[id]+=1

def readPushLog(f):
	vs={}
	rs={}
	for line in f:
		cols=line.strip().split("\t")
		u=cols[0]
		if len(cols) < 3:
			continue
		id=cols[1]
		appid=cols[2]
		if len(cols) == 3:
			if u not in vs:
				vs[u]=[]
			vs[u].append((id,appid))
		else:
			if u not in rs:
				rs[u]=[]
			rs[u].append((id,appid))
	return (vs,rs)

def evaluate(date):
	dataDir="/home/zhangzhonghui/data/push/"+date+"/"
	
	(targetMessage,targetItem,defaultItems)=itemBank.getDefaultItems(date)

	f=open(dataDir+"log.txt")
	(vs,rs)=readPushLog(f)
	f.close()

	print targetItem

	if targetItem == "":
		max=0
		ids={}
		for user in rs:
			for id,channel in rs[user]:
				if id not in ids:
					ids[id]=1
				else:
					ids[id]+=1
		for id in ids:
			if ids[id] > max:
				targetItem=id
				max=ids[id]
		print "max-targetItem",targetItem

	f=open(dataDir+"userPolicy.txt")
	ps=readUserPolicy(f,targetItem)
	f.close()
	day="".join(date.split("-"))
	for user in ps:
		print "policy",user,ps[user]
		break
	for user in rs:
		print "receive",user,rs[user]
		break

	for user in vs:
		print "view",user,vs[user]
		break

	es=EvBank(ps,rs,targetItem)
	for user in rs:
		for id,appid in rs[user]:
			es.addReceive(user,id,appid,day)

	for user in vs:
		for id,appid in vs[user]:
			es.addView(user,id,appid,day)
	
	wf=open(dataDir+"score.txt","w")
	for k in es.bank:
		print es.bank[k]
		wf.write(str(es.bank[k])+"\n")
	wf.close()
	rs=None
	f=open(dataDir+"sendedLog.txt")
	mid=readSended(f)
	f.close()
	idm={}
	rs=None
	for m in mid:
		id=mid[m]
		idm[id]=m
	wf=open(dataDir+"score.detail.txt","w")
	for k in es.bank:
		#print "detail:",k
		e=es.bank[k]
		for id in e.rms:
			rnum=e.rms[id]
			vnum=0
			if id in e.vms:
				vnum=e.vms[id]
			m=id
			if id in idm:
				m=idm[id]
			wf.write("%s\t%s\t%s\t%d\t%d\t%.3f\n"%(e.policy,id,m,rnum,vnum,vnum/(rnum+1e-16)))
	wf.close()

if __name__=="__main__":
	evaluate(sys.argv[1])


