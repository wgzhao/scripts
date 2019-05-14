#encoding=utf-8
import sys

sys.path.append("../")
import column

sys.path.append("./")
from actionUserInfo import *
from getActionItem import *

sys.path.append("../util")
import DictUtil

def readNotSeq(f):
	ns={}
	for line in f:
		cols=line.strip().split("\t")
		if len(cols) < 2:
			continue
		m1=cols[0]
		m2=cols[1]
		if m1 not in ns:
			ns[m1]={}
		ns[m1][m2]=1
	return ns

ns=readNotSeq(open("notSeq.txt"))
ons=readNotSeq(open("onlySeq.txt"))

def getParentMethod(method,para):
	offset=column.getValue(para,"offset")
	if offset != "0" and offset != "":
		return method
	if method == "search.getlist":
		scene=column.getValue(para,"scene")
		if scene == "t1":
			return "search.getcatelist"
		elif scene == "t2":
			return "recipe.getcollectlist"
		elif scene == "k3":
			return "info.getinfo"
	return ""

class Action(object):
	def __init__(self,id,time,method,para,user):
		self.id=id
		self.time=time
		self.method=method
		self.para=para
		self.response={}
		self.hit={}
		self.pull=[]
		self.sons=[]
		self.parentId=-1
		self.parentMethod=getParentMethod(method,para)
		self.user=user

	def __str__(self):
		#if len(self.hit) <= 0:
		#	return ""
		return self.user.name()+"\t"+str(self.id)+"\t"+self.time+"\t"+self.method+"\t"+self.para+"\t"+str(self.response)+"\t"+str(self.hit)+"\t"+str(self.sons)+"\t"+str(self.pull)+"\t"+str(self.parentId)+"\t"+self.parentMethod+"\t"+str(self.getItem())+"\t"+DictUtil.listStr(self.resultItems())

	@staticmethod
	def readAction(cols,user):
		try:
			id=int(cols[1])
			if id == -1:
				return None
		except:
			sys.stderr.write("\t".join(cols))
			return None
		time=cols[2]
		method=cols[3]
		para=cols[4]
		action=Action(id,time,method,para,user)
		try:
			action.response=eval(cols[5])
		except:
			action.response=eval('"'+cols[5]+'"')
		action.hit=eval(cols[6])
		action.sons=eval(cols[7])
		action.pull=eval(cols[8])
		action.parentId=int(cols[9])
		action.parentMethod=cols[10]
		return action

	def name(self):
		return "#"

	def resultItems(self):
		return actionResultItems(self.method,self.para,self.response)

	def getItem(self):
		return getActionItem(self.method,self.para,self.user.version)

	def getNotVoidItem(self):
		item=getActionItem(self.method,self.para,self.user.version)
		if item == self.method:
			return ""
		return item

	def requestIdCheck(self,action):
		rqid=column.getValue(action.para,"return_request_id")
		qid=column.getValue(self.para,"request_id")
		if rqid != "" and qid != "":
			if rqid == qid:
				return 1
			else:
				return -1
		return 0

	def addHit(self,action):
		#print "self-addHit",self
		#print "action-addHit",action
		if action.method in ons and not self.method in ons[action.method]:
			#print "self",self
			#print "action",action
			#print ""
			return False
		if action.user.sameUserGivenIP(self.user) < 0:
			return False
		if self.method in ns and action.method in ns[self.method]:
			#print "self",self
			#print "action",action
			#print ""
			return False
		if self.method == action.method:
			offset=column.getValue(self.para,"offset") #只有列表页可以有方法名与自己想同的子节点
			offset1=column.getValue(action.para,"offset")
			if offset !="0" or offset1 == "" or offset1 == "0":
				#print "self",self
				#print "action",action
				#print ""
				return False
			else:
				self.pull.append(action.id)
				action.parentId=self.id
				action.parentMethod=self.method
				return True
		#else:
			#if self.requestIdCheck(action) < 0:
				#print "self",self
				#print "action",action
				#print ""
				#return False
		if action.method == "search.getlist":
			scene=column.getValue(action.para,"scene")
			if scene != "":
				if self.method == "info.getinfo":
					if scene != "k3":
						#print "self",self
						#print "action",action
						#print ""
						return False
				elif self.method == "recipe.getcollectlist":
					if scene != "t2":
						return False
				elif self.method == "search.getcatelist":
					if scene != "t1":
						return False
				elif self.method == "recipe.getcollectrecomment":
					if scene != "k2":
						return False
		#print "self-after-addHit",self
		#print "action-after-addHit",action
		if action.getItem() not in self.hit:
			self.hit[action.getItem()]=1
		else:
			self.hit[action.getItem()]+=1
		self.sons.append(action.id)
		action.parentId=self.id
		action.parentMethod=self.method
		return True

def getActcion(line,user,lastAid):
	id=lastAid+1
	time=""
	method=""
	para=""
	return Action(id,time,method,para,user)


if __name__=="__main__":
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if cols[1] == "-1":
			userinfo=UserInfo.readUserInfo(cols)
			print userinfo
		else:
			action=Action.readAction(cols,userinfo)
			print action

