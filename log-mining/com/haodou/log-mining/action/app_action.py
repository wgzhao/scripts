#encoding=utf-8

import sys
sys.path.append("./")
from app_action1 import *

sys.path.append("../")
import column

class User(object):
	def __init__(self):
		self.uuid="#"
		self.device_time=0
		self.server_time=0
		self.device="#"  #对应"o"字段
		self.version="#" #"d"
		self.ip="#" #user_ip
		self.channel="#"
		self.net='#'  #网络环境，3G或者WIFI

	#def __str__(self):
		#return
		#return "%s\t%s\t%s\t%s\t%s\t%r\t%r\t%s"%(self.uuid,self.device,self.ip,self.channel,self.version,self.device_time,self.server_time,self.net)

	def noUuidStr(self):
		return "%s\t%s\t%s\t%s\t%r\t%r\t%s"%(self.device,self.ip,self.channel,self.version,self.device_time,self.server_time,self.net)

	def __str__(self):
		return self.uuid+"\t"+self.noUuidStr()

	@staticmethod
	def readUserNoUuid(cols):
		user=User()
		user.device=cols[0]
		user.ip=cols[1]
		user.channel=cols[2]
		user.version=cols[3]
		if len(cols) >= 5:
			user.device_time=float(cols[4])
		if len(cols) >= 6:
			user.server_time=float(cols[5])
		if len(cols) >= 7:
			user.net=cols[6]
		return user

class Action(object):
	def __init__(self,user):
		self.time=0
		self.action="#"
		self.page="#"
		self.user=user

	def __str__(self):
		return "%s\t%r\t%s\t%s\t%s"%(self.user.uuid,self.time,self.action,self.page,self.user.noUuidStr())

	@staticmethod
	def readAction(cols):
		action=Action(User.readUserNoUuid(cols[4:]))
		try:
			action.user.uuid=cols[0]
			action.time=float(cols[1])
			action.action=cols[2]
			action.page=cols[3]
		except:
			pass
		return action

def getUser(data):
	user=User()
	user.uuid=getUuid(data)
	if user.uuid == "":
		return None
	user.server_time=getServerTime(data)
	user.device_time=getDeviceTime(data)
	user.device=getValString(data,'"o":"')
	user.ip=getValString(data,'"user_ip":"')
	user.channel=getValString(data,'"channel":"')
	user.version=getValString(data,'"d":"')
	user.net=getValString(data,'"i":"')
	return user


def parse(data,VER=None):
	if VER == None:
		VER = getVer44(data)
	rtn=[]
	p1 = 0
	p2=0
	user=getUser(data)
	if user == None:
		return []
	'''
	user.uuid=getUuid(data)
	if user.uuid == "":
		return []
	user.server_time=getServerTime(data)
	user.device_time=getDeviceTime(data)
	user.device=getValString(data,'"o":"')
	user.ip=getValString(data,'"user_ip":"')
	user.channel=getValString(data,'"channel":"')
	user.version=getValString(data,'"d":"')
	user.net=getValString(data,'"i":"')
	'''
	while(p1 >= 0):
		action=Action(user)
		p1=data.find('"action"',p2)
		if p1 == -1 : 
			continue
		while (data[p1]!='{' and data[p1]!='}' and p1 >=p2):  #因为是字典，不是序列，所以json中的page可能在“action”前面，因此查找“action”前面的“{”或者“}”符号作为这个item的开始
			p1 -= 1
		p1+=1
		p2 = data.find('}',p1)
		if p2 == -1 : continue
		substr=data[p1:p2]
		p1 = p2
		action.action=getAction(substr)
		action.page=getPage(substr)
		action.time=getExtTime(substr)
		if action.time <= 0:
			action.time=user.server_time
		rtn.append(action)
	return rtn

def testParser():
	for line in sys.stdin:
		#print line.strip()
		rtn=parse(line.strip())
		for action in rtn:
			print action

def parseUser():
	for line in sys.stdin:
		user=getUser(line.strip())
		if user != None:
			print user

def request(line):
	if True:
		cols=line.strip().split("\t")
		if len(cols) < column.APP_LOG_COLUMNS:
			return 1
		uuid=column.uuidOnly(cols)
		if uuid == None or uuid == "":
			return 1
		print uuid+"\t"+line.strip()
	return 0

def merge():
	action_wrong=0
	other_wrong=0
	for line in sys.stdin:
		line=line.strip()
		if line.startswith('{"'):
			rtn=parse(line)
			for action in rtn:
				print action
			if len(rtn) == 0:
				action_wrong+=1
		else:
			other_wrong+=request(line)
	print "wrong\t0\taction_wrong=%d\tother_wrong=%d"%(action_wrong,other_wrong)	

def diff():
	lastU=""
	vs={}
	ms={}
	v=""
	m=""
	type=""
	for line in sys.stdin:
		cols=line.strip().split("\t")
		u=cols[0]
		if lastU == "":
			lastU=u
		if lastU != u:
			output(vs,ms,v,m,type)
			v=""
			m=""
			type=""
			lastU=u
		if len(cols) < column.APP_LOG_COLUMNS+1:
			v=cols[7]
			m=cols[8]
		else:
			v=cols[column.VERSION_CID+1]
			tm=cols[column.MEDIA_CID]
			p=tm.find("_")
			if p > 0:
				m=tm[0:p]
				v=tm[p+1:]

def actionCount():
	cs={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 3:
			sys.stderr.write(line)
			continue
		action=cols[2]
		if action not in cs:
			cs[action]=1
		else:
			cs[action]+=1
	for action in cs:
		print action+"\t"+str(cs[action])

def countMerge():
	cs={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 2:
			continue
		a=cols[0]
		c=int(cols[1])
		if a not in cs:
			cs[a]=c
		else:
			cs[a]+=c
	for action in cs:
		print action+"\t"+str(cs[action])

def net():
	ns={}
	for line in sys.stdin:
		n=getValString(line,'"i":"')
		if n not in ns:
			ns[n]=1
		else:
			ns[n]+=1
	for n in ns:
		print n,ns[n]

def printNet(nets):
	tuv=0
	tpv=0
	for net in nets:
		uv,nuv,pv,npv,void=nets[net]
		tuv+=uv
		tpv+=pv
	for net in nets:
		uv,nuv,pv,npv,void=nets[net]
		print "%s\t%d\t%d\t%d\t%.4f\t%d\t%d\t%.4f\t%d\t%.4f\t%d\t%.4f"%(net,void,uv,nuv,uv/(nuv+1e-12),pv,npv,pv/(npv+1e-12),tuv,uv/(tuv+1e-12),tpv,pv/(tpv+1e-12))
#
#统计查看视频菜谱的用户中有多少WIFI或者3G的比例
#
def netInVideo():
	nets={}
	for line in sys.stdin:
		user=getUser(line.strip())
		if user == None:
			continue
		rtn=parse(line.strip())
		net=user.net
		if net == "":
			net="Unkown"
		net+="_"+user.channel
		if net not in nets:
			nets[net]=[0,0,0,0,0]
		nets[net][1]+=1 #nuv
		has=False
		for action in rtn:
			if action.action.startswith("A400"): #视频行为的代码
				has=True
				nets[net][2]+=1 #pv
		nets[net][3]+=len(rtn) #npv
		if len(rtn) ==0:
			nets[net][4]+=1  #void
		if has:
			nets[net][0]+=1 #uv
	printNet(nets)
	
def videoNetMerge():
	print "网络类型\t该网络视频用户\t该网络总用户\t视频用户比例\t该网络视频行为数\t该网络行为总数\t视频行为比例\t视频用户数\t视频用户中该网络比例\t视频行为数\t视频行为中该用户比例"
	nets={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 6:
			continue
		net=cols[0]
		if net not in nets:
			nets[net]=[0,0,0,0,0]
		nets[net][0]+=int(cols[2]) #uv
		nets[net][1]+=int(cols[3]) #nuv
		nr=float(cols[4]) #uv/nuv
		nets[net][2]+=int(cols[5]) #pv
		nets[net][3]+=int(cols[6]) #npv
		nets[net][4]+=int(cols[1]) #void
	printNet(nets)	

if __name__=="__main__":
	if sys.argv[1] == "merge":
		merge()
	elif sys.argv[1] == "parser":
		testParser()
	elif sys.argv[1] == "actionCount":
		actionCount()
	elif sys.argv[1] == "countMerge":
		countMerge()
	elif sys.argv[1] == "net": #统计网络类型分布：3G，WIFI
		net()
	elif sys.argv[1] == "user":
		parseUser()
	elif sys.argv[1] == "netInVideo":
		netInVideo()
	elif sys.argv[1] == "videoNetMerge":
		videoNetMerge()

