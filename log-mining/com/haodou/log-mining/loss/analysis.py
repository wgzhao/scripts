#encoding=utf-8

import sys

sys.path.append("./")
sys.path.append("../sequence")
sys.path.append("../util")
sys.path.append("../")

import TimeUtil
import actionUserInfo
import DictUtil
import column
import mergeData

MaxDiv=14

import readHitMethod
hits=readHitMethod.readMethod()
us=mergeData.readUser()

def topnRateStr(d,n=500):
	sd=sorted(d.items(),key=lambda e:e[1],reverse=True)
	nn=0
	sum=DictUtil.sum(d)
	if sum == 0:
		sum+=1e-32
	s=""
	for w,c in sd:
		v=float(c)/sum
		nn+=1
		if nn > n:
			break
		s+="("+str(w)+",%d,%.4f"%(c,v)+")"
	return "["+s+"]"

class DayRet(object):
	def __init__(self,day):
		self.hitNum=0
		self.ms={}
		self.mts={}
		self.day=day
		self.lastDiv=0
		self.firstDiv=0
	
	def div(self,firstDay,lastDay):
		self.lastDiv=TimeUtil.daysDiv(lastDay,self.day) #与上一次的访问间隔
		self.firstDiv=TimeUtil.daysDiv(firstDay,self.day) #与第一次的访问间隔
	
	def addMethod(self,method):
		if method not in hits:
			return
		(method,type)=hits[method]
		self.hitNum+=1
		if method not in self.ms:
			self.ms[method]=1
		else:
			self.ms[method]+=1
		if type not in self.mts:
			self.mts[type]=1
		else:
			self.mts[type]+=1

class UserRet(object):
	def __init__(self,user,firstDay):
		self.user=user
		self.days=[]
		self.firstDay=firstDay
		self.lastDay=firstDay

	def addDay(self,dayRet):
		ldiv=TimeUtil.daysDiv(self.lastDay,dayRet.day)
		sdiv=TimeUtil.daysDiv(self.firstDay,dayRet.day)
		if (len(self.days) > 0 and ldiv <= 0) or sdiv > MaxDiv:
			return
		self.days.append(dayRet)
		dayRet.div(self.firstDay,self.lastDay)
		self.lastDay=dayRet.day

class DayUsersRet(object):
	def __init__(self):
		self.userNum=0
		self.channels={}
		self.hits={}
		self.lastDivs={}
		self.firstDivs={}
		self.mc={}
		self.mu={}
		self.tu={}
	
	def addChannel(self,user):
		self.userNum+=1
		media=""
		if user.uuid != None and user.uuid in us:
			media=us[user.uuid]
		DictUtil.addOne(self.channels,media)
	
	def addDay(self,user,dayRet):
		self.addChannel(user)
		DictUtil.addOne(self.hits,dayRet.hitNum)
		DictUtil.addOne(self.lastDivs,dayRet.lastDiv)
		DictUtil.addOne(self.firstDivs,dayRet.firstDiv)
		for m in dayRet.ms:
			if m not in self.mu:
				self.mu[m]=1
				self.mc[m]=dayRet.ms[m]
			else:
				self.mu[m]+=1
				self.mc[m]+=dayRet.ms[m]
		for t in dayRet.mts:
			if t not in self.tu:
				self.tu[t]=1
			else:
				self.tu[t]+=1
	
	def printStr(self):
		s=str(self.userNum)
		s+="\t"+topnRateStr(self.channels)
		s+="\t"+topnRateStr(self.hits)
		s+="\t"+topnRateStr(self.firstDivs)
		s+="\t"+topnRateStr(self.lastDivs)
		s+="\t"+topnRateStr(self.mc)
		s+="\t"+topnRateStr(self.mu)
		s+="\t"+topnRateStr(self.tu)
		return s
	
	def __str__(self):
		s=str(self.userNum)
		s+="\t"+str(self.channels)+"\t"+str(self.hits)+"\t"+str(self.firstDivs)+"\t"+str(self.lastDivs)+"\t"+str(self.mc)+"\t"+str(self.mu)+"\t"+str(self.tu)
		return s

	@staticmethod
	def readDaysRet(cols):
		dr=DayUsersRet()
		dr.userNum=int(cols[0])
		dr.hits=eval(cols[1])
		dr.channels=eval(cols[2])
		dr.firstDivs=eval(cols[3])
		dr.lastDivs=eval(cols[4])
		dr.mc=eval(cols[5])
		dr.mu=eval(cols[6])
		dr.tu=eval(cols[7])
		return dr

	def merge(self,dr):
		self.userNum+=dr.userNum
		DictUtil.merge(self.channels,dr.channels)
		DictUtil.merge(self.hits,dr.hits)
		DictUtil.merge(self.firstDivs,dr.firstDivs)
		DictUtil.merge(self.lastDivs,dr.lastDivs)
		DictUtil.merge(self.mc,dr.mc)
		DictUtil.merge(self.mu,dr.mu)
		DictUtil.merge(self.tu,dr.tu)

def addUserRet(daysRet,userRet):
	if userRet == None:
		return
	for i in range(len(userRet.days)):
		dayRet=userRet.days[i]
		if i == 0:
			if len(userRet.days) == 1:
				if "loss" not in daysRet:
					daysRet["loss"]=DayUsersRet()
				daysRet["loss"].addDay(userRet.user,dayRet)
			else:
				if "retain" not in daysRet:
					daysRet["retain"]=DayUsersRet()
				daysRet["retain"].addDay(userRet.user,dayRet)
		si=str(i)
		if si not in daysRet:
			daysRet[si]=DayUsersRet()
		daysRet[si].addDay(userRet.user,dayRet)

def count():
	lastU=""
	lastUser=None
	daysRet={}
	userRet=None
	dayRet=None
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 5:
			continue
		u=cols[0]
		time=cols[1]
		if time == "-1":
			user=actionUserInfo.UserInfo.readUserInfo(cols)
			if lastU == u:
				lastUser.merge(user)
		if lastU == "":
			lastU=u
			lastUser=user
		if lastU != u:
			addUserRet(daysRet,userRet)
			lastU=u
			lastUser=user
			userRet=None
		if time == "-1":
			continue
		method=column.detailMethod(cols[3],cols[4])
		#if method.startswith("search.getlist"):
		#	sys.stderr.write(method+"\t"+cols[4]+"\n")
		if cols[2] == "0":
			day=TimeUtil.getDay(int(time))
			if userRet == None:
				userRet=UserRet(lastUser,day)
			dayRet=DayRet(day)
			userRet.addDay(dayRet)
		if dayRet != None:
			dayRet.addMethod(method)
	if lastU != "":
		addUserRet(daysRet,userRet)
	for i in daysRet:
		print str(i)+"\t"+str(daysRet[i])

if __name__=="__main__":
	count()



