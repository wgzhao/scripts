#encoding=utf-8

import re

CommSign="comm"
EntityLastUser="ilu"
EntityUser="iu"
EntityStatis="is"
UserEntity="ui"
UserStatis="us"

UserNumForEntity=300
EntityNumForUser=300
EntityTypeNumForUser=100

MinItemNum=50

ActiveEntityRecipeNum=5000
ActiveEntityTypeNum=2000
ActiveUserNum=50000

SumId=0 #行为总数
MaxId=1 #最大行为数
MaxDayId=2
MinId=3 #最小行为数
MinDayId=4
quardSumId=5  #平方和
LastDayId=6  #最旧日期
HitDaysCountId=7 #有行为日期总数
StatisKey="__statis__"
DaysKey="__day__"
MonthsKey="__month__"
YearsKey="__year__"

MaxDaysNum=7
MaxMonthsNum=6

NotRegistrUser="0"

StatisKeys={
	StatisKey:1,
	DaysKey:2,
	MonthsKey:2,
	YearsKey:2,
}


userUidP=re.compile(r"^uid-[0-9]+$")

def getRegisterUser(user): #只用注册用户为item建模
	if userUidP.match(user):
		return user[4:]
	return None


def escapeMongoKey(key):
	ns={}
	for i in range(len(key)):
		k=key[i]
		if k == "." or k == "$" or k == "\0":
			ns[i]=1
	if len(ns) > 0:
		s=""
		for i in range(len(key)):
			if i in ns:
				s+="@"
			else:
				s+=key[i]
		return s
	return key

def nutPrint(user,action,entity,time):
	user=escapeMongoKey(user)
	entity=escapeMongoKey(entity)
	uid=getRegisterUser(user)
	if uid:
		print uid+"\t"+UserEntity+"\t"+entity+"\t"+action+"\t"+time
		print entity+"\t"+EntityUser+"\t"+uid+"\t"+action+"\t"+time
	else:
		print user+"\t"+UserEntity+"\t"+entity+"\t"+action+"\t"+time
		print entity+"\t"+EntityUser+"\t"+NotRegistrUser+"\t"+action+"\t"+time

def isNutLine(cols):
	if len(cols) == 5 and (cols[1] == UserEntity or cols[1] == EntityUser):
		return True
	return False

def isAccLine(cols):
	if len(cols) == 6 and (cols[1] == UserEntity or cols[1] == EntityUser):
		return True
	return False

def parseLine(cols):
	if isNutLine(cols):
		k1=cols[0]
		t=cols[1]
		k2=cols[2]
		action=cols[3]
		time=cols[4]
		return (k1,t,k2,action,time)
	return None


def getEntityType(entity):
	pos=entity.find("-")
	if pos > 0:
		return entity[0:pos]
	return ""

if __name__=="__main__":
	keys=["1.2.3,4",".ert","234.html","$3"]
	for key in keys:
		print key,escapeMongoKey(key)


