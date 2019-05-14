#encoding=utf-8

import sys

sys.path.append("./")
from userRecomConf import *

sys.path.append("../util")
import TimeUtil

def filter(d,num):
	if len(d) <= num:
		return d
	newDict={}
	sortedItems=sorted(d.items(),key=lambda k:(k[1][0]+k[1][1]/1000000.0),reverse=True)
	for u,(time,n) in sortedItems:  #选出一部分时间上最新的条目
		newDict[u]=(time,n)
		if len(newDict) >= (2*num/3):
			break
	sortedItems=sorted(d.items(),key=lambda k:(k[1][1]*1000000+k[1][0]),reverse=True)
	for u,(time,n) in sortedItems: #选出一部分访问次数最多的条目
		newDict[u]=(time,n)
		if len(newDict) >= num:
			break
	sys.stderr.write("size:%d\tnum:%d\n"%(len(newDict),num))
	return newDict

#
#要保证不同类型的实体都有
#
def filterEntityForUser(entitys):
	if len(entitys) < EntityNumForUser:
		return entitys
	newDict={}
	ets={}
	for e in entitys:
		pos=e.find("-")
		t=e[0:pos]
		if t not in ets:
			ets[t]={}
		ets[t][e]=entitys[e]
	for t in ets:
		for e,(time,n) in filter(ets[t],EntityTypeNumForUser).items():
			newDict[e]=(time,n)
		#sys.stderr.write("size:%d\tt:%s\n"%(len(newDict),t))
	return newDict

def getTimeDict(statis,TimeKey):
	d={}
	if TimeKey in statis:
		timeList=statis[TimeKey]
		i=0
		while i < len(timeList):
			d[timeList[i]]=int(timeList[i+1])
			i+=2
	return d

def addYear(months,statis,IsDay=True):
	years=getTimeDict(statis,YearsKey)
	for item,n in months.items():
		if IsDay:
			year=item[0:-6]
		else:
			year=item[0:-3]
		if year not in years:
			years[year]=n
		else:
			years[year]+=n
	statis[YearsKey]=[]
	for year,n in years.items():
		statis[YearsKey].append(year)
		statis[YearsKey].append(str(n))

def getMonths(days,statis):
	months=getTimeDict(statis,MonthsKey)
	for day,n in days.items():
		month=day[0:-3]
		if month not in months:
			months[month]=n
		else:
			months[month]+=n
	return months

def getOldDays(olds,statis):
	days=getTimeDict(statis,DaysKey)
	if len(days) > 0:
		return days
	for k2,(time,n) in olds.items():
		day=TimeUtil.getDay(int(time))
		if day not in days:
			days[day]=n
		else:
			days[day]+=n
	return days

def getStatis(statis):
	if StatisKey in statis:
		sum=int(statis[StatisKey][0])
		max=int(statis[StatisKey][1])
		maxDay=statis[StatisKey][2]
		min=int(statis[StatisKey][3])
		minDay=statis[StatisKey][4]
		sum2=long(statis[StatisKey][5])
		lastDay=statis[StatisKey][6]
		hitDaysCount=int(statis[StatisKey][7])
	else:
		sum=0
		max=0
		maxDay=""
		min=sys.maxint
		minDay=""
		sum2=0l  #用长整型
		lastDay="9999-00-00"
		hitDaysCount=0
	return (sum,max,maxDay,min,minDay,sum2,lastDay,hitDaysCount)

def addSum(days, statis):
	dayItems=days.items()
	(sum,max,maxDay,min,minDay,sum2,lastDay,hitDaysCount)=getStatis(statis)
	for day,n in dayItems:
		sum+=n
		if n > max:
			max=n
			maxDay=day
		if n < min:
			min=n
			minDay=day
		sum2+=n*n
		if day < lastDay:
			lastDay=day
		hitDaysCount+=1
	statis[StatisKey]=[str(sum),str(max),maxDay,str(min),minDay,str(sum2),str(lastDay),str(hitDaysCount)]

def setMonths(months,statis):
	if len(months) > MaxMonthsNum:
		items=sorted(months.items(),key=lambda k:k[0],reverse=True)
	else:
		items=months.items()
	statis[MonthsKey]=[]
	for month,n in items:
		statis[MonthsKey].append(str(month))
		statis[MonthsKey].append(str(n))
		if len(statis[MonthsKey]) >= 2*MaxMonthsNum:
			break

def setDays(days,statis):
	if len(days) > MaxDaysNum:
		items=sorted(days.items(),key=lambda k:k[0],reverse=True)
	else:
		items=days.items()
	statis[DaysKey]=[]
	for day,n in items:
		statis[DaysKey].append(day)
		statis[DaysKey].append(str(n))
		if len(statis[DaysKey]) >= 2* MaxDaysNum:
			break

def merge(statis,olds,news,days):
	oldDays=getOldDays(olds,statis)
	for k2 in news: #累加新项，计算新项覆盖的日期
		(time,n)=news[k2]
		if k2 in olds:
			olds[k2][1]+=n
			if time > olds[k2][0]:
				olds[k2][0]=time
		else:
			olds[k2]=[time,n]
	newDays={}
	for day,n in days.items():
		if day not in oldDays:
			oldDays[day]=n
			newDays[day]=n
	#print len(newDays),len(oldDays)
	if len(oldDays) > MaxDaysNum:
		if StatisKey in statis: #旧的已经累加，只要加新的
			addSum(newDays,statis)
		else:
			addSum(oldDays,statis) #从头开始累计
		if MonthsKey not in statis:
			oldMonths=getMonths(oldDays,statis)
		else:
			oldMonths=getMonths(newDays,statis)
		if len(oldMonths) >= MaxMonthsNum:
			if YearsKey in statis:
				addYear(newDays,statis,IsDay=True)
			else:
				addYear(oldMonths,statis,IsDay=False)
		setMonths(oldMonths,statis) #月数过多会剪枝
	if len(oldDays) > 1 or len(olds) >= MinItemNum: #有多天或者单天条目较多，应该记录每天的数目
		setDays(oldDays,statis) #天数过多会剪枝
	return (statis,olds)

def output(lastKey,ds):
	for t in ds:
		for action in ds[t]:
			statis,olds,news,days=ds[t][action]
			(statis,items)=merge(statis,olds,news,days)
			if t == UserEntity:
				filterDict=filterEntityForUser(items)
			else:
				filterDict=filter(items,UserNumForEntity)
			for k2 in filterDict: #项目累加结果输出
				time,n=filterDict[k2]
				print lastKey+"\t"+t+"\t"+k2+"\t"+action+"\t"+str(time)+"\t"+str(n)
			for k2 in statis: #统计结果在merge步骤已经过滤
				statisCols=statis[k2]
				print lastKey+"\t"+t+"\t"+k2+"\t"+action+"\t"+"\t".join(statisCols)

def acc():
	lastKey=""
	ds={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		key=cols[0]
		if lastKey == "":
			lastKey=key
		if lastKey != key:
			output(lastKey,ds)
			lastKey=key
			ds={}
		t=cols[1]
		action=cols[3]
		if t not in ds:
			ds[t]={}
		if action not in ds[t]:
			ds[t][action]=[{},{},{},{}]
		statis,olds,news,days=ds[t][action]
		k2=cols[2]
		if k2 in StatisKeys:  #历史统计数据
			statis[k2]=cols[4:]
		elif len(cols) >= 6: #历史数据
			time=int(cols[4])
			n=int(cols[5])
			olds[k2]=[time,n]
		else: #新增数据
			time=int(cols[4])
			day=TimeUtil.getDay(time)
			if day not in days:
				days[day]=1
			else:
				days[day]+=1
			if k2 not in news:
				news[k2]=[time,1]
			else:
				news[k2][1]+=1
				if time > news[k2][0]:
					news[k2][0]=time
	if lastKey != "":
		output(lastKey,ds)

if __name__=="__main__":
	acc()


