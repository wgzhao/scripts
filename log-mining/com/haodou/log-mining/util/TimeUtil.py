import time
import datetime
import sys


#input:2014-01-01
def addDay(dateStr,n):
	intTime=time.mktime(time.strptime(dateStr,"%Y-%m-%d"))+86400*n
	return time.strftime("%Y-%m-%d",time.localtime(intTime))

def getDay(intTime):
	return time.strftime("%Y-%m-%d",time.localtime(intTime))

def getTodayStr():
	return time.strftime('%Y-%m-%d',time.localtime(time.time()))

def getYesterdayStr():
	return addDay(getTodayStr(),-1)

def testAddDay():
	print addDay("2014-12-01",-1)
	print addDay("2014-12-01",31)

def daysDiv(begin,end):
	bt=time.strptime(begin,"%Y-%m-%d")
	et=time.strptime(end,"%Y-%m-%d")
	bit = int(time.mktime(bt))
	eit = int(time.mktime(et))
	return (eit-bit)/86400

def format(dateStr,before,next):
	return time.strftime(next,time.strptime(dateStr,before))

def testDaysDiv():
	print daysDiv("2014-12-01","2015-01-01")

def toIntTime(s,format='%d/%b/%Y:%H:%M:%S'):
	return int(time.mktime(time.strptime(s,format)))

if __name__ == "__main__":
	if len(sys.argv) >= 2:
		if sys.argv[1] == "addDay":
			print addDay(sys.argv[2],int(sys.argv[3]))
		elif sys.argv[1] == "format":
			print format(sys.argv[2],sys.argv[3],sys.argv[4])
	else:
		testAddDay()
		testDaysDiv()

