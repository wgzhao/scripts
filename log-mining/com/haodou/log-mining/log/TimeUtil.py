import sys
import time
import datetime

def days(begin,end):
	bt=time.strptime(begin,"%Y-%m-%d")
	et=time.strptime(end,"%Y-%m-%d")
	bit = int(time.mktime(bt))
	eit = int(time.mktime(et))
	ds=[]
	while bit <= eit:
		begin = time.strftime("%Y-%m-%d",bt)
		ds.append(begin)
		bdt = datetime.datetime.fromtimestamp(bit) 
		bdt += datetime.timedelta(days = 1)
		bit = int(time.mktime(bdt.timetuple()))
		bt = time.localtime(bit)
	return ds


if __name__=="__main__":
	print days("2013-12-28","2014-01-01")

