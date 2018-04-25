import sys

sys.path.append("./")

from analysis import *

def merge():
	daysRet={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		nth=cols[0]
		dr=DayUsersRet.readDaysRet(cols[1:])
		#print dr
		if nth not in daysRet:
			daysRet[nth]=dr
		else:
			daysRet[nth].merge(dr)

	for i in daysRet:
		#print str(i)+"\t"+str(daysRet[i].userNum)
		print str(i)+"\t"+daysRet[i].printStr()

if __name__=="__main__":
	merge()



