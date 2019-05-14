#encoding=utf-8

import sys

sys.path.append("./")

if __name__=="__main__":
	today=sys.argv[1]
	userNum=0
	newNum=0
	for line in open("/home/zhangzhonghui/data/reg/newUser.txt"):
		cols=line.strip().split()
		if len(cols) <= 0:
			continue
		if cols[0] != today:
			continue
		userNum+=int(float(cols[2]))
		newNum+=int(float(cols[3]))
	for line in open("/home/zhangzhonghui/data/reside/all.reside"):
		cols=line.strip().split()
		if len(cols) <= 0:
			continue
		if cols[0] == today and userNum <= 1:
			userNum=int(cols[3])
	if userNum==0:
		userNum=1
	regNum=0
	newRegNum=0
	for line in sys.stdin:
		cols=line.strip().split()
		day=cols[0]
		if day == today:
			newRegNum=int(cols[1])
		regNum+=int(cols[1])
	print "%s\t%d\t%d\t%d\t%d\t%.4f\t%d\t%.4f"%(today,userNum,newNum,regNum,newRegNum,float(newRegNum)/(regNum+1e-12),regNum-newRegNum,float(regNum-newRegNum)/(userNum+1e-12))


