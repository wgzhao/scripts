#encoding=utf-8

import sys
import os

def change(f1,f2):
	wf=open(f2,"w")
	n=0
	openV=0
	high=0
	low=100000
	close=0
	vol=0
	day=""
	for line in open(f1):
		cols=line.strip().split("\t")
		day=cols[0]
		v1=float(cols[1])
		v2=float(cols[2])
		v3=float(cols[3])
		close=float(cols[4])
		vol+=float(cols[5])
		if n %5 == 4:
			wf.write("%s\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n"%(day,openV,high,low,close,vol))
			high=0
			low=100000
			vol=0
		if n % 5 == 0:
			openV=v1
		if v2 > high:
			high=v2
		if v3 < low:
			low=v3
		n+=1
	if n > 0 and n %5 != 4:
		wf.write("%s\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n"%(day,openV,high,low,close,vol))
	wf.close()

def test():
	dir="./stock_day"
	weekDir="./stock_week"
	for file in os.listdir(dir):
		change(dir+"/"+file,weekDir+"/"+file)

if __name__=="__main__":
	test()


