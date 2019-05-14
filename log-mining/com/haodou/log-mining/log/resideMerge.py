#encoding=utf-8

import sys
sys.path.append("./")
sys.path.append("../")

import os
import dictInfo

dirname="./"
if len(sys.argv) >= 2:
	dirname=sys.argv[1]
fix="reside"
if len(sys.argv) >= 3:
	fix=sys.argv[2]
files=os.listdir(dirname)
files=sorted(files)
print "日期\t平均停留时间\t总停留时间\t用户数\t最大停留时间\t最小停留时间\t修正的总时间\t修正的平均时间"
for file in files:
	if file.startswith(fix):
		day=file
		if file.find("201") > 0:
			pos=file.find("201")
			day=file[pos:]
		f=open(dirname+"/"+file)
		minfo=dictInfo.mergeInfo(f)
		f.close()
		if minfo != None:
			if len(minfo) == 0:
				continue
			print day+"\t"+str(minfo["avg"])+"\t"+str(minfo["sum"])+"\t"+str(minfo["userNum"])+"\t"+str(minfo["max"])+"\t"+str(minfo["min"])+"\t"+str(minfo["csum"])+"\t"+str(minfo["cavg"])
		

