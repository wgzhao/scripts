#encoding=utf-8

import sys

sys.path.append("./")
sys.path.append("../util/")

import FileUtil

def readVideo():
	d={}
	for line in FileUtil.openUncertainDirFile(["../readDB/","./","./readDB/,/home/data/recomData_tmp"],"recipe.video.txt"):
		cols=line.strip().split()
		if len(cols) < 2:
			continue
		if cols[0].endswith("L"):
			cols[0]=cols[0][0:-1]
		d[cols[0]]=cols[1]
		d[int(cols[0])]=cols[1]
	return d


