import sys
sys.path.append("./")
sys.path.append("./util")

import FileUtil
TMP_DATA_DIR="/home/data/recomeData_tmp/"

from column import *
from boundedQueue import *

MAX_COOCUUR_NUM=15

rs={}
for line in FileUtil.openUncertainDirFile(["./",TMP_DATA_DIR],"quality.rid"):
	rs[line.strip()]=1

def coOccur():
	users={}
	userTags={}
	for line in sys.stdin:
		cols=line.split("\t")
		if len(cols) < APP_LOG_COLUMNS:
			#print line.strip()
			#print len(cols)
			continue
		userid=uuid(cols)
		#cols[USER_CID]
		#if userid == '0':
		#	userid=cols[IP_CID]
		rid=getRid(cols)
		#sys.stdout.write(line+"\n")
		#sys.stdout.write(cols[PARA_ID]+"\n")
		#sys.stdout.write(rid+"\n")
		if rid not in rs:
			rid=""
		if rid != "" and (userid not in users or not users[userid].contains(rid)):
			if userid in userTags:
				for tag in userTags[userid].list:
					if tag != rid:
						print tag+"\t"+rid

		t=getTag(cols)
		if t != None and (userid not in userTags or not userTags[userid].contains(t[0])):
			tag=t[0]
			if userid in users:
				for rid in users[userid].list:
					if tag != rid:
						print tag+"\t"+rid
		if rid != "":
			if userid not in users:
				users[userid]=Queue(MAX_COOCUUR_NUM)
			users[userid].add(rid)
		if t != None:
			if userid not in userTags:
				userTags[userid]=Queue(MAX_COOCUUR_NUM)
			tag=t[0]
			userTags[userid].add(tag)


if __name__=="__main__":
	coOccur()


