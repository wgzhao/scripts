#encoding=utf-8

import sys

idReserveForRecipe=3000000

def tag2id():
	lastTid=""
	tag=""
	freq=0
	for line in sys.stdin:
		cols=line.strip().split("\t")
		tid=cols[0]
		if int(tid) < 3000000:
			continue
		if lastTid == "":
			lastTid=tid
		if lastTid != tid:
			if (freq > 0 and tag != "") or tag.startswith("typeid-"):
				print tag+"\t"+lastTid+"\t"+str(freq)
			lastTid=tid
			tag=""
			freq=0
		if len(cols) > 2:
			freq=int(cols[1])
		else:
			tag=cols[1]
	if lastTid != "":
		if freq > 0 and tid != "" and  not tag.startswith("typeid-"):
			print tag+"\t"+lastTid+"\t"+str(freq)

if __name__=="__main__":
	tag2id()


