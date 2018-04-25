import sys
import os

sys.path.append("../log")
import clickCount

def titleHitInfo():
	ack=clickCount.Click()
	if True:
		for line in sys.stdin:
			cols=line.strip().split("\t")
			k=cols[0]
			if not (k.startswith("ck45") or k.startswith("ck44")):
				continue
			#print line
			(k,click)=clickCount.readClick(cols)
			ack.merge(click)
	ack.printHitTitleHitRate()

if __name__=="__main__":
	#titleHitInfo("/home/zhangzhonghui/data/backup.1209/")
	titleHitInfo()

