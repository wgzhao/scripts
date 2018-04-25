import sys
sys.path.append("./")

from column import *


def actionCount():
	users={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		userid=user(cols)
		if userid == None:
			continue
		if userid == "":
			sys.stderr.write(line)
			continue
		rid=getRid(cols)
		if rid != "":
			print userid+"\t"+rid

if __name__=="__main__":
	actionCount()


