import sys

def readConf(file="ABConf.txt"):
	d={}
	for line in open(file):
		line=line.strip()
		if len(line) == 0:
			continue
		ss=line.split()
		if len(ss) < 2:
			continue
		d[ss[0].strip()]=ss[1]
	return d

def getStartTime():
	d=readConf()
	if "startTime" in d:
		print d["startTime"]
	else:
		print ""

def getABOption():
	d=readConf()
	if "ABOption" in d:
		abs=d["ABOption"].split(",")
		for ab in abs:
			print ab


if __name__=="__main__":
	if len(sys.argv) >= 2:
		if sys.argv[1] == "startTime":
			getStartTime()
		elif sys.argv[1] == "ABOption":
			getABOption()

