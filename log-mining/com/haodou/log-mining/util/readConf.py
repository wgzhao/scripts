import sys

def readConf(file="Conf.txt"):
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


def getOption(name):
	d=readConf()
	return d[name]

if __name__=="__main__":
	if len(sys.argv) >= 2:
		readOption(sys.argv[1])

