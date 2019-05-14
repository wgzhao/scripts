import sys

NameKey="__name__"
NameSplit=":"

def readParaSetting(file):
	ms={}
	for line in open(file):
		line=line.strip()
		if len(line) == 0:
			continue
		cols=line.strip().split()
		ss=cols[0].strip().split(NameSplit)
		method=ss[0]
		methodName=ss[0]
		if len(ss) > 1:
			methodName=ss[1]
		ms[method]={}
		ms[method][NameKey]=methodName
		for i in range(1,len(cols)):
			if len(cols[i]) == 0:
				continue
			ss=cols[i].split(NameSplit)
			if len(ss) == 0:
				continue
			dimension=ss[0]
			dimensionName=ss[0]
			if len(ss) > 1:
				dimensionName=ss[1]
			ms[method][dimension]=dimensionName
	return ms

