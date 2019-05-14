import sys

def readMethod(f=open("hitMethod.txt")):
	ms={}
	for line in f:
		line=line.strip()
		if line.startswith("#"):
			continue
		cols=line.split("\t")
		if len(cols) < 1:
			continue
		method=cols[0]
		if len(method) == 0:
			continue
		name=method
		if len(cols) >= 2:
			name=cols[1]
		type=0
		if len(cols) >= 3:
			type=int(cols[2])
		ms[method]=(name,type)
	return ms

if __name__=="__main__":
	ms=readMethod()
	for m in ms:
		print m,ms[m]

