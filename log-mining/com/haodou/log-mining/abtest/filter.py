import sys

def filterLine(line,method,para):
	cols=line.strip().split("\t")
	if len(cols) < 4:
		sys.stderr.write(line)
		return
	if cols[0] == method and cols[1].find(para) >= 0:
		u=cols[2]
		requestId=cols[3]
		if len(cols) >= 5:
			print requestId+"\t"+u+"\t"+cols[1]+"\t"+cols[4]
		else:
			print requestId+"\t"+u+"\t"+cols[1]

def filterF(f,method,para):
	for line in f:
		filterLine(line,method,para)

if __name__=="__main__":
	filterF(sys.stdin,sys.argv[1],sys.argv[2])


