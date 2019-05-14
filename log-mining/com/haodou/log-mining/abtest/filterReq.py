import sys

sys.path.append("./")
sys.path.append("../")

from column import *

def filterLine(line):
	cols=line.strip().split("\t")
	if len(cols) < APP_LOG_COLUMNS4:
		return
	uuid=cols[UUID_ID]
	requestId=getValue(cols[PARA_ID],"request_id")
	if requestId != None and requestId != "":
		print requestId+"\t"+uuid+"\treq"

def filterF(f):
	for line in f:
		filterLine(line)

if __name__=="__main__":
	filterF(sys.stdin)


