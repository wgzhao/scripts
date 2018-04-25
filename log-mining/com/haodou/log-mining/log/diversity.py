import sys

sys.path.append("./")
sys.path.append("../")

from column import *

def ridCount(f):
	ts={}
	for line in f:
		cols=line.strip().split("\t")
		if len(cols) < APP_LOG_COLUMNS:
			continue
		rid=getRid(cols)
		if rid == None or rid == "":
			continue
		if len(rid) > 20:
			sys.stderr.write(line)
			continue
		method=cols[METHOD_CID]
		if not method.startswith("info.getinfo"):
			continue
		print rid

if __name__=="__main__":
	ridCount(sys.stdin)

