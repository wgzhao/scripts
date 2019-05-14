import sys

sys.path.append("./")

def count(f):
	lastRid=""
	c=0
	for line in f:
		rid=line.strip()
		if lastRid == "":
			lastRid = rid
		if lastRid != rid:
			print str(c)+"\t"+lastRid
			lastRid=rid
			c=0
		c+=1
	if lastRid != "":
		print str(c)+"\t"+lastRid

if __name__=="__main__":
	count(sys.stdin)

