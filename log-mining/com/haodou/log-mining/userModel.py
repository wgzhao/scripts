import sys
sys.path.append("./")

from column import *

fac=1.0
if len(sys.argv) >= 2:
	fac=float(sys.argv[1])

sys.stderr.write("fac:"+str(fac)+"\n")

def actionCount():
	users={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		userid=uuid(cols)
		if userid == None:
			continue
		if userid == "":
			sys.stderr.write(line)
			continue
		t=getTag(cols)
		if t == None:
			continue
		if t[0] == None or t[0] == "":
			continue
		n=int(t[1])
		for i in range(n):
			print userid+"\t"+t[0]

if __name__=="__main__":
	actionCount()


