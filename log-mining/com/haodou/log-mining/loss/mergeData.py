import sys

sys.path.append("./")

def readUser(f=open("user.txt")):
	us={}
	for line in f:
		cols=line.strip().split("\t")
		if len(cols) < 2:
			sys.stderr.write(line)
			continue
		us[cols[0]]=cols[1]
	return us

def merge():
	us=readUser()
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 4:
			continue
		u=cols[0]
		if u not in us:
			continue
		id=cols[1]
		if id == "-1":
			print line
		else:
			t=cols[1]
			cols[1]=cols[2]
			cols[2]=t
			print "\t".join(cols)

if __name__=="__main__":
	merge()

