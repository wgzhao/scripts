import sys

sys.path.append("./")
sys.path.append("../")

def count(f):
	lastId=""
	AB=""
	u=""
	for line in f:
		cols=line.strip().split("\t")
		id=cols[0]
		if lastId == "":
			lastId=id
		if lastId != id:
			if AB != "" and u != "":
				print AB+"\t"+u
			AB=""
			u=""
			lastId=id
		if len(cols) == 3:
			u=cols[1]
		else:
			AB=cols[1]
	if lastId != "":
		if AB != "" and u != "":
			print AB+"\t"+u

if __name__=="__main__":
	count(sys.stdin)

