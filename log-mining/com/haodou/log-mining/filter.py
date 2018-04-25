import sys
sys.path.append("./")

AN=100
sumThre=10.0
if len(sys.argv) >= 2:
	sumThre=float(sys.argv[1])

def output(lastA,list,sum):
	lastA+="\t"+str(sum)
	for Bv in list:
		lastA+="\t"+Bv
	print lastA

def filter(all):
	lastA=""
	list=[]
	sum=0
	has=all
	for line in sys.stdin:
		cols=line.strip().split("\t")
		A=cols[0]
		if lastA == "":
			lastA=A
		if lastA != A:
			if has and sum >= sumThre:
				output(lastA,list,sum)
			lastA=A
			list=[]
			sum=0
			has=all
		B=cols[1]
		if len(cols) == 5:
			has=True
		intv=int(cols[3])
		sum+=int(float(cols[2]))
		if len(list) < AN:
			list.append(B+"\t"+cols[3])

	if lastA != "":
		if has and sum >= sumThre:
			output(lastA,list,sum)

if __name__=="__main__":
	if len(sys.argv) >= 3:
		filter(True)
	else:
		filter(False)


