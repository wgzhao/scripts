import sys

sys.path.append("./")
sys.path.append("../log/")

import dictInfo

def outputa(lastMethod,paras,paraUs):
	allu={}
	for p in paras:
		print lastMethod+"-"+p+"\t"+str(dictInfo.info(paraUs[p]))
		for u in paraUs[p]:
			if u not in allu:
				allu[u]=paraUs[p][u]
			else:
				allu[u]+=paraUs[p][u]
		paraUs[p]={}
	print lastMethod+"\t"+str(dictInfo.info(allu))

def output(lastMethod,paras,paraRs):
	outputa(lastMethod+"_rid",paras,paraRs)

def reduce(f):
	lastMethod=""
	paras={}
	paraRs={}
	for line in f:
		cols=line.strip().split("\t")
		if len(cols) < 4:
			sys.stderr.write(line)
			continue
		method=cols[0]
		if lastMethod == "":
			lastMethod=method
		if lastMethod != method:
			output(lastMethod,paras,paraRs)
			lastMethod=method
			paras={}
			paraRs={}
		p=cols[1]
		if p not in paras:
			paras[p]=0
			paraRs[p]={}
		u=cols[2]
		q=cols[3]
		if len(cols) >= 5:
			paras[p]+=1
			r=cols[4]
			if r not in paraRs[p]:
				paraRs[p][r]=0
			paraRs[p][r]+=1
	if lastMethod != "":
		output(lastMethod,paras,paraRs)

if __name__=="__main__":
	reduce(sys.stdin)

