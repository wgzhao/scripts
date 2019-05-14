import sys

sys.path.append("./")
import dictInfo

def output(lastm,us,vs):
	s=lastm
	info=dictInfo.info(us)
	print lastm+"\t"+str(info)
	info=dictInfo.info(vs)
	print "v_"+lastm+"\t"+str(info)

if __name__=="__main__":
	lastm=""
	us={}
	vs={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		m=cols[0]
		if lastm == "":
			lastm=m
		if lastm != m:
			output(lastm,us,vs)
			lastm=m
			us={}
		u=cols[1]
		if u not in us:
			us[u]=1
		else:
			us[u]+=1
		if len(cols) >= 3:
			v=cols[2]
			if v not in vs:
				vs[v]=1
			else:
				vs[v]+=1

	if lastm != "":
		output(lastm,us,vs)

