import sys

for line in sys.stdin:
	cols=line.strip().split("\t")
	try:
		sn=int(cols[1])
		if sn < 100:
			continue
		s=line.find("albumHit:")
		e=line.find("\t",s)
		an=int(line[s+len("albumHit:"):e])
		s=line.find("topicHit:")
		e=line.find("\t",s)
		tn=int(line[s+len("topicHit:"):e])
		if tn >= an*0.8:
			print cols[0],sn,an,tn
	except:
		continue

