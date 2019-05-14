import sys

sys.path.append("./")
sys.path.append("../")

import queryTag

allmts={}

def output(lastU,ts,ms):
	HasTag=(len(ts) > 0)
	HasHit=False
	ts["VOID_TAG"]=1
	for message in ms:
		if message in allmts:
			mts=allmts[message]
		else:
			mts={}
			tags={}
			queryTag.match(message.decode("utf-8"),tags,0,len(message.decode("utf-8")),True)
			for sub in tags:
				if sub[1] in queryTag.needWords:
					mts[sub[1].encode("utf-8")]=1
				for tag in tags[sub]:
					if tag in queryTag.needWords:
						mts[tag.encode("utf-8")]=1
					if tag in queryTag.tagm:
						for t in queryTag.tagm[tag]:
							if t in queryTag.needWords:
								mts[t.encode("utf-8")]=1
			allmts[message]=mts
		rv="push"
		if "pushview" in ms[message]:
			rv="view"
			if "push_received" not in ms[message]:
				continue
		hit=False
		for tag in ts:
			if tag in mts:
				hit=True
				print tag+"\tHIT\t"+rv+"\t"+lastU+"\t"+message
			else:
				print tag+"\tNOT\t"+rv+"\t"+lastU+"\t"+message
		if HasTag:
			for tag in mts:
				if tag in ts:
					print "C_"+tag+"\tHIT\t"+rv+"\t"+lastU+"\t"+message
				else:
					print "C_"+tag+"\tNOT\t"+rv+"\t"+lastU+"\t"+message
		if hit:
			HasHit=True
		if HasTag:
			if hit:
				print "ALL_TAG\tHIT\t"+rv+"\t"+lastU+"\t"+message
			else:
				print "ALL_TAG\tNOT\t"+rv+"\t"+lastU+"\t"+message
	if HasTag and HasHit:
		for message in ms:
			mts=allmts[message]
			rv="push"
			if "pushview" in ms[message]:
				rv="view"
				if "push_received" not in ms[message]:
					continue
			hit=False
			for tag in ts:
				if tag in mts:
					hit=True
			for tag in mts:
				if tag in ts:
					print "HC_"+tag+"\tHIT\t"+rv+"\t"+lastU+"\t"+message
				else:
					print "HC_"+tag+"\tNOT\t"+rv+"\t"+lastU+"\t"+message
			if hit:
				print "HIT_ALL_TAG\tHIT\t"+rv+"\t"+lastU+"\t"+message
			else:
				print "HIT_ALL_TAG\tNOT\t"+rv+"\t"+lastU+"\t"+message
		
#u	tag	tc	sum
#u	received/view	message
#
def join():
	lastU=""
	ts={}
	ms={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 3:
			continue
		u=cols[0]
		if lastU == "":
			lastU = u
		if lastU != u:
			output(lastU,ts,ms)
			lastU=u
			ts={}
			ms={}
		#exclude version 4
		if len(cols) >= 5 and cols[4].startswith("4"):
			continue
		if len(cols) == 4:
			tag=cols[1]
			tc=int(cols[2])
			sum=float(cols[3])
			ts[tag]=tc/sum
		else:
			method=cols[1]
			message=cols[2]
			time=0
			if len(cols) >= 5:
				time=int(cols[3])
			if message not in ms:
				ms[message]={}
			ms[message][method]=time
	if lastU != "":
		output(lastU,ts,ms)

def viewMessageTag(allmts):
	for m in allmts:
		sys.stderr.write(m+"\n")
		for tag in allmts[m]:
			sys.stderr.write("\t"+tag+"\n")

if __name__=="__main__":
	join()
	#viewMessageTag(allmts)
	#for m in allmts:
	#	print m
	#	for tag in allmts[m]:
	#		print "\t",tag,allmts[m][tag]


