import sys

sys.path.append("./")
sys.path.append("../")

import datetime

def getHour(time):
	return datetime.datetime.fromtimestamp(time).hour

def getWeekDay(time):
	return datetime.datetime.fromtimestamp(time).weekday()


def output(lastU,channel,ms,rs,vs):
	for message in ms:
		if "push_received" in ms[message]:
			time=ms[message]["push_received"]
			hour="hour%d"%(getHour(time))
			weekday="week%d"%(getWeekDay(time))
			chour=channel+"\t"+hour
			if channel not in rs:
				rs[channel]=1
				vs[channel]=0
			else:
				rs[channel]+=1
			if chour not in rs:
				rs[chour]=1
				vs[chour]=0
			else:
				rs[chour]+=1
			if hour not in rs:
				rs[hour]=1
				vs[hour]=0
			else:
				rs[hour]+=1
			if weekday not in rs:
				rs[weekday]=1
				vs[weekday]=0
			else:
				rs[weekday]+=1
			if "pushview" in ms[message]:
				vs[hour]+=1
				vs[weekday]+=1
				vs[chour]+=1
				vs[channel]+=1

		
#u	tag	tc	sum
#u	received/view	message
#
def join():
	rs={}
	vs={}
	lastU=""
	ms={}
	channel=""
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 5:
			continue
		u=cols[0]
		if lastU == "":
			lastU = u
		if lastU != u:
			output(lastU,channel,ms,rs,vs)
			lastU=u
			ts={}
			ms={}
		if cols[4].startswith("4"):
			continue
		method=cols[1]
		message=cols[2]
		time=int(cols[3])
		channel=cols[5]
		if message not in ms:
			ms[message]={}
		if method in ms[message]:
			if time > ms[message]:
				ms[message][method]=time
			else:
				sys.stderr.write(line)
		else:
			ms[message][method]=time
	if lastU != "":
		output(lastU,channel,ms,rs,vs)
	for c in rs:
		r=rs[c]
		v=0
		if c in vs:
			v=vs[c]
		print "%s\t%d\t%d\t%f"%(c,v,r,float(v)/(r+1e-16))


if __name__=="__main__":
	join()

