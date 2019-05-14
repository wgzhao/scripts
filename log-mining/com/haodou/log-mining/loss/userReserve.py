#encoding=utf-8

import sys
sys.path.append("./")
sys.path.append("../")
import column

def readUsers():
	try:
		users={}
		for line in open("user.txt"):
			cols=line.strip().split("\t")
			users[cols[0]]="1"
			if len(cols) >= 2:
				t=cols[1]
				users[cols[0]]=t
	except:
		return None
	if len(users) == 0:
		return None
	return users
	
stopMethod={
	"mobiledevice.initandroiddevice":1,
	"ad.getadinmobi":1,
	"ad.getad_imocha":1,
	"ad.getad_imochav2":1
}

#输入app log
#
def count(users):
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < column.APP_LOG_COLUMNS:
			continue
		method=cols[column.METHOD_CID]
		if method in stopMethods:
			continue
		uuid=column.uuidOnly(cols)
		if uuid == None or uuid == "":
			continue
		if users == None:
			print uuid
		elif uuid in users:
			print uuid+"\t"+str(users[uuid])
			del users[uuid]

def reduce():
	lastU=""
	v=""
	for line in sys.stdin:
		cols=line.strip().split("\t")
		u=cols[0]
		if lastU == "":
			lastU=u
		if lastU != u:
			print lastU+"\t"+v
			lastU=u
			v=""
		if len(cols) >= 2:
			v=cols[1]
	if lastU != "":
		print lastU+"\t"+v

def countReserveRate():
	users=readUsers()
	ts={}
	for u in users:
		t=users[u]
		if t not in ts:
			ts[t]=1
		else:
			ts[t]+=1
	tr={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		u=cols[0]
		if u in users:
			t=users[u]
			if t not in tr:
				tr[t]=1
			else:
				tr[t]+=1
	for t in ts:
		s=ts[t]
		r=0
		if t in tr:
			r=tr[t]
		if s == 0:
			sv=1e-32
		else:
			sv=float(s)
		print "%s\t%d\t%d\t%.3f"%(t,s,r,r/(sv))

if __name__=="__main__":
	if sys.argv[1] == "getUser":
		count(None)
	elif sys.argv[1] == "reserve":
		users=readUsers()
		count(users)
	elif sys.argv[1] == "reduce":
		reduce()
	elif sys.argv[1] == "rate":
		countReserveRate()


