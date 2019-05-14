#encoding=utf-8

import sys

sys.path.append("../")
sys.path.append("./")

import column


def uids():
	us={}
	for line in open("nutriUid.txt"):
		line=line.strip()
		if len(line) == 0:
			continue
		p=line[0:-1].rfind("/")
		uid=line[p+1:-1]
		#print uid
		us[uid]={}
	return us


def ips():
	us=uids()
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < column.APP_LOG_COLUMNS:
			continue
		uid=column.uid(cols)
		if uid == "":
			continue
		if uid not in us:
			continue
		ip=cols[column.IP_CID]
		if ip not in us[uid]:
			us[uid][ip]=[1,0]
		else:
			us[uid][ip][0]+=1
	wf=open("nutriUidIps.txt","w")
	for uid in us:
		print uid+"\t"+str(us[uid])
		wf.write(uid+"\t"+str(us[uid])+"\n")
	wf.close()

def readUIps():
	us={}
	for line in open("nutriUidIps.txt"):
		cols=line.strip().split("\t")
		uid=cols[0]
		ips=eval(cols[1])
		us[uid]=ips
	return us

def uidVisit():
	us=readUIps()
	ip2u={}
	for uid in us:
		for ip in us[uid]:
			if ip not in ip2u:
				ip2u[ip]=[]
			ip2u[ip].append(uid)
	for line in sys.stdin:
		cols=line.strip().split("\01")
		if len(cols) < 6:
			continue
		ip=cols[0]
		if ip not in ip2u:
			continue
		if cols[4].find("topic-329950.") > 0:
			for uid in ip2u[ip]:
				us[uid][ip][1]+=1
	wf=open("nutriUidVisit.txt","w")
	for uid in us:
		sum=0
		for ip in us[uid]:
			sum+=us[uid][ip][1]
		print uid+"\t"+str(sum)+"\t"+str(us[uid])
		wf.write(uid+"\t"+str(sum)+"\t"+str(us[uid])+"\n")
	wf.close()

if __name__=="__main__":
	if sys.argv[1] =="ips":
		ips()
	elif sys.argv[1] == "uids":
		uids()
	elif sys.argv[1] == "uidVisit":
		uidVisit()



