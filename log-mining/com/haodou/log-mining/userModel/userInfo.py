#encoding=utf-8
#汇总一个用户的基本信息：通过ip计算地区，uid到uuid的映射
#

import sys
sys.path.append("./")
sys.path.append("../")

import re
 
UuidPattern=re.compile(r"[0-9a-zA-Z\\-]{32}")

import column

Uid_Fix="uid-"
Uuid_Fix="uuid-"
Uid_Type="uid"
U_Type="u"
Channel_Type="channel"
Ip_Type="ip"

Reduce_Cols=4

types={
	Uid_Type:1,
	U_Type:1,
	Channel_Type:1,
	Ip_Type:1
}

'''
def map():
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) == Reduce_Cols and cols[1] in types: #过去的数据累加
			print line
			continue
		if not column.valid(cols):
			continue
		uid=column.uid(cols)
		if uid == None:
			uid=""
		uuid=column.uuidOnly(cols)
		if uuid == None:
			uuid=""
		ip=cols[column.IP_CID]
		time=cols[column.TIME_CID]
		channel=cols[column.MEDIA_CID]
		if uid != "":
			print Uid_Fix+uid+"\tall\t"+time+"\t"+uuid+"\t"+channel+"\t"+ip
		if uuid != "":
			print Uuid_Fix+uuid+"\tall\t"+time+"\t"+uid+"\t"+channel+"\t"+ip
'''

def output(lastU,d):
	for t,(v,time) in d.items():
		v=v.strip()
		if len(v) > 0:
			print lastU+"\t"+t+"\t"+time+"\t"+v

def addItem(t,v,time,d):
	if v == None or len(v) == 0: #忽略空字段
		return
	if t not in d or time > d[t][1]:
		d[t]=[v,time]

def addItem2(t,v,time,us,u):
	if u == None or u == "":
		return
	if u not in us:
		us[u]={}
	addItem(t,v,time,us[u])

def map():
	us={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) == Reduce_Cols and cols[1] in types: #过去的数据累加
			print line.strip()
			continue
		if not column.valid(cols):
			continue
		uid=column.uid(cols)
		uuid=column.uuidOnly(cols)
		ip=cols[column.IP_CID]
		time=cols[column.TIME_CID]
		channel=cols[column.MEDIA_CID]
		if uid != None and len(uid) !=0:
			addItem2(U_Type,uuid,time,us,Uid_Fix+uid)
			addItem2(Channel_Type,channel,time,us,Uid_Fix+uid)
			addItem2(Ip_Type,ip,time,us,Uid_Fix+uid)
		if uuid != None and len(uuid) !=0:
			addItem2(U_Type,uid,time,us,Uuid_Fix+uuid)
			addItem2(Channel_Type,channel,time,us,Uuid_Fix+uuid)
			addItem2(Ip_Type,ip,time,us,Uuid_Fix+uuid)
	for u in us:
		output(u,us[u])


def reduce():
	lastU=""
	d={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 4:
			sys.stderr.write(line)
			continue
		u=cols[0]
		t=cols[1]
		time=cols[2]
		if lastU == "":
			lastU=u
		if lastU != u:
			output(lastU,d)
			lastU=u
			d={}
		addItem(t,cols[3],time,d)

	if lastU != "":
		output(lastU,d)

if __name__=="__main__":
	if sys.argv[1] == "map":
		map()
	elif sys.argv[1] == "reduce":
		reduce()

