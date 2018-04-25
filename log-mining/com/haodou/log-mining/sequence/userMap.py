#encoding=utf-8

'''
统计ip，设备id，uuid之间的映射比例。
比如平均一个ip映射到多少个uuid，平均一个uuid映射到多少个ip
'''

import sys

sys.path.append("./")
sys.path.append("../")

import column


def count():
	ips={}
	uuids={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if not column.valid(cols):
			continue
		uuid=column.uuid(cols)
		ip=cols[column.IP_CID]
		if ip not in ips:
			ips[ip]={}
		if uuid not in uuids:
			uuids[uuid]={}
		ips[ip][uuid]=1
		uuids[uuid][ip]=1
	sum=0
	for ip in ips:
		sum+=len(ips[ip])
	print "ipNum\t%d\tipUuidNum\t%d\tuuidPerIp\t%.4f"%(len(ips),sum,float(sum)/len(ips))
	sum=0
	for uuid in uuids:
		sum+=len(uuids[uuid])
	print "uuidNum\t%d\tuuidIpNum\t%d\tipPerUuuid\t%.4f"%(len(uuids),sum,float(sum)/len(uuids))


if __name__=="__main__":
	count()


