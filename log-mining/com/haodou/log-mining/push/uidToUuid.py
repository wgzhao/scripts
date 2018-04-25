#encoding=utf-8

def toUuid(uids):
	for line in open("/home/data/recomData_tmp/uid2uuid.txt"):
		cols=line.strip().split("\t")
		uid=cols[0]
		uuid=cols[1]
		if uid in uids:
			print uuid+"\t"+uids[uid]

def notRegister(tag="8eed53ea78abc35cdccb433649346d7f"): #2015-12-23推送消息生成的MD5
	uids={}
	for line in open("/home/data/recomData_tmp/userPhone.txt"):
		cols=line.strip().split("\t")
		if len(cols) < 2:
			continue
		if cols[1] == "None":
			uids[cols[0]]=tag
	toUuid(uids)

if __name__=="__main__":
	notRegister() #python uidToUuid.py > /data/push_tag/2015-12-23/users.tags

