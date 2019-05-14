#encoding=utf-8

import sys
sys.path.append("../util")
import columnUtil
import PushName

def readView(f,mid):
	#消息
	ms={}
	for line in f:
		uuid=columnUtil.getStrValue(line,"uuid")
		if uuid == None:
			continue
		appid=columnUtil.getStrValue(line,"appid")
		if appid == None:
			sys.stderr.write(line)
			continue
		message=columnUtil.getStrValue(line,"message")
		if message == None:
			continue
		message=columnUtil.escapeUtf8(message)
		#if uuid == "bfce144e15d3a3cc19bda3946fd7d503":
		#	sys.stderr.write(uuid+"\t"+message+"\t"+str(message in mid)+"\n")
		if message in mid:
			if uuid not in ms:
				ms[uuid]=[]
			ms[uuid].append((mid[message],appid))
	return ms


#;:{&quot;alert&quot;:&quot;\u3010\u5149\u68cd\u8282\u7279\u4f9b\u3011\u7b80\u5355\u4e00\u62db\uff0c\u5403\u8d27\u6559\u4f60\u8fc5\u901f\u8131\u5355~&quot;,&quot;content-available&quot;:1}
def readSended(f):
	mid={}
	for line in f:
		cols=line.strip().split("\t")
		u=cols[0]
		body=cols[1]
		s=body.find(";: &quot;")
		slen=len(";: &quot;")
		if s < 0:
			s=body.find(";:&quot;")
			slen=len(";:&quot;")
		e=body.find("&quot;",s+slen)
		if s > 0 and e > 0:
			message=columnUtil.escapeUnicode(body[s+slen:e]).encode("utf-8") #
		else:
			message=body
			#sys.stderr.write("no message: "+str(s)+":"+str(e)+":"+line)
			#continue
		s=body.find("/?id=")
		e=body.find("&quot;",s+len("/?id="))
		if s > 0 and e > s and body[s:e].find("&") < 0:
			id=body[s+len("/?id="):e]
		else:
			s=body.find("topic")
			if s > 0:
				e=body.find(".",s)
				e1=body.find("'",s)
				if e1 > s and e1 < e:
					e=e1
				if e > s:
					id=body[s:e]
				else:
					id=message
			else:
				id=message
		if message not in mid:
			mid[message]=id
	return mid
	'''
	cutMid={}
	for message in mid:
		max=0
		maxId=""
		for id in mid[message]:
			if mid[message][id] > max:
				max=mid[message][id]
				maxId=id
		cutMid[message]=maxId
	'''
	return (us,mid)

def readReceive(f,mid):
	us={}
	for line in f:
		cols=line.strip().split("\t")
		if len(cols) < 4: #有些私信消息过长，会到第二行
			continue
		u=cols[0]
		message=cols[1]
		appid=cols[2]
		if message in mid:
			id = mid[message]
		else:
			id=message
		if u not in us:
			us[u]=[]
		us[u].append((id,appid))
	return us

def testReadLog(date):
	dataDir="/home/zhangzhonghui/data/push/"+date+"/"
	f=open(dataDir+"sendedLog.txt")
	mid=readSended(f)
	f.close()
	#m="别拿健康开玩笑，这些菜千万不能带！"
	#if m not in mid:
	#	mid[m]="topic-329950_20150611"
	f=open(dataDir+"receiveLog.txt")
	rs=readReceive(f,mid)
	f.close()
	f=open(dataDir+"pushview.txt")
	ms=readView(f,mid)
	f.close()
	wf=open(dataDir+"log.txt","w")
	for u in ms:
		for id,appid in ms[u]:
			wf.write(u+"\t"+id+"\t"+appid+"\n")
	for u in rs:
		for id,appid in rs[u]:
			if id in mid:
				id=mid[id]
			wf.write(u+"\t"+id+"\t"+appid+"\treceive\n")
	wf.close()

def testReadSended():
	date="2015-03-24"
	dataDir="/home/zhangzhonghui/data/push/"+date+"/"
	f=open(dataDir+"sendedLog.txt")
	mid=readSended(f)
	f.close()
	for message in mid:
		if message != mid[message]:
			print message,mid[message]

if __name__=="__main__":
	if sys.argv[1] == "send":
		testReadSended()
	else:
		testReadLog(sys.argv[1])


