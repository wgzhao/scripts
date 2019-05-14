#encoding=utf-8

import sys
sys.path.append("./")

import urllib
import itemBank
import conf

subject = u"好豆菜谱"
dataFormat = """{"aps": {"alert": "%s", "content-available": 1}, "vibrator": false, "subject": "%s", "url": "%s", "eventtype": 1, "eventid":"%s"}"""

def getUrl(id,type="album"):
	if type == "album":
		return "haodourecipe://haodou.com/collect/info/?id="+str(id)
	elif type == "recipe":
		if id.startswith("recipe-"):
			id=id[len("recipe-"):]
		return "haodourecipe://haodou.com/recipe/info/?id="+str(id)+"&video=1"
	elif type == "topic":
		tid=id[len("topic-"):]
		sys.stderr.write(str(id)+"\n")
		#tid=id[len("topic-"):]
		#return "haodourecipe://haodou.com/openurl/?url=http://m.haodou.com/app/recipe/topic/view.php?id="+tid
		return "haodourecipe://haodou.com/openurl/?url=http://m.haodou.com/"+str(id)+".html&id="+str(tid)

def packTime(message,send_at):
	return 'send_at='+send_at+'&'+message

def packWithUrl(title,tag,url,send_at):
	data = dataFormat%(title.decode("utf-8").encode('unicode-escape'), subject.encode('unicode-escape'), url,tag)
	body=urllib.quote(data)
	message='expire=172800&api_key=xxxxxx&from=%E7%AE%A1%E7%90%86%E5%91%98&body='+body+'&timestamp=1407725392&to=c3&broadcast=tag&tag='+tag+'&mid='+tag
	if send_at == None:
		return message
	else:
		return packTime(message,send_at)

def tagFromMessage(message):
	tagStr="&tag="
	endStr="&"
	p1=message.find(tagStr)
	p2=message.find(endStr,p1+len(tagStr))
	#print p1,p2
	if p1 < 0:
		return None
	if p2 < 0:
		p2=len(message)
	return message[p1+len(tagStr):p2]

#send_at=20150121160000
def pack(title,id,type="album",send_at=None):
	tag=str(id)
	if send_at != None:
		tag+="_"+send_at
	id=itemBank.removeSpecialMark(id)
	url=getUrl(id,type)
	data = dataFormat%(title.decode("utf-8").encode('unicode-escape'), subject.encode('unicode-escape'), url,tag)
	body=urllib.quote(data)
	message='expire=172800&api_key=xxxxxx&from=%E7%AE%A1%E7%90%86%E5%91%98&body='+body+'&timestamp=1407725392&to=c3&broadcast=tag&tag='+tag+'&mid='+tag
	if send_at == None:
		return message
	else:
		return packTime(message,send_at)

def testPack(send_at):
	print pack("小米粥的30种养胃做法，转给胃不好的人！","topic-266249",type="topic",send_at=send_at)

def packItemBank(send_at=None):
	for line in open(conf.itemBankFile):
		item=itemBank.PushItem(line)
		if item.id != None:
			print pack(item.title,item.id,item.type,send_at)

def packOneItem(id,title,send_at):
	type="album"
	if id.startswith("topic"):
		type="topic"
	if id.startswith("recipe-"):
		type="recipe"
	print pack(title,id,type,send_at)

def testPackWithUrlV49():
	import os
	for line in open("random.conf"):
		if line.startswith("#"):
			continue
		cols=line.strip().split("\t")
		if len(cols) < 3:
			continue
		send_at=cols[0]
		dir="/data/push_tag/"+send_at+"/"
		os.system("mkdir "+dir)
		mf=open(dir+"/message.cmd","w")
		send_at="".join(send_at.split("-"))
		url=cols[1]
		title=cols[2]
		tag="472877_"+send_at+"_v49"
		mf.write("%s\n"%(packWithUrl(title,tag,url,send_at)))
		mf.close()
		#os.system('hive  -e \"select dev_uuid, \'472877_'+send_at+'_v49\' from bing.dw_app_device_ds where last_version in (\'v4.9.0\', \'v4.10.0\',\'4.9.0\',\'4.10.0\')\" > '+dir+'/users.tags')

#
#'haodourecipe://haodou.com/openurl/?url=' . urlencode($url);  网页链接
#"haodourecipe://haodou.com/collect/info/?id=$cid";   专辑
#"haodourecipe://haodou.com/recipe/info/?id=$rid";   菜谱
#
#haodourecipe://haodou.com/openurlid/?id=472877&src=push   #只有4.9以上支持
#
#像这种，属于链接：http://m.haodou.com/mall/index.php?r=wap/goods&id=43
#
#send_at,url,title,tag,urlType(可选)  --------link,album,recipe,orig
def testPackWithUrlType():
	for line in sys.stdin:
		if line.startswith("#"):
			continue
		cols=line.strip().split("\t")
		if len(cols) < 4:
			continue
		send_at=cols[0]
		send_at="".join(send_at.split("-"))
		url=cols[1]
		title=cols[2]
		tag=cols[3]
		if len(cols) >=5:
			urlType=cols[4]
		else:
			if url.find("m.haodou.com") >= 0 \
				or url.find("group.haodou.com") >= 0 \
				or url.find("topic-") >= 0:
				urlType="link"
			elif url.find("album") > 0:
				urlType="album"
			elif url.startswith("http://www.haodou.com/recipe/"):
				urlType="recipe"
			else:
				urlType="orig"
		if urlType == "link":
			url="haodourecipe://haodou.com/openurl/?url="+url
		elif urlType == "album":
			if url.endswith("/"):
				url=url[0:-1]
			pos=url.rfind("/")
			aid=url[pos+1:]
			url="haodourecipe://haodou.com/collect/info/?id="+aid
		elif urlType == "recipe":
			if url.endswith("/"):
				url=url[0:-1]
			pos=url.rfind("/")
			rid=url[pos+1:]
			url="haodourecipe://haodou.com/collect/info/?id="+rid
		print packWithUrl(title,tag,url,send_at)

if __name__ == "__main__":
	if len(sys.argv) >= 2:
		if sys.argv[1] == "testPack":
			testPack("".join(sys.argv[2].split("-")))
		elif sys.argv[1] == "packItem":
			packItemBank("".join(sys.argv[2].split("-")))
		elif sys.argv[1] == "packOneItem":
			if len(sys.argv) >= 6:
				packOneItem(sys.argv[2],sys.argv[3]+" "+sys.argv[4],sys.argv[5])
			else:
				packOneItem(sys.argv[2],sys.argv[3],sys.argv[4])
		elif sys.argv[1] == "testPackWithUrlV49":
			testPackWithUrlV49()
		elif sys.argv[1] == "testPackWithUrlType":
			testPackWithUrlType()
		elif sys.argv[1] == "tagFromMessage":
			for line in sys.stdin:
				print tagFromMessage(line.strip())
	else:
		packItemBank()


