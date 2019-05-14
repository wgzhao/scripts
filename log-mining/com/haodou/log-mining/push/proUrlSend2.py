#encoding=utf-8

import sys
import os

sys.path.append("./")
sys.path.append("../log/")
sys.path.append("../util")

import queryTag
import itemBank
import package
import Md5Util

import conf

 # 4 TagPushMessageFileName="message.cmd"
#    5 TagPushUsersFileName="users.tags"

def readUrlSend2(file,date):
	bank=itemBank.readItemBank(file)
	if not os.path.exists(conf.TagPushDataDir+date):
		os.mkdir(conf.TagPushDataDir+date)
	mwf=open(conf.TagPushDataDir+date+"/"+conf.TagPushMessageFileName,"w")
	w2cwf=open(conf.Tag2ClassFile,"w")
	w2cs={}
	for id in bank:
		item=bank[id]
		#print item.title,"\t".join(item.tags.keys())
		url=package.packUrl(item.url)
		title=item.title
		mid=Md5Util.toMd5(title+"\t"+url)
		send_at="".join(date.split("-"))
		mwf.write(package.packWithUrl(title,mid,url,send_at)+"\n")
		try:
			itemTags=queryTag.getNeedTags(title+"\t"+"\t".join(item.tags.keys()))
		except:
			print "\t".join(item.tags.keys()).strip()
		for t in item.tags:
			itemTags[t]=1
		for t in itemTags:
			if t not in w2cs:
				w2cs[t]={}
			if mid not in w2cs[t]:
				w2cs[t][mid]=1
			else:
				w2cs[t][mid]+=1
	mwf.close()
	for t in w2cs:
		w2cwf.write(t+"\t"+"\t".join(w2cs[t].keys())+"\n")
	w2cwf.close()

if __name__=="__main__":
	#readUrlSend2("urlSend2.txt","2015-12-18")
	readUrlSend2(sys.argv[2],sys.argv[1])
