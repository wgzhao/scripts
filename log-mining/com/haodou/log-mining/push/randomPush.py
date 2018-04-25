#encoding=utf-8

import sys
import random
import os

def generateUser(dayForUser,sampleRate=0.5):
	rwf=open("/data/push_tag/randomUser/random.txt","w")
	cwf=open("/data/push_tag/randomUser/compare.txt","w")
	for line in open("/data/push_tag/"+dayForUser+"/users.tags"):
		uuid=line.strip().split()[0]
		if random.random() <= sampleRate:
			rwf.write(uuid+"\n")
		else:
			cwf.write(uuid+"\n")
	rwf.close()
	cwf.close()

def match(mid,title,pushDay,userFile="/data/push_tag/randomUser/random.txt"):
	if pushDay == None or pushDay == "":
		sys.stderr.write("没有指定推送日期！")
		return
	os.system("mkdir /data/push_tag/"+pushDay)
	utp=open("/data/push_tag/randomUser/userTags.txt","w")
	utNoDate=open("/data/push_tag/"+pushDay+"/users.tags.noDate","w")
	for line in open(userFile):
		uuid=line.strip().split()[0]
		utp.write("%s\t%s\tdefault\n"%(uuid,mid))
		utNoDate.write("%s\t%s\n"%(uuid,mid))
	utp.close()
	utNoDate.close()
	os.system("hdfs dfs -rm -r /user/zhangzhonghui/logcount/push/userTags/"+pushDay+"/*")
	os.system("hdfs dfs -mkdir /user/zhangzhonghui/logcount/push/userTags/"+pushDay)
	os.system("hdfs dfs -rm -r /user/zhangzhonghui/logcount/push/user_mid/"+pushDay+"/*")
	os.system("hdfs dfs -mkdir /user/zhangzhonghui/logcount/push/user_mid/"+pushDay)
	os.system("hdfs dfs -put /data/push_tag/randomUser/userTags.txt /user/zhangzhonghui/logcount/push/userTags/"+pushDay+"/")
	os.system("hdfs dfs -put /data/push_tag/"+pushDay+"/users.tags.noDate /user/zhangzhonghui/logcount/push/user_mid/"+pushDay+"/")
	os.system("cat /data/push_tag/"+pushDay+"/users.tags.noDate | python packDate.py "+pushDay+ "> /data/push_tag/"+pushDay+"/users.tags")
	os.system("rm /data/push_tag/"+pushDay+"/users.tags.noDate")
	os.system("python package.py packOneItem "+mid+" "+ title +" "+"".join(pushDay.split("-"))+" > /data/push_tag/"+pushDay+"/message.cmd")
	os.system("hdfs dfs -rm -r /user/zhangzhonghui/logcount/push/message/"+pushDay+"/*")
	os.system("hdfs dfs -mkdir /user/zhangzhonghui/logcount/push/message/"+pushDay)
	os.system("hdfs dfs -put /data/push_tag/"+pushDay+"/message.cmd /user/zhangzhonghui/logcount/push/message/"+pushDay+"/")

def append(mid,title,pushDay,userFile="/data/push_tag/randomUser/random.txt"):
	if pushDay == None or pushDay == "":
		sys.stderr.write("没有指定推送日期！")
		return
	name=userFile[userFile.rfind("/")+1:]
	utp=open("/data/push_tag/randomUser/userTags."+name,"w")
	utNoDate=open("/data/push_tag/"+pushDay+"/users.tags.noDate."+name,"w")
	for line in open(userFile):
		uuid=line.strip().split()[0]
		utp.write("%s\t%s\t%s\n"%(uuid,mid,name))
		utNoDate.write("%s\t%s\n"%(uuid,mid))
	utp.close()
	utNoDate.close()
	os.system("hdfs dfs -rm -r /user/zhangzhonghui/logcount/push/userTags/"+pushDay+"/userTags."+name)
	os.system("hdfs dfs -put /data/push_tag/randomUser/userTags."+name+" /user/zhangzhonghui/logcount/push/userTags/"+pushDay+"/")
	os.system("hdfs dfs -rm -r /user/zhangzhonghui/logcount/push/user_mid/"+pushDay+"/users.tags.noDate."+name)
	os.system("hdfs dfs -put /data/push_tag/"+pushDay+"/users.tags.noDate."+name+" /user/zhangzhonghui/logcount/push/user_mid/"+pushDay+"/")
	os.system("cat /data/push_tag/"+pushDay+"/users.tags.noDate."+name+" | python packDate.py "+pushDay+ ">> /data/push_tag/"+pushDay+"/users.tags")
	os.system("rm /data/push_tag/"+pushDay+"/users.tags.noDate."+name)
	os.system("python package.py packOneItem "+mid+" "+ title +" "+"".join(pushDay.split("-"))+" >> /data/push_tag/"+ pushDay+"/message.cmd")
	os.system("hdfs dfs -rm -r /user/zhangzhonghui/logcount/push/message/"+pushDay+"/*")
	os.system("hdfs dfs -put /data/push_tag/"+pushDay+"/message.cmd /user/zhangzhonghui/logcount/push/message/"+pushDay+"/")

def matchCompare(mid,title,pushDay,userFile="/data/push_tag/randomUser/compare.txt",appendMessage=False):
	if pushDay == None or pushDay == "":
		sys.stderr.write("没有指定推送日期！")
		return
	name=userFile[userFile.rfind("/")+1:]
	utp=open("/data/push_tag/randomUser/userTags."+name,"w")
	for line in open(userFile):
		uuid=line.strip()
		utp.write("%s\t%s\t%s\n"%(uuid,mid,name))
	utp.close()
	os.system("hdfs dfs -mkdir /user/zhangzhonghui/logcount/push/userTags/"+pushDay)
	os.system("hdfs dfs -put /data/push_tag/randomUser/userTags."+name+" /user/zhangzhonghui/logcount/push/userTags/"+pushDay+"/")
	if appendMessage:
		os.system("python package.py packOneItem "+mid+" "+ title +" "+"".join(pushDay.split("-"))+" >> /data/push_tag/"+pushDay+"/message.cmd")
		os.system("hdfs dfs -mkdir /user/zhangzhonghui/logcount/push/message/"+pushDay)
		os.system("hdfs dfs -rm -r /user/zhangzhonghui/logcount/push/message/"+pushDay+"/*")
		os.system("hdfs dfs -put /data/push_tag/"+pushDay+"/message.cmd /user/zhangzhonghui/logcount/push/message/"+pushDay+"/")

import itemBank
def days(userFile="/data/push_tag/randomUser/random.txt"):
	for line in open("random.conf"):
		line=line.strip()
		if line.startswith("#"):
			continue
		cols=line.strip().split("\t")
		if len(cols) < 3:
			continue
		(pushDay,url,title)=cols[0:3]
		if url.startswith("http:"):
			(type,mid)=itemBank.parseUrl(url)
		else:
			mid=url
		print pushDay,mid,title
		if len(cols) >= 5 and cols[4] == "append":
			print "append!"
			append(mid,title,pushDay,"/data/push_tag/randomUser/"+cols[3]+".txt")
		elif len(cols) >= 4 and cols[3]=="nutrition":
			match(mid,title,pushDay,"/data/push_tag/randomUser/nutrition.txt")
		elif len(cols) >= 4 and (cols[3].startswith("nutrition") or cols[3].startswith("woman")): #更新用户群
			match(mid,title,pushDay,"/data/push_tag/randomUser/"+cols[3]+".txt")
		elif len(cols) >= 4 and cols[3] == "-nutrition":
			print "minus nutrition!"
			matchCompare(mid,title,pushDay,"/data/push_tag/randomUser/random-nutrition.txt",True)
			matchCompare(mid,title,pushDay,"/data/push_tag/randomUser/compare-nutrition.txt",True)
		else:
			match(mid,title,pushDay,userFile)
			matchCompare(mid,title,pushDay)

def addNutrition():
	for line in open("random.conf"):
		line=line.strip()
		if line.startswith("#"):
			continue
		cols=line.strip().split("\t")
		if len(cols) < 3:
			continue
		(pushDay,url,title)=cols[0:3]
		if url.startswith("http:"):
			(type,mid)=itemBank.parseUrl(url)
		else:
			mid=url
		#print pushDay,mid,title
		tday="".join(pushDay.split("-"))
		os.system("python package.py packOneItem "+mid+" "+ title +" "+tday+" >> /data/push_tag/"+ pushDay+"/message.cmd")
		wf=open("/data/push_tag/"+ pushDay+"/users.tags","w")
		us={}
		userFile="/data/push_tag/randomUser/nutrition"+tday+".txt"
		for line in open(userFile):
			cols=line.strip().split("\t")
			if len(cols) < 1:
				continue
			u=cols[0]
			us[u]=1
		for line in open("/data/push_tag/"+ pushDay+"/users.tags.bak"):
			cols=line.strip().split("\t")
			if len(cols) < 2:
				continue
			u=cols[0]
			if u in us:
				wf.write("%s\t%s_%s\n"%(u,mid,tday))
			else:
				wf.write(line)
		wf.close()

if __name__=="__main__":
	if sys.argv[1] == "generate":
		generateUser("2015-03-17")
	elif sys.argv[1] == "match":
		match(sys.argv[2],sys.argv[3])
	elif sys.argv[1] == "days":
		if len(sys.argv) < 3:
			days()
		else:
			days(sys.argv[2])
	elif sys.argv[1] == "addNutrition":
		addNutrition()


