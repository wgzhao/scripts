#encoding=utf-8

import sys

#
#uuid.daojia.20151029.txt
#
#uuid.txt
def daojia1029():
	#tag1029="daojia_jira_10581_20151029"
	tag1029="daojia_zhangfei_20151111"
	for line in open("uuid.daojia.20151029.txt"):
		cols=line.strip().split()
		uuid=cols[1]
		print uuid+"\t"+tag1029
	
if __name__=="__main__":
	daojia1029()




