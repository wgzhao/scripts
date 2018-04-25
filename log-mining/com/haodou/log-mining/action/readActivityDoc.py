#encoding=utf-8

api2name={}
for line in open("./activitydoc.txt"):
	line=line.strip()
	if line.startswith("名称："):
		api=line[len("名称："):]
	elif line.startswith("注解："):
		name=line[len("注解："):].split()[0]
		api2name[api]=name


#for api in api2name:
#	print api+"\t"+api2name[api]

