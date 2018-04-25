#encoding=utf-8
import sys
sys.path.append("./")
import geoip2.database

import userInfo

ipreader = geoip2.database.Reader('./GeoLite2-City.mmdb')

def ip2s(ip):
	try:
		ret = ipreader.city(ip)
	except geoip2.errors.AddressNotFoundError,ex:
		return "other"
	if 'zh-CN' not in ret.subdivisions.most_specific.names:
		return "void"
		#print ret.subdivisions.most_specific.names
	return ret.subdivisions.most_specific.names['zh-CN'].encode('utf8') #省份

def uuid2region(): #/user/zhangzhonghui/userModel/userInfo/2015-12-21/*
	for line in sys.stdin:
		if not line.startswith(userInfo.Uuid_Fix):
			continue
		cols=line.strip().split("\t")
		uuid=cols[0][len(userInfo.Uuid_Fix):]
		if not userInfo.UuidPattern.match(uuid):
			sys.stderr.write(uuid+"\n")
			continue
		if cols[1] == userInfo.Ip_Type:
			ip=cols[3].strip()
			#print line,ip
			province=ip2s(ip)
			if province != None and province != "":
				print uuid+"\t"+province

if __name__=="__main__":
	#print ip2s('113.240.253.251')
	uuid2region()

