#encoding=utf-8
import sys


def strNone(s):
	if s == None:
		return "\t#"
	return "\t"+str(s)

def notVoid(a):
	if a != None and a != "":
		return True
	return False	

def choose(a,b):
	if a == None or a == "":
		if b != None:
			return b
	return a

class UserInfo(object):
	def __init__(self,ip):
		self.ip=ip
		self.media=None
		self.deviceid=None
		self.uuid = None
		self.uid=None
		self.version=None
		self.appid=None
		self.update={}

	def name(self):
		if self.uuid != None and self.uuid != "":
			return self.uuid
		if self.uid != None and self.uid != "":
			return self.uid
		if self.deviceid != None and self.deviceid != "":
			return self.deviceid
		if self.ip != None and self.ip != "":
			return self.ip
		return "#"

	def nameGivenIP(self):
		if self.uid != None and self.uid != "":
			return self.uid
		if self.uuid != None and self.uuid != "":
			return self.uuid
		if self.deviceid != None and self.deviceid != "":
			return self.deviceid
		if self.media != None and self.media != "":
			return self.media
		if self.version != None and self.version != "":
			return self.vesion
		return "#"

	def sameUserGivenIP(self,other):
		#return 0
		if notVoid(self.uid) and notVoid(other.uid):
			if self.uid == other.uid:
				return 1
			else:
				return -1
		if notVoid(self.uuid) and notVoid(other.uuid):
			if self.uuid == other.uuid:
				return 1
			else:
				return -1
		if notVoid(self.deviceid) and notVoid(other.deviceid):
			if self.deviceid == other.deviceid:
				return 1
			else:
				return -1
		if notVoid(self.version) and notVoid(other.version):
			if self.version != other.version:
				return -1
		return 0
	
	def merge(self,u):
		self.uid = choose(self.uid,u.uid)
		self.uuid = choose(self.uuid,u.uuid)
		self.version =choose(self.version,u.version)
		self.media=choose(self.media,u.media)
		self.deviceid=choose(self.deviceid,u.deviceid)
		self.ip=choose(self.ip,u.ip)

	def __str__(self):
		return self.name()+"\t-1"+strNone(self.uuid)+strNone(self.uid)+strNone(self.version)+strNone(self.deviceid)+strNone(self.ip)+strNone(self.media)

	@staticmethod
	def readUserInfo(cols):
		ip=cols[6]
		u=UserInfo(ip)
		u.uuid=cols[2]
		u.uid=cols[3]
		u.version=cols[4]
		u.deviceid=cols[5]
		if len(cols) >= 8:
			u.media=cols[7]
		return u


