#encoding=utf-8

import hashlib   

def toMd5(src):
	m2 = hashlib.md5()   
	m2.update(src)   
	return m2.hexdigest()   

