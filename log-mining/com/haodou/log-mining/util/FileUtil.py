#encoding=utf-8

import sys
import os

#
#一个文件位于多个可能的目录，遍历目录，直到找到为止
#
def openUncertainDirFile(dirs,fileName):
	for d in dirs:
		if os.path.isfile(d+"/"+fileName):
			return open(d+"/"+fileName)
	return []

