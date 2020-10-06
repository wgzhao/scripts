#!/usr/bin/env python
import sys,os,string
"""
extract pictures from PowerPoint files.
the picture format includes jpeg,gif and png
wgzhao(at)gmail.com

http://blog.wgzhao.com
"""

headers=[("JFIF", 6, "jpg"), ("GIF", 0, "gif"), ("PNG", 1, "png")]
marker=[]

def usage():
	print("Usage: " + os.path.split(sys.argv[0])[1] + " ")
	print("\t\tfile must be powerpoint format at present!")

def getpic(filename="",prefix="img"):
	try:
		fid = open(filename, 'rb')
	except:
		sys.exit(1)
	numlin = len(fid.readlines())
	fid.seek(0)
	i = 0; s = 0
	curlin = fid.readline()
	while i < numlin:
		for flag,offset, ext in headers:
			index = string.find(curlin,flag)
			if index < 0:
				continue
			else:
				pos = s + index -offset
				marker.append((pos, ext))
		s = s + len(curlin)
		curlin = fid.readline()
		i += 1
		print(i, end=' ')
		fid.seek(0)
		j = len(marker)
		imgnum = 0
		if j == 0:
			print("No images included in the document")
			sys.exit(1)
		for i in range(0, j):
			if i == j-1:
				info = marker[i]
				thispos = info[0]
				thisext = info[1]
				nextpos = s
				gap = nextpos - thispos
				fid.seek(thispos)
				data = fid.read(gap)
				imgname = "%s%02d.%s" % (prefix,i, thisext)
				fid1 = open(imgname, 'wb')
				fid1.write(data)
				fid1.close()
				imgnum += 1
			else:
				info = marker[i]
				thispos = info[0]
				thisext = info[1]
				nextinfo = marker[i+1]
				nextpos = nextinfo[0]
				gap = nextpos - thispos
				fid.seek(thispos)
				data = fid.read(gap)
				imgname = "%s%02d.%s" % (prefix,i, thisext)
				fid1 = open(imgname, 'wb')
				fid1.write(data)
				fid1.close()
				imgnum += 1
				fid.close()
	print("%02d imgaes have been extracted from file %s\n" % (imgnum ,filename))
	
if __name__ == "__main__":
	if len(sys.argv[1:]) >0:
		filelist=sys.argv[1:]
		while filelist:
			filename=filelist.pop()
			prefix=os.path.splitext(os.path.split(filename)[1])[0]
			getpic(filename,prefix)
	else:
		usage()
		sys.exit(1)