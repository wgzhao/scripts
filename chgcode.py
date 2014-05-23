#!/usr/bin/python -tt
# -*- encoding:utf-8 -*-
import os,sys
import chardet
'''
将指定目录下的文件或者目录统一按照指定的编码方式编码，默认是utf-8
用法
chcode <dir> [encode]
chcode ./ gb2312
'''

def usage():
    print '''
    将指定目录下的文件或者目录统一按照指定的编码方式编码，默认是utf-8
    用法
    chcode <dir> [encode]
    chcode ./ gb2312
    '''
    sys.exit(1)
    
def chcode(dir,encodinf='utf-8'):
    for f in os.listdir(dir):
        if os.path.isdir(f):
            chcode("%s/%s" % (dir,f),encodinf)
        charset = chardet.detect(f)['encoding']
        print charset
        fname = unicode(f,charset)
        os.rename(f,unicode.encode(fname,encoding))
        
if __name__ == '__main__':
    if len(sys.argv) < 2:
        usage()
    else:
        os.chdir(sys.argv[1])
        if len(sys.argv) >2:
            encoding = sys.argv[2]
        else:
            encoding = 'utf-8'
        chcode('./',encoding)
