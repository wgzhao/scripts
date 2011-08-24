#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys, os
from ID3 import ID3, InvalidTagError
import chardet

version = 1.2
name = 'id3-tagger.py'

def _smartcode(stream):
    '''
    convert everything into utf-8 code
    '''
    codedetect = chardet.detect(stream)["encoding"]
    try:
        ustring = unicode(ustring, codedetect)
        return "%s %s"% ("",ustring.encode('utf8'))
    except:
        return False
    
    
def titlecrt(filename):
    try:
        id3info = ID3(filename)
        for k,v in id3info.items():
            struf8 = _smartcode(v)
            if struf8:
                id3info[k] = struf8
            else:
                continue
        id3info.write()

    except InvalidTagError, msg:
        print "Invalid ID3 tag:", msg
        pass

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'directory is must'
        sys.exit(2)
    flist = os.listdir(sys.argv[1])
    flist = [ os.path.join(sys.argv[1],f) for f in flist ]
    
    for f in flist:
        titlecrt(f)
    
