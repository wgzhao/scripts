#!/usr/bin/env python
# -*- coding:utf8 -*-
#encoding:utf8
##
'''
get all links from file or stdin or url
'''

import urllib2
import sys
import os
reload(sys)
sys.setdefaultencoding('utf-8')
try:
    from BeautifulSoup import BeautifulSoup as bs
except ImportError:
    print "Install BeautifulSoup first"
    sys.exit(2)
    
def get_content():
    if len(sys.argv) < 2:
        return '\n'.join(sys.stdin.readlines())
        
    if os.path.exists(sys.argv[1]):
        return open(sys.argv[1]).read()
     
    return urllib2.urlopen(sys.argv[1]).read()
    
    
    
if __name__ == '__main__':
    
    try:
        content = get_content()
    except Exception,err:
        print "cat not get anything %s" % err
        exit(1)
    soup = bs(content)
    urls=[]
    for url in soup.findAll('a',href=True):
        if url['href'].startswith('http://'):
            urls.append(url['href'])
    
    #remove duplicate items
    urls = list(set(urls))
    print '\n'.join(urls)
