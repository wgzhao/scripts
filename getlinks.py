#!/usr/bin/env python3
# -*- coding:utf8 -*-
#encoding:utf8
##
'''
get all links from file or stdin or url
'''

import urllib.request, urllib.error, urllib.parse
import sys
import os
from bs4 import BeautifulSoup as bs
    
def get_content():
    if len(sys.argv) < 2:
        return '\n'.join(sys.stdin.readlines())
        
    if os.path.exists(sys.argv[1]):
        return open(sys.argv[1]).read()
     
    return urllib.request.urlopen(sys.argv[1]).read()
    
    
    
if __name__ == '__main__':
    
    try:
        content = get_content()
    except Exception as err:
        print("cat not get anything %s" % err)
        exit(1)
    soup = bs(content)
    urls=[]
    for url in soup.findAll('a',href=True):
        if url['href'].startswith('http://') or url['href'].startswith('https://'):
            urls.append(url['href'])
    
    #remove duplicate items
    urls = list(set(urls))
    print('\n'.join(urls))
