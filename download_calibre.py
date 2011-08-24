#!/usr/bin/python -tt
# -*- coding:utf-8 -*-
import urllib2
import os,sys
if os.getuid() != 0:
  print 'use root account'
  sys.exit(2)
exec urllib2.urlopen('http://status.calibre-ebook.com/linux_installer').read();
main()

