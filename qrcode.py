#!/usr/bin/env python
# -*- coding: utf-8 -*-
 
'''generate and decode qrcode pictures'''
 
import qrcode as pqr
import sys
 
def gen_qrpic(text, file):
    '''generate a qrcode file contains the given text'''
    stat, img = pqr.encode(text)
    if stat:
        img.save(file)
        return file
    else:
        print "failed to generate qrcode picture."
def qrdecode(file):
    '''decode the qrcode picture'''
    stat, text = pqr.decode(file)
    if stat:
        return text
    else:
        print "failed to decode the qrcode picture."
 
if __name__ == "__main__":
    args = sys.argv
    if len(args) == 1 or args[1] == '-h' or args[1] == '--help':
        print "encode: " + args[0] + " -e [text] [image file]"
        print "decode: " + args[0] + " -d [imgae file]"
    elif args[1] == '-e':
        try:
            print "generated file: " + gen_qrpic(args[2],args[3])
        except:
            pass
    elif args[1] == '-d':
        try:
            print "get text: " + qrdecode(args[2])
        except:
            pass
    else:
        print 'unrecoded arguments'
