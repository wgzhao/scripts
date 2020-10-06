#!/usr/bin/env python
from sys import argv
__DOC__ = '''Convert your 15bit old ID card into 18bit ID Card'''
__Author__ = 'wgzhao d2d6aGFvQGdtYWlsLmNvbQo='
crc=[ '1','0','X','9','8','7','6','5','4','3','2' ] #CRC bit
weight=[ 7,9,10,5,8,4,2,1,6,3,7,9,10,5,8,4,2 ] #weight factor

def idcard(oldcard):
    #insert two number "19" after sixth,born latter than 2000 ignore
    tmpcard = oldcard[:6] + "19" + oldcard[6:]
    y = 0
    for i in range(17):
        y += int(tmpcard[i]) * weight[i]
    return tmpcard + crc[(y % 11)]

def usage():
    print(argv[0] + " <oldcard>")
    print("e.g " + argv[0] + " 110201640506718")

def check(oldcard):
    '''valid oldcard '''
    if len(oldcard) != 15:
        return False
    try:
        int(oldcard) #only digital ?
    except(ValueError):
        return False
    return True

if __name__ == '__main__':
    if len(argv) < 2 or not check(argv[1]):
        usage()
        exit(1)
    oldcard = argv[1]
    print("old card is " + oldcard)
    print("new card is " + idcard(oldcard))
