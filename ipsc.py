#!/usr/bin/env python
#-*- coding:utf-8 -*-
__Author__ = "wgzhao(wgzhao AT gmail DOT com)"
import sys
import struct
import socket
from math import floor
from os import SEEK_SET

"""
根据提供的ip地址查询归属地
IP地址库依赖纯真IP地址库，最新版可以从网络上下载

"""


class TQQwry:

    StartIP = 0 
    EndIP = 0 
    Country = '' 
    Local = '' 
    CountryFlag = 0 
    '''标识 Country位置 
	
	  // 0x01, 随后3字节为Country偏移, 没有Local 
	
	  // 0x02, 随后3字节为Country偏移，接着是Local 
	  // 其他, Country, Local, Local有类似的压缩。可能多重引用。 
    '''
    fp = None
    nRet = ''
    FirstStartIp = 0 
    LastStartIp = 0 
    EndIpOff = 0  
    QQWRY = "qqwry.dat"  #根据自己文件的实际路径修改
    
    
    def IpToInt(self, Ip):
		return struct.unpack('I', struct.pack('I', (socket.ntohl(struct.unpack('I', socket.inet_aton(Ip))[0]))))[0]
        
    def IntToIp(self, Int):
	  	return socket.inet_ntoa(struct.pack('I', socket.htonl(Int))) 
	  
    def  getStartIp (self, RecNo): 
        offset = self.FirstStartIp + int(RecNo * 7) 
        self.fp.seek(offset, SEEK_SET) 
        buf = self.fp.read(7)  
        self.EndIpOff = ord(buf[4]) + (ord(buf[5]) * 256) + (ord(buf[6]) * 256 * 256) 
        self.StartIp = ord(buf[0]) + (ord(buf[1]) * 256) + (ord(buf[2]) * 256 * 256) + (ord(buf[3]) * 256 * 256 * 256) 


    def  getEndIp (self):
        self.fp.seek(self.EndIpOff)
        buf = self.fp.read(5)  
        self.EndIp = ord(buf[0]) + (ord(buf[1]) * 256) + (ord(buf[2]) * 256 * 256) + (ord(buf[3]) * 256 * 256 * 256) 
        self.CountryFlag = ord (buf[4])  



    def  getCountry (self):
        if (self.CountryFlag == 1 or self.CountryFlag == 2):
            self.Country = self.getFlagStr (self.EndIpOff + 4)  
            if (self.CountryFlag == 1):
                self.Local = ''
            else:
                self.Local = self.getFlagStr (self.EndIpOff + 8)
        else:
            self.Country = self.getFlagStr (self.EndIpOff + 4)  
            self.Local = self.getFlagStr (self.fp.tell ())  

    def  getFlagStr (self, offset) :
        flag = 0  
        while (1): 
            self.fp.seek(offset, SEEK_SET) 
            flag = ord (self.fp.read(1))  
            if (flag == 1 or flag == 2):
                buf = self.fp.read(3)  
                if (flag == 2):
                    self.CountryFlag = 2  
                    self.EndIpOff = offset - 4  
                
                offset = ord(buf[0]) + (ord(buf[1]) * 256) + (ord(buf[2]) * 256 * 256) 
            else:
               break 
            
         
        if (offset < 12): 
            return '' 
        self.fp.seek(offset, SEEK_SET) 
        return self.getStr() 
     

    def  getStr (self):
        str = '' 
        while (1): 
            c = self.fp.read(1) 
            if (ord (c[0]) == 0):
               break 
            str = str + c  
        
        return str
    

    def  __init__ (self, dotip):
        ip = self.IpToInt (dotip) 
        self.fp = open(self.QQWRY, "rb") 
        if (not self.fp):
            print "OpenFileError" 
            sys.exit(1)
           
        self.fp.seek(0)
        buf = self.fp.read (8)
        self.FirstStartIp = ord(buf[0]) + (ord(buf[1]) * 256) + (ord(buf[2]) * 256 * 256) + (ord(buf[3]) * 256 * 256 * 256) 
        self.LastStartIp = ord(buf[4]) + (ord(buf[5]) * 256) + (ord(buf[6]) * 256 * 256) + (ord(buf[7]) * 256 * 256 * 256)
        
        RecordCount = floor((self.LastStartIp - self.FirstStartIp) / 7) 
        if (RecordCount <= 1):
            self.Country = "FileDataError"
            self.fp.close()
            sys.exit(2)
        
        RangB = 0 
        RangE = RecordCount 
        #Match ... 
        while (RangB < RangE - 1): 
        
          RecNo = floor((RangB + RangE) / 2) 
          self.getStartIp (RecNo) 
          if (ip == self.StartIp):
               RangB = RecNo  
               break  
            
          if (ip > self.StartIp): 
            RangB = RecNo 
          else:
            RangE = RecNo 
        
        self.getStartIp (RangB)  
        self.getEndIp ()  
        if ((self.StartIp <= ip) and (self.EndIp >= ip)):
            self.nRet = 0  
            self.getCountry ()  
        else:
            self.nRet = 3  
            self.Country = '未知'  
            self.Local = ''  
         
        self.fp.close()
         
     

if __name__ == "__main__":
	
    if (len(sys.argv) < 2):
        print "Usage: ipsc.py <ip address>"
        sys.exit(65)
    wry = TQQwry(sys.argv[1])
	#地址库里信息是gbk编码，程序是utf8编码，如果是乱码，注意你保存文件的编码格式
    print "Country:  ", wry.Country.decode('gbk'), " Local:  ", wry.Local.decode('gbk')
