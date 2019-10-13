#!/usr/bin/env python3
"""
QQwry IP address utils
"""
import sys
import os
import struct
import zlib
import requests

class QQwry():
    """
    QQ IP address database 
    """
    start_ip = 0
    end_ip = 0
    country = ''
    local = ''
    country_flag = 0
    first_startip = 0
    last_startip = 0
    end_ipoff = 0

    def __init__(self, dbfile='./qqwry.dat'):
        if not os.path.exists(dbfile):
            print(f"{dbfile}: no such file, try to download")
            self.get_qqwry(dbfile)
        self.fd = open(dbfile, 'rb')

    def ip2int(self, ip):
        """
        convert ip address to int
        """
        return sum(map(lambda x, y: int(y) << x ,[24,16,8,0],ip.split('.')))

    def int2ip(self, ipint):
        """
        convert int-formal ip to ip address string
        """
        return '.'.join(map(lambda x, y: str(y >> x & 0x00ff), \
                    [24,16,8,0],[ipint, ipint, ipint, ipint])
                )

    def get_startip(self, record):
        offset = self.first_startip + record * 7
        self.fd.seek(offset, 0)
        buf = self.fd.read(7)
        self.end_ipoff = buf[4] + buf[5] <<8 + buf[6] << 16
        self.start_ip = buf[0] + buf[1] << 8 + buf[2] << 16 + buf[3] << 24
        return self.start_ip

    def get_endip(self):
        self.fd.seek(self.end_ipoff, 0)
        buf = self.fd.read(5)
        self.end_ip = buf[0] |  buf[1] << 8 |  buf[2] <<16 | buf[3] <<24
        self.country_flag = buf[4]
        return self.end_ip
    
    def get_flag(self, offset):
        flag = 0
        while True:
            self.fd.seek(offset, 0)
            flag = self.fd.read(1)
            if flag in [1,2]:
                buf = self.fd.read(3)
                if flag == 2:
                    self.country_flag = 2
                    self.end_ipoff = offset - 4
                offset = buf[0] + buf[1] << 8 + buf[2] << 16 
            else:
                break
        if offset < 12:
            return ''
        self.fd.seek(offset, 0)
        return self.getstr()

    def getstr(self):
        s_str = ''
        while True:
            c = self.fd.read(1)
            if c == 0:
                break
            s_str += str(c)
        return s_str

    def get_country(self):
        if self.country_flag in [1,2]:
            self.country = self.get_flag(self.end_ipoff+4)
            if 1 == self.country_flag:
                self.local = ''
            else:
                self.local = self.get_flag(self.end_ipoff + 8)
        else:
            self.country = self.get_flag(self.end_ipoff + 4)
            self.local = self.get_flag(self.fd.tell())

    def ip_local(self,ip):
        """
        get the real location in earth according an ip address
        """
        ipint = self.ip2int(ip)
        self.fd.seek(0, 0)
        buff = self.fd.read(8)
        # first_startip = self.ip2int('.'.join([str(x) for x in buff[::-1][:4]]))
        # last_startip = self.ip2int('.'.join([str(x) for x in buff[::-1][4:]]))
        self.first_startip = buff[0] | buff[1] <<8  | buff[2] << 16 | buff[3] <<24
        self.last_startip  = buff[4] | buff[5] <<8 |  buff[6] <<16  | buff[7] <<24
        record_num = int( ( self.last_startip - self.first_startip ) / 7 )
        if record_num < 2:
            print("corrupt dbfile")
            self.fd.close()
            sys.exit(11)
        
        # 2-way search
        ret = None
        search_begin = 0
        search_end = record_num
        while True:
            print(f"begin:{search_begin} \t end:{search_end}")
            idx = int( ( search_begin + search_end ) / 2)
            self.get_startip(idx)
            if ipint == self.start_ip:
                search_begin = idx
                break
            if ipint > self.start_ip:
                search_begin = idx
            else:
                search_end = idx
            
        self.get_startip(search_begin)
        self.get_endip()
        if (( self.start_ip <= ipint) and ( self.end_ip >= ipint)):
            ret = 0
            self.get_country()
        else:
            ret = 3
            self.country = 'unknown'
            self.local = ''
        self.fd.close()
        return ret

    def get_qqwry(self, outfile):
        """
        download qqwry data
        """
        #get metadata
        meta_url = 'http://update.cz88.net/ip/copywrite.rar'
        try:
            data = requests.get(meta_url).content
        except:
            print(f"failed to get metadata from {meta_url}")
            sys.exit(1)
        
        if len(data) < 24 and data[:4] != b'CZIP':
            print(f"invalid metadata, try again")
            sys.exit(2)
        
        # extract from metadata
        version, info1, size, info2, key = struct.unpack_from('<IIIII', data, 4)
        if info1 != 1:
            print(f"failed to extract metadata")
        print(f"current version : {version}")

        # begin download real ip database
        db_url = 'http://update.cz88.net/ip/qqwry.rar'
        # it may takes a few mintues
        try:
            data = requests.get(db_url).content
        except:
            print(f"failed to download ip database from {db_url}")
            sys.exit(3)
        
        if size != len(data):
            print(f"metadata tell me size is {size}, but real size is {len(data)}")
            sys.exit(4)
        
        # generate decrypt key
        head = bytearray(0x200)
        for i in range(0x200):
            key = (key * 0x805 + 1) & 0xff
            head[i] = data[i] ^ key
        #concat
        data = head + data[0x200:]

        #decompress
        try:
            result = zlib.decompress(data)
        except:
            print(f"failed to decompress")
            sys.exit(5)
        
        # write to dbfile
        open(outfile,'wb').write(result)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(("Usage {} <ip address>".format(sys.argv[0])))
        sys.exit(1)
    ip = sys.argv[1]
    qq = QQwry(dbfile = './qqwry.dat')
    qq.ip_local(ip)