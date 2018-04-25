#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import geoip2.database

class GeoIP2Reader(object):
  def __init__(self, libfile=None):
    self.geoip2reader = None
    self.unknownstr, self.unknownstr_cn = 'UNKNOWN', '未知'
    if libfile is None:
      if '__file__' in locals().keys():
        mmdbfile = "%(curdir)s/GeoLite2-City.mmdb" % {'curdir':os.path.dirname(os.path.abspath(__file__))}
      else:
        mmdbfile = "%(curdir)s/GeoLite2-City.mmdb" % {'curdir':os.getcwd()}
    else:
      mmdbfile = libfile
    self.geoip2reader = geoip2.database.Reader(mmdbfile)
  def close(self):
    if self.geoip2reader:
      self.geoip2reader.close()
      self.geoip2reader = None
  def ip2city(self, ip):
    if self.geoip2reader is None:
      self.__init__()
    result = {}
    try:
      ipinfo = self.geoip2reader.city(ip)
    except geoip2.errors.AddressNotFoundError:
      result = {'city':self.unknownstr, 'city_cn':self.unknownstr_cn, 'province':self.unknownstr, 'province_cn':self.unknownstr_cn, 'country':self.unknownstr, 'country_cn':self.unknownstr_cn, 'error':'NOTFOUND'}
      return result
    if ipinfo.country.name is None:
      result = {'city':self.unknownstr, 'city_cn':self.unknownstr_cn, 'province':self.unknownstr, 'province_cn':self.unknownstr_cn, 'country':self.unknownstr, 'country_cn':self.unknownstr_cn, 'error':'NOTFOUND'}
      return result
    else:
      cur_country    = ipinfo.country.name.replace(" ","").replace("`","'")
      cur_country_cn = ipinfo.country.names.get('zh-CN',cur_country).replace(" ","").replace("`","'")
    if ipinfo.subdivisions.most_specific.name is None:
      cur_province    = cur_country
      cur_province_cn = cur_country_cn
    else:
      cur_province    = ipinfo.subdivisions.most_specific.name.replace(" ","").replace("`","'")
      cur_province_cn = ipinfo.subdivisions.most_specific.names.get('zh-CN',cur_province).replace(" ","").replace("`","'")
    if ipinfo.city.name is None:
      cur_city    = cur_province
      cur_city_cn = cur_province_cn
    else:
      cur_city    = ipinfo.city.name.replace(" ","").replace("`","'")
      cur_city_cn = ipinfo.city.names.get('zh-CN',cur_city).replace(" ","").replace("`","'")
    result = {'city':cur_city, 'city_cn':cur_city_cn, 'province':cur_province, 'province_cn':cur_province_cn, 'country':cur_country, 'country_cn':cur_country_cn, 'error':'None'}
    #按国情调整
    #geoip2将港澳台视为国家一级，将其国家调整为中国
    chinaarea = ["HongKong","Macao","Taiwan"]
    if result['country'] in chinaarea:
      result['province'] = result['country']
      result['province_cn'] = result['country_cn']
      result['country'] = 'China'
      result['country_cn'] = '中国'
    #geoip2将直辖市的区县视为市一级，调整为市
    chinadc = ["BeijingShi","ShanghaiShi","TianjinShi","ChongqingShi","HongKong","Macao"]
    if result['province'] in chinadc:
      result['city'] = result['province']
      result['city_cn'] = result['province_cn']
    return result

if __name__=='__main__':
  if len(sys.argv)==1:
    iplist = ['202.130.159.50', '124.207.105.90', '58.32.39.255', '221.238.175.167', \
              '58.17.136.1', '103.30.128.0', '61.56.48.17', '202.175.122.227', \
              '123.150.200.216', '8.8.8.8', '114.114.114.114', '127.0.0.1', '192.168.1.1']
  else:
    iplist, n, i = [], len(sys.argv), 1
    while i<n:
      iplist.append(sys.argv[i])
      i += 1
  reader = GeoIP2Reader()
  for ip in iplist:
    ipinfo = reader.ip2city(ip)
    print ip, ipinfo['city'], ipinfo['city_cn'], ipinfo['province'], ipinfo['province_cn'], ipinfo['country'], ipinfo['country_cn'], ipinfo['error']
