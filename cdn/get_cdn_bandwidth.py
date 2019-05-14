#!/usr/bin/env python
# -*- coding:utf-8 -*-
#获取CDN的带宽信息，包括峰值，平均值等
#hive table
# create table bing.cdn_bandwidth (
# domain  string comment 'domain name',
# maxbw   float  comment 'maximum bandwidth,unit is Mbps',
# minbw   float  comment 'minimum  bandwidth,unit is Mbps',
# avgbw   float  comment 'average bandwith,unit is mb',
# provider    string comment 'cdn provider',
# cdate   string   comment 'record date without time'
# )
# ROW FORMAT DELIMITED
#    FIELDS TERMINATED BY ',';

## 网宿信息获取方式
# 通过发送 HTTP 请求的方式调用接口,格式为: https://myview.chinanetcenter.com/api/bandwidth-channel.action?u=xxxx&p=xxxx&c ust=xxx&date=xxxx&channel=xxxxxx;xxxxx&isExactMatch=false&region=xxxx&isp=xxxx&resultType=xxxx&format=xxx&startdate=xxxx&enddate=xxxx
# 1. 第一个参数 u:网宿后台管理系统的用户名;
# 2. 第二个参数 p:MYVIEW API 密码;
# 3. 第三个参数 cust(可选):合并账号下的某个客户的英文名,当合并账号要查看子客户
#   的信息时,必须填写子客户的英文名;
# 4. 第四个参数 date(可选):为需要获取带宽数据的日期,日期格式为 yyyy-mm-dd,如
# 2011-06-02,不选或者为空时默认为当天;(注意:所填写的日期不能大于当天,且查
# 询时间不能离当前日期超过 2 年)
# 5. 第五个参数 channel(可选):需要获取带宽数据的频道,多个频道值请用英文分号“;”
# 分隔开(与 isExactMatch 配合,显示带宽数据,详见第六个参数说明);不选或者为
#   空时默认为所查询客户的所有频道。
# 6. 第六个参数 isExactMatch(可选):是否必须填写完整的域名,不选或者为空时默认为
# “true”。当 HTTP 请求的 URL 中没有出现此参数或者此参数等于 true 时,表示必须填 写完整的域名(此时会过滤用户输入的无效或重复的频道,所有输入频道都无效时返 403)。当 isExactMatch 不为 true 时,将显示以用户输入的频道为结尾的所有频道的带 宽信息。
# 7. 第七个参数 region(可选):要查询的地区的缩写,多个地区请用英文分号“;”分隔 开。地区的缩写格式参考附录:具体地区信息的代号。地区未写时 ,不选或者为空时默 认为“cn”。
# 8. 第八个参数 isp(可选):要查询的运营商的缩写,多个 isp 请用英文分号“;”分隔开。 运营商的缩写格式参考附录:具体运行商(ISP)信息的代号。备注:只有当地区只写 了“cn”时,填写 isp 信息才有效。不选或者为空时默认为所有 isp。
# 9. 第九个参数 resultType(可选):结果的显示是否提供合并值。填写 1 时:只提供合并 结果;填写 2 时:只提供拆分值;填写 3 时:既提供合并值,又提供拆分值。不选或者 为空时默认为“1”。
# 10. 第十个参数 format(可选):返回结果格式,支持格式为 xml 和 json,默认为 xml。
# 11. 第十一个参数 startdate(可选):为需要查询带宽数据的起始日期,精确到分钟,日期格
# 式为 yyyy-mm-dd hh-MM,如 2013-01-07 00:05,若没有输入时、分,则时分默认为 00:01;
# 此参数需与 enddate 参数配合,若存在 date 参数,则该参数无效。
# 12. 第十二个参数 enddate(可选): 为需要查询带宽数据的结束日期,精确到分钟,日期格式 为 yyyy-mm-dd hh-MM,如 2013-01-07 00:35,若没有输入时、分,则时分默认为 24:00; 此参数需与 startdate 参数配合,若存在 date 参数,则该参数无效。
import xml.etree.ElementTree as ET
from urllib2 import urlopen
from time import sleep
from datetime import datetime,timedelta
import tempfile
import commands
from os import remove
import sys
# yesterday=$(date -d "1 day ago" +"%Y-%m-%d")
if len(sys.argv) > 1:
    yesterday=sys.argv[1]
else:
    yesterday=str((datetime.now() - timedelta(hours=24)).date())
    
ts = datetime.now()
domains=["img1.hoto.cn","img2.hoto.cn","img3.hoto.cn","img4.hoto.cn ","avatar0.hoto.cn",
"avatar1.hoto.cn","recipe1.hoto.cn","pai0.hoto.cn","pai1.hoto.cn","head0.hoto.cn","head1.hoto.cn","dl.hoto.cn"]
res=[]
def get_dnion_bandwidth(): 
    
    ## 帝联CDN信息获取方式
    ### 带宽明细采集接口
    #http://customer.dnion.com/DnionBandwidth/bandwidthValue.do?captcha=客户标识符&domain=www.dnion.com&date=20110508(表示获取 20110508 日域名 www.dnion.com 的带宽明细)
    ### 每日带宽统计接口:统计每日的带宽的最大值,最小值,平均值
    #http://customer.dnion.com/DnionBandwidth/bandwidthRatio.do?captcha=客户标识符 &domain=WEB(表示前一天所有 web 加速域名的最大值,最小值,平均值)
    
    urlprefix="http://customer.dnion.com/DnionBandwidth/bandwidthRatio.do?captcha=436bcdb7&domain="
    for domain in domains:
        url = urlprefix + domain
        data = urlopen(url).read() #$(curl -sS  "${url}&domain=$domain" |xmlstarlet sel -t -m //Traffice -v @max -o "," -v @min -o "," -v @avg)
        tree = ET.fromstring(data)
        
        #<Traffice time="2014-09-28 17:25" max="52.45" min="0.88" avg="19.1"/>
        bd = tree.find('./date/Product/Traffice')
        #column
        #domain,maxbw,minbw,avgbw,provider,cdate
        try:
            res.append("%s,%.2f,%.2f,%.2f,%s,%s" %  (domain,float(bd.attrib['max']),float(bd.attrib['min']),float(bd.attrib['avg']),'dnion',yesterday))
        except AttributeError,e:
            print domain,e
def get_ws_bandwidth():
    '''
    一次性获得指定日期的所有域名带宽信息，然后针对每个域名进行统计
    '''
    url="https://myview.chinanetcenter.com/api/bandwidth-channel.action?u=hoto&p=Hoto123456&resultType=2&format=xml&date=%s" % (yesterday)
    try:
        data = urlopen(url,None,120).read()
    except Exception,e:
        print "failure to connect %s: %s" % (url,e)
        exit(1)
        
    tree = ET.fromstring(data)
    channels = tree.findall('./date/channel')
    for ch in channels:
        domain = ch.attrib['name']
        bds = ch.findall('bandwidth')
        cnt=0
        csum=0.0
        cmin=999999.0
        cmax=0.0
        for b  in bds:
        	v=float(b.text)
        	if v > cmax:
        		cmax = v 
        	elif v < cmin:
        		cmin = v 
        	csum += v 
        	cnt += 1
        cavg=csum / cnt    
        res.append("%s,%.2f,%.2f,%.2f,%s,%s" % (domain,cmax,cmin,cavg,'wangsu',yesterday))

if __name__ == '__main__':
    get_dnion_bandwidth()
    get_ws_bandwidth()
    #write to tempfile then load into hive table
    _,filepath = tempfile.mkstemp()
    print '\n'.join(res)
    open(filepath,'w').write('\n'.join(res))
    cmd="""hive -e "load data local inpath '%s' into  table bing.cdn_bandwidth" """ % filepath
    print commands.getoutput(cmd)
    remove(filepath)
    
