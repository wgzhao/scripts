# 说明

主要是个人写的一些脚本，包括为项目写的，或者为自动化某些工作而写的，也收集一些网络上看到的很实用精巧的脚本(会注明)
使用到的语言主要有  

1. bash  
2. python  
3. php 


## 一些工具的说明

[Safari非扩展组建方式添加下载链接到Aria](./docs/aria2_service.md)   详见说明

[rescan-scsi-bus.sh](./rescan-scsi-bus.sh)  重新扫描SCSI总线，用来发现新设备，比如当你热插拔一个设备后，通过该脚本可以让Linux内核识别出新设备，而不用重启

[dbf2csv.py](./dbf2csv.py) 基于多进程的导出[DBF](https://en.wikipedia.org/wiki/DBF)文件为CSV格式的文件

[qrcode.py](./qrcode.py) 生成基于给定内容的二维码

[send_email.py](./send_email.py) 发送邮件，可以包含多个附件

[setupvpn.sh](./setupvpn.sh) 一键创建基于pptp的VPN服务端，在Debian系统下测试通过

[stripcomment.py](./stripcomment.py) 删除指定源代码文件中的所有注释

[text2ebook.py](./text2ebook.py) 将特定格式的文本小说文件转为epub图书，自动提取章节内容

[tvideo_dl.py](./tvideo_dl.py) 下载Twitter上的视频文件，支持代理

[uninstall_hdp_completely.sh](./uninstall_hdp_completely.sh) 完整卸载[HDP集群](https://www.hortonworks.com/hdp)

[xinfadi_crawler.py](./xinfadi_crawler.py) 多进程下载[北京新发地批发市场](http://www.xinfadi.com.cn/marketanalysis/0/list/1.shtml)蔬菜行情价格为CSV格式

[ipcalc.pl](./ipcalc.pl) IP地址计算工具，用非常漂亮的方式打印IP网段，子网以及子网包括的主机数量等

[ipcalc.py](./ipcal.py) 用Python实现的 [ipcalc.pl](./ipcalc.pl)

[lunar_pg.sql](./lunar_pg.sql) 让PostgreSQL 支持农历查询的扩展

[idcard.py](./idcard.py) 把15位的身份证号升级到18位

[idcard.sh](./idcard.sh) [idcard.py](./idcard.py) 的 shell 实现版本

[getpics.py](./getpics.py) 从PowerPoint文件中提出所有的图片

[orcle_ddl_to_hive.py](./oracle_ddl_to_hive.py) 把Oracle导出的DDL语句转为兼容Hive的DDL语句，包括字段注释，表注释信息