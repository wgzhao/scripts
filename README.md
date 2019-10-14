# 说明

主要是个人写的一些脚本，包括为项目写的，或者为自动化某些工作而写的，也收集一些网络上看到的很实用精巧的脚本(会注明)

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

[ipcalc.pl](./ipcalc.pl) IP地址计算工具，用非常漂亮的方式打印IP网段，子网以及子网包括的主机数量等，来源于 http://jodies.de/ipcalc

[ipcalc.py](./ipcal.py) 用Python实现的 [ipcalc.pl](./ipcalc.pl)

[lunar_pg.sql](./lunar_pg.sql) 让PostgreSQL 支持农历查询的扩展

[idcard.py](./idcard.py) 把15位的身份证号升级到18位

[idcard.sh](./idcard.sh) [idcard.py](./idcard.py) 的 shell 实现版本

[getpics.py](./getpics.py) 从PowerPoint文件中提出所有的图片

[orcle_ddl_to_hive.py](./oracle_ddl_to_hive.py) 把Oracle导出的DDL语句转为兼容Hive的DDL语句，包括字段注释，表注释信息

[mysql2hive.py](./mysql2hive.py) 利用Sqoop工具导出MySQL数据库到Hive，支持库倒入，表倒入，表正则表达式，排除特定表等选项，支持文件读取

[ocf_kingbase.sh](./ocf_kingbase.sh) 一个基于[Linux HA Project](http://www.linux-ha.org/wiki/Main_Page) 的OCF组件例子，用来控制某个服务的启动停止和监控

[ed2k_monitor.py](./ed2k_monitor.py) 监控剪贴板，如果有ed2k链接，则转为正确的编码，主要用于某些特定网站的ed2k链接编码不正确的情况

[ed2klinks.py](./ed2klinks.py) 从给出的URL链接里提取所有的ed2k链接，主要用于批量下载

[getlinks.py](./getlinks.py) 从给出的内容或者链接里提取所有的链接地址，主要用于批量下载

[ip.php](./ip.php) 一个[纯真IP地址库](http://update.cz88.net/ip)的查询工具

[qqwry_ip.py](./qqwry_ip.py) [ip.php](./ip.php) 工具的 Python 实现，增加了自动下载[纯真IP地址库](http://update.cz88.net/ip)

[bond.py](./bond.py) 一个TK编程练习脚本，用来生成Linux下多个网卡bond的配置文件，支持图形界面和命令行界面

[chgcode.py](./chgcode.py) 批量修改指定目录下所有文件的内容编码

[fixeth.sh](./fixeth.sh) 设置Linux下网卡位置固定的配置文件