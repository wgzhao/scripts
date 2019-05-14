# geoip2pack
对geoip2 的city库进行封装。包括：处理异常以及按中国国情修改结果。

处理的异常包括：

* 有些ip查不到 except geoip2.errors.AddressNotFoundError
* 有些ip查出来的结果为空 country.name is None
* 有些国外地址没有省份 subdivisions.most_specific.name is None
* 有些地址没有中文名 country.names.get('zh-CN',cur_country)
* 地址中的空格" "和"`"字符处理

中国国情修改包括：

* geoip2将港澳台视为国家一级，将其国家调整为中国
* geoip2将直辖市的区县视为市一级，调整为市

返回信息只包括该ip对应的城市，省份，国家的中文/英文名。

使用说明：
```
from geoip2pack import GeoIP2Reader

reader = GeoIP2Reader()
ip = '8.8.8.8'
ipinfo = reader.ip2city(ip)
print ip, ipinfo['city'], ipinfo['city_cn'], ipinfo['province'], ipinfo['province_cn'], ipinfo['country'], ipinfo['country_cn'], ipinfo['error']
```

文件依赖：

* python 安装 geoip2.database 库
* 缺省情况下，GeoLite2-City.mmdb 文件应该在执行目录

相关URL：

* pypi https://pypi.python.org/pypi/geoip2
* geoip2 http://dev.maxmind.com/geoip/
* GeoLite2-City.mmdb下载 http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz
