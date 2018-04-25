#!/usr/bin/python
# -*- coding: UTF-8 -*-

subject = u"好豆菜谱"
alert = u"各种海鲜的家常做法，想吃海鲜再也不用下馆子了~"
link = "haodourecipe://haodou.com/collect/info/?id=5178358"

import urllib

data = """{"aps": {"badge": 0, "alert": "%s"}, "vibrator": false, "subject": "%s", "url": "%s", "eventtype": 1}"""
data = data % (alert.encode('unicode-escape'), subject.encode('unicode-escape'), link)

body = urllib.quote(data)

print "\n"
print """curl -v -d 'expire=86400&api_key=xxxxxx&from=%E7%AE%A1%E7%90%86%E5%91%98&body=""" + \
	body + \
	"""&timestamp=1407725392&to=c369a85bfcb99d19586194929743e964,FCD48D0445CFFBCD259BFAE5FB707556' 'http://10.0.10.202:9292/api/v1/msg/push'"""
print "\n"
