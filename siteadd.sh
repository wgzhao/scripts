#!/bin/bash
#
# auto add a website for nginx server
# wgzhao 2010.3.24
# usage: $0 domain document_root
conf="/etc/nginx/conf.d/virtual.conf"
function usage()
{
	echo "$0 <domain> <website document root>"
	echo " example: $0 mlsx.xplore.cn /web/xplore.cn/mlsx/"
	exit 65
}
if [ $UID -ne 0 ];then
	echo "must be root privilege"
	exit 66
elif [ $# -lt 2 ];then
	usage
fi
	
if [ ! -d $2 ];then
	mkdir -p $2
	echo  "It works!" >>$2/index.html
fi

exec 6>&1
exec >>$conf
echo -e "server {
  listen       80;  #could also be 1.2.3.4:80
  server_name  ${1};

  root ${2};
  
  access_log  /var/log/nginx/access_$1.log;
  error_log 	/var/log/nginx/error_$1.log;
  
  location / {
    index  index.shtml index.html index.htm index.php ;
	}
  location ~ \.php$ {
    include /etc/nginx/fcgi.conf;
   }

  location ~* ^.+.(jpg|jpeg|gif|css|png|js|ico|html)$ {
    access_log        off;
    expires           30d;
	}
  location ~ /\.ht {
            deny  all;
        }
}"
exec 1>&6 6>&-

#reload nginx
service nginx reload
