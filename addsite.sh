#!/bin/bash
# add virtual site to apache server
# the script only test on mac os x
# $1 domain such like www.example.com
[ -z $1 ] && exit 1
apachefile="/etc/apache2/extra/http-vhosts.conf"
wpversion="3.9"
domain=$1

cat - >> $apachefile <<EOF
<VirtualHost *:80>
    ServerAdmin webmaster@$domain
    DocumentRoot "/Users/weiguo/Sites/$domain/"
    ServerName  $domain
    <Directory "/Users/weiguo/Sites/$domain/">
        Options +FollowSymLinks
        AllowOverride All
    </Directory>
    ErrorLog "/private/var/log/apache2/$domain-error_log"
    CustomLog "/private/var/log/apache2/$domain-access_log" common
</VirtualHost>
EOF

echo "127.0.0.1    $domain" >>/etc/hosts

if [ ! -d ~weiguo/Sites/$domain ];then
  mkdir ~weiguo/Sites/$domain 
  cd ~weiguo/Sites/$domain
  unzip  ../wordpress-${wpversion}-zh_CN.zip >/dev/null 2>&1
  mv wordpress/* .
  rm -r wordpress
  cd -
  chmod -R 777 ~weiguo/Sites/$domain
fi

launchctl load /System/Library/LaunchDaemons/org.apache.httpd.plist
launchctl unload /System/Library/LaunchDaemons/org.apache.httpd.plist

#pass it to notication
echo $domain
exit $?


