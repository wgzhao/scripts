#!/bin/bash
# add virtual site to apache server
# the script only test on mac os x
# $1 domain such like www.example.com
[ -z $1 ] && exit 1
apachefile="/etc/apache2/extra/httpd-vhosts.conf"
wpversion="3.9"
domain=$1
wwwroot="/opt/websites"
cat - >> $apachefile <<EOF
<VirtualHost *:80>
    ServerAdmin webmaster@$domain
    DocumentRoot "$wwwroot/$domain/"
    ServerName  $domain
    <Directory "$wwwroot/$domain/">
        Options +FollowSymLinks
        AllowOverride All
		Allow from All
    </Directory>
    ErrorLog "/private/var/log/apache2/$domain-error_log"
    CustomLog "/private/var/log/apache2/$domain-access_log" common
</VirtualHost>
EOF

echo "127.0.0.1    $domain" >>/etc/hosts

if [ ! -d $wwwroot/$domain ];then
  mkdir -p $wwwroot/$domain 
  cd $wwwroot/$domain
  wget -q -O wordpress.zip http://cn.wordpress.org/wordpress-${version}-zh_CN.zip
  unzip -q wordpress.zip
  mv wordpress/* .
  rm -r wordpress
  cd -
  chmod -R 777 $wwwroot/$domain
fi

launchctl load /System/Library/LaunchDaemons/org.apache.httpd.plist
launchctl unload /System/Library/LaunchDaemons/org.apache.httpd.plist

#pass it to notication
echo $domain
exit $?


