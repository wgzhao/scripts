#!/bin/bash
# auto install and configure Linux,Nginx,MySQL and PHP
# just only for Debian 7.5 
# $1 hostname
if [ -n "$1" ];then
	name="$1"
else
	name=`hostname -f`
fi


# get all ip address except of loopback device
#ips=$(ifconfig   | awk '/inet addr:/ {print $2}' |cut -d: -f2  |head -n-1)

apt-get -y update
#install mysql and setup initial password
debconf-set-selections <<< 'mysql-server mysql-server/root_password password abc123'
debconf-set-selections <<< 'mysql-server mysql-server/root_password_again password abc123'
apt-get -y install php5 php5-fpm nginx php5-{mysqlnd,cli,cgi,gd} mysql-server mysql-client 

#install other utils
apt-get -y install vim parted

#disable ipv6
cat - >> /etc/sysctl.conf <<EOF
#disable ipv6
net.ipv6.conf.all.disable_ipv6=1
net.ipv6.conf.default.disable_ipv6=1
net.ipv6.conf.lo.disable_ipv6=1
net.ipv6.conf.eth0.disable_ipv6 = 1
#turn ip forward
net.ipv4.ip_foward = 1
EOF

/sbin/sysctl -p

mkdir -p /root/.ssh/ 2>/dev/null
echo "ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEA4ZhB4QHrvL/jH+thCqLdV5W8OOe90xUoMFLdYgdILF+vEjPOScCup4qWCq/BR1EMgTX0hGQzoMHCfDAO8wUxs3WuyyV8WT7D8xa65sEHxHrhFzOPXlCSEp9YWBMYnMNSsVDDgtxV3Nw40FPvW9RIvqyOJxuLgT+1/xs2/PgS0Q8HEvzQZfoRdjA0J6HVXQ/OpvTVBLOYnD7FBLz7Yr65Vq6wHgBTY8sGCy/OOYh/EDoVwQh3FogxkvqK5hEbb1c6oigoZ3QY81HJlaWIC0hR4SN7K+dYKUVjEA1Ts8Dm9AYqudpTCvb5ouHdQcSqRpZZlQPob/xiCsQV0nDEmFVYAw==" >>/root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys

#keep ssh alive
sed -i '67i ClientAliveInterval 30' /etc/ssh/sshd_config
sed -i '68i ClientAliveCountMax 3' /etc/ssh/sshd_config
/etc/init.d/ssh reload


#change bash prompt
echo "export PS1='\[\e[1;31m\][\u@\H \W]\$\[\e[0m\] '" >>/root/.bashrc

mkdir -p  /websites/{web,logs,mysql}

#reinitial mysql ,datadir point to /websites/mysql

service mysql stop

echo """
[client]
port		= 3306
socket		= /var/run/mysqld/mysqld.sock
default_character_set = utf8
[mysql]
prompt		= \\u@\\h [\\d]>
no-auto-rehash

[mysqld_safe]
socket		= /var/run/mysqld/mysqld.sock
nice		= 0

[mysqld]
user					= mysql
pid-file				= /var/run/mysqld/mysqld.pid
socket					= /var/run/mysqld/mysqld.sock
port					= 3306
basedir					= /usr
datadir					= /websites/mysql
tmpdir					= /tmp
character-set-server 	= utf8
lc-messages-dir			= /usr/share/mysql
skip-external-locking
skip-name-resolve
key_buffer				= 16M
max_allowed_packet		= 16M
thread_stack			= 192K
thread_cache_size   	= 8
myisam-recover      	= BACKUP
query_cache_limit		= 1M
query_cache_size    	= 16M
server-id				= 10
log_bin					= /websites/mysql/mysql-bin.log
expire_logs_days		= 10
max_binlog_size     	= 1G
log_slave_updates   	= 1
binlog_ignore_db		= test
binlog_format       	= mixed

[mysqldump]
quick
quote-names
max_allowed_packet	= 16M

!includedir /etc/mysql/conf.d/
""" >/etc/mysql/my.cnf

mysql_install_db --defaults-file=/etc/mysql/my.cnf --datadir=/websites/mysql --user=mysql 

#setup mysql root password
mysqladmin -uroot password 'abc123'

#setup nginx

cd /etc/nginx/site-enabled
rm -f *

echo '
server {
    listen 80;
    server_name _;
    root /websites/web/$host;
    access_log /websites/logs/${host}.access.log;
    error_log  /websites/logs/error.log;
    location / {
        index index.html index.htm index.php;
    }
    location ~ \.php$ {
        try_files $uri = 404;
        fastcgi_pass unix:///var/run/php5-fpm.sock;
        fastcgi_param SCRIPT_FILENAME $document_root$fastcgi_script_name;
        fastcgi_index index.php;
        include fastcgi_params;
        fastcgi_connect_timeout 180;
        fastcgi_read_timeout 600;
        fastcgi_send_timeout 600;
 
    }
    location ~ .*\.(gif|jpg|jpeg|png|bmp|swf|ico)$ {
        expires 30d;
    }
}' >/etc/nginx/sites-available/virtualhost.conf

ln -s /etc/nginx/sites-available/virtualhost.conf .

service nginx reload

exit $?

