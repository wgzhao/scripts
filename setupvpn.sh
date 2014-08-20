#!/bin/bash
# auto setup pptp vpn 
# only test in linode vps
# author: wgzhao <wgzhao@gmail.com>
export PATH=/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin
export LANG=en_US.UTF-8
export LANGUAGE=en_US.UTF-8

usage()
{
	echo "$0 [ -u <username> ] [ -p <password> ] [ -l <local ip> ] [ -r <remote ip>] "
	echo """ 
	-u vpn username , the default is gfw
	-p password to connect vpn server , the default is fuck-gfw
	-l specifies the local IP address , the default is 172.16.31.1
    -r specifies the remote Ip address range,you can specify single IP address speerated by commas 
       or you can specify ranges, or both. For example:
       
               192.168.0.234,192.168.0.245-249,192.168.0.254
       the default is 172.16.31.2-20
	-h print the help
		"""		
	exit 2 
}

#default 
username='gfw'
password='fuck-gfw'
localip='172.16.31.1'
remoteip='172.16.31.2-20'

while getopts "u:p:l:r:h" opt
do
	case $opt in
	u) username=$OPTARG
	;;
	p) password=$OPTARG
	;;
	l) localip=$OPTARG
	;;
    r) remoteip=$OPTARG
    ;;
	h) usage
	;;
	esac
done

os=$(head -n1 /etc/issue 2>/dev/null |awk '{print $1}' )
if [ "$os" = "CentOS" -o "$os" = "RedHat" ];then
    cmd="yum install -y "
elif [ "$os" = "Debian" -o "$os"  = "Ubuntu" ];then
    cmd="apt-get -y install "
else
    echo "Unsupported OS distribution"
    exit 2
fi

# install pptpd 
which pptpd >/dev/null 2>&1  

if [ $? -ne 0 ];then
    echo "install pptpd package..."
    $cmd pptpd

    if [ $? -ne 0 ];then
        echo " failed "
        exit 3
    fi
fi
#setup pptp

echo "
localip=$localip
remoteip=$remoteip
" >>/etc/pptpd.conf

# add account to chap file
echo -e "$username \t pptpd \t $password \t * " >> /etc/ppp/chap-secrets

#start pptpd service
service pptpd start

# print account information
echo "Now,you can connect your VPN(PPTP) username: $username and password: $password . enjoy it"
exit 0
