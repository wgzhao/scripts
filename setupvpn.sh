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
if [ $uid -ne 0 ];then
	echo "setup vpn need root privileges"
	echo "you can aslo execute with sudo ,if your account has sudo privilege"
	exit 1
fi
osdetect=$(head -n1 /etc/issue 2>/dev/null |awk '{print $1}' )
if [ "$osdetect" = "CentOS" -o "$osdetect" = "Red" ];then
	ostype="Redhat"
    cmd="yum install -y "
elif [ "$osdetect" = "Debian" -o "$osdetect"  = "Ubuntu" ];then
	ostype="Debian"
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
localip $localip
remoteip $remoteip
" >>/etc/pptpd.conf

# add dns configure 
if [ "$ostype"  = "Redhat" ];then
	optionsfile="/etc/ppp/options.pptpd"
else
	optionsfile="/etc/ppp/pptpd-options"
fi

sed -i '13i ms-dns 8.8.8.8\nms-dns 8.8.4.4' $optionsfile

# add account to chap file
echo -e "$username \t pptpd \t $password \t * " >> /etc/ppp/chap-secrets

#start pptpd service
service pptpd start

#set ip forward
echo "net.ipv4.ip_foward=1" >>/etc/sysctl.conf
sysctl -p >/dev/null 2>&1

#set iptables rules
if [ "$ostype" = "Redhat" ];then
	/sbin/iptables -t nat -A POSTROUTING -o eth0 -j MASQUERADE
	/sbin/iptables -o eth0 -A FORWARD -p tcp --tcp-flags SYN,RST SYN -m tcpmss --mss 800:1536 -j TCPMSS --clamp-mss-to-pmtu
	service iptables save
else
	#save to a script
	echo "#!/bin/sh
	IPT='/sbin/iptables'

	$IPT -t nat -A POSTROUTING -o eth0 -j MASQUERADE
	$IPT -o eth0 -A FORWARD -p tcp --tcp-flags SYN,RST SYN -m tcpmss --mss 800:1536 -j TCPMSS --clamp-mss-to-pmtu
	" > /etc/network/if-up.d/pptp.sh
	chmod +x /etc/network/if-up.d/pptp.sh
	#execute it
	/etc/network/if-up.d/pptp.sh
fi
# print account information
echo "Now,you can connect your VPN(PPTP) username: $username and password: $password . enjoy it"
exit 0
