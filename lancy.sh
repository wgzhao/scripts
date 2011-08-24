#!/bin/bash
echo 1 >/proc/sys/net/ipv4/ip_forward
/etc/init.d/iptables start
iptables -A POSTROUTING -t nat   -o eth0 -j SNAT --to-source  10.10.116.173
/etc/init.d/iptables save

