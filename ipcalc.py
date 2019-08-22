#!/usr/bin/env python3
"""
simple IP Calculator
"""
import sys
import ipaddress
import binascii
from termcolor import colored, cprint
import textwrap 

def dotmask2int(mask):
    """
    convert net mask to cidr
    eg: 255.255.255.0 => 24
        255.255.252.0 => 22
    """
    res = 0
    for x in ''.join(map(lambda x: bin(int(x))[2:], mask.split('.'))):
        res += int(x)
    return res

def int2ip(ipint):
    """
    convert int-formal ip to ip address string
    """
    return '.'.join(map(lambda x, y: str(y >> x & 0x00ff), \
                [24,16,8,0],[ipint, ipint, ipint, ipint])
            )

def validate_obj(obj):
    """
    validate input
    it's must be valid ip/netmask combine
    """
    if '/' in obj:
        ip, mask = obj.split('/')
    else:
        ip = obj
        mask = None
    # validate IP
    ip_arr = ip.split('.')
    for item in ip_arr:
        if int(item) < 0 or int(item) > 254:
            return False
    if mask:
        # validate mask
        if '.' in mask:
            for item in mask.split('.'):
                if int(item) < 0 or int(item) > 255:
                    return False
        elif int(mask) < 0 or int(mask) > 32:
            return False
    return True

def ip2int(ip):
    """
    convert ip address to int
    """
    return sum(map(lambda x, y: int(y) << x ,[24,16,8,0],ip.split('.')))

def anytobin(obj):
    """
    convert num or dot-form address to 32-bit binary string
    eg: 
        24 -> 11111111 11111111 11111111 0000000
    """
    bi_str = ''
    if '.' in obj:
        for item in obj.split('.'):
            bi_str += '{0:08b}'.format(int(item))
        return bi_str
    else:
        return bin(0xffffffff << (32 - obj))[2:]

def split_bin(b, num=24):
    """
    split binary string to 4-bytes separator
    and insert space in num position
    eg: 11111111111111111111111100000000 =>
        11111111.11111111.11111111. 00000000
    """
    res = "{}.{}.{}.{}".format(b[:8], b[8:16], b[16:24], b[24:])
    num += num // 8 
    return res[:num] + ' ' + res[num:]

def pretty_print(msg, obj, cidr=24, color=['cyan','yellow']):
    print("{0:10}".format(msg + ":"), end=" ")
    
    if msg.lower() == 'netmask':
        print("{0:30}".format(colored(obj + ' = ' + str(cidr), color[0])), end=" ")
    else:
        print("{0:30}".format(colored(obj,color[0])), end=" ")

    str_bin = split_bin(anytobin(obj), cidr)
    cprint("{0:33}".format(str_bin), color[1])

def main(obj):
    try:
        addr = ipaddress.ip_network(obj)
        ip, cidr = addr.compressed.split('/')
        cidr = int(cidr)
    except ValueError:
        # 非法IP地址与掩码组合，尝试矫正
        ip, mask = obj.split('/')
        if '.' in mask:
            cidr = dotmask2int(mask)
        else:
            cidr = int(mask)
        int_ip = ip2int(ip)
        network_addr = int2ip(int_ip & (0xffffffff << (32 - cidr)))
        addr = ipaddress.ip_network(f"{network_addr}/{cidr}")

    # print ip address
    pretty_print("Address", ip, cidr=cidr)

    netmask = addr.netmask
    pretty_print("Netmask", netmask.exploded, cidr=cidr, color=['cyan','red'])

    wildcard = addr.with_hostmask.split('/')[-1]
    pretty_print("Wildcard", wildcard, cidr=cidr)
   
    print("=======>")

    if cidr < 32:
        network = addr.network_address
        pretty_print("Network", network.exploded, cidr=cidr, color=['cyan','red'])
     
        min_host = ipaddress.ip_address(int(network) + 1)
        pretty_print("hostMin", min_host.exploded, cidr)

        max_host = ipaddress.ip_address(int(network) + 2**(32 - cidr) -2)
        pretty_print("HostMax", max_host.exploded, cidr)
     
        brd = addr.broadcast_address.compressed
        pretty_print("Brodcast", brd, cidr)

        num_hosts = int(max_host) - int(min_host) + 1
        print("{0:10}".format("Host/Net:"), end=" ")
        print("{0:30}".format(colored(num_hosts, 'cyan')), end=" ")
    else:
        pretty_print("hostroute", ip, cidr)
        print("{0:10}".format("Host/Net:"), end=" ")
        print("{0:30}".format(colored(1, 'cyan')), end=" ")
    
    print()

def usage():
    print(textwrap.dedent("""
    Usage: ipcalc <ADDRESS>[[/]<NETMASK>]

    ipcalc takes an IP address and netmask and calculates the resulting broadcast,
    network, Cisco wildcard mask, and host range. By giving a second netmask, you
    can design sub- and supernetworks. It is also intended to be a teaching tool
    and presents the results as easy-to-understand binary values.

    Examples:
    ipcalc 192.168.0.1/24
    ipcalc 192.168.0.1/255.255.128.0
    ipcalc 192.168.0.1
    """)
    )

if __name__ == '__main__':
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)
    
    ip = sys.argv[1]
    if validate_obj(ip):
        main(ip)
    else:
        print("invalid input")
        sys.exit(2)