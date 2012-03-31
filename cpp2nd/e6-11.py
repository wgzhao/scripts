def ex_611(data):
    '''
    如果给出的是整数，则将其变成四个八进制IP地址，否则转换为整数
    同时要判断IP地址是否合法
    >>> ex_611('113.240.196.93')
    1911604317
    >>> ex_611(1911604317)
    113.240.196.93
    >>> ex_611('128.256.33.4')
    illegal ip address
    >>> ex_611('127.1.1.0')
    illegal ip address
    '''
    if type(data) == type(2):
        ipaddr = int2ip(data)
        print ipaddr
    elif type(data) == type('string'):
        #ip address,valiate it
        isIP = True
        iplist = data.split('.')
        if len(iplist)  != 4:
            isIP = False
        elif int(iplist[0]) < = 0 or int(iplist[-1]) <=0:
            isIP = False
        else:
            for char in iplist:
                if int(char) > 255:
                    isIP = False
                    break
                    
        if isIP:
            num = ip2int(data)
            print num
        else:
            print 'illegal ip address'

    
if __name__ == "__main__":
    from doctest import testmod
    testmod()
