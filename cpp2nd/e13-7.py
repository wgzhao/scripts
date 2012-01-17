#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from datetime import date
import time
class DateFmt:
    
    """
        提供一个 time 模块的接口,允许用户按照自己给定时间的格式,比如:
        MM/DD/YY,
        MM/DD/YYYY,
        DD/MM/YY,
        DD/MM/ YYYY, 
        Mon DD, YYYY,
        或是标准的 Unix 日期格式:
        Day Mon DD, HH:MM:SS YYYY
        来查看日期
    """

    def __init__(self):
        self.value = date.today()

    def update(self,arg):
        '''
        约定以YY(YY)-M(M)-D(D)格式更新日期
        '''
        (year,month,day) = arg.split('-')
        self.value = date(int(year),int(month),int(day))

    def display(self,fmt='MDY'):
        '''
        以指定格式显示日期
        以M表示月，D表示日，Y表示年，YY表示世纪年，MO表示月单词缩写
        比如
        MDY –> 3/20/01
        DMYY –> 20/3/2001
        MODY –> Mar 20,01
        MODYY –> Mar 20,2001
        '''
        cd = self.value.timetuple()
        fmtdict={'M':'%m','D':'%d','Y':'%y','YY':'%Y'}
        strftm = ''

        if 'MOD' in fmt:
            strftm = '%b %d,'
            if 'YY' in fmt:
                strftm += '%Y'
            else:
                strftm += '%y'
            print time.strftime(strftm,cd)
        else:
            fmt = list(fmt)
            if fmt.count('Y') > 1:
                cent=True
            else:
                cent=False

        	while fmt:
	            char = fmt.pop(0)
	            if char == 'Y' and cent:
	                strftm += fmtdict['YY']
	                fmt.pop(0)
	            else:
	                strftm += fmtdict[char]

            		strftm+='/'

        	print time.strftime(strftm[:-1],cd)

    def __getattr__(self,attr):
        return getattr(self.value,attr)

if __name__ == '__main__':

    b = DateFmt()
    b.display('YYMD')
    b.update('1999-2-28')
    b.display('MODY')