#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from datetime import date
def ex_615(birthday):
    '''
    我们省去第一个要求的实现,因为第一个要求的实现包含在后面两个实现里。
    给定一个生日（格式为DD/MM/YY)，分别计算出词人已经活过的天数，包括闰年天数
    然后计算这个人的下一个生日还有多少天
    该代码充分利用了datetime模块里的date对象
    '''
    (d,m,y) = birthday.split('/') #分隔出天，月和年，这里假定用户的输入是合法的
    myb = date(int(y),int(m),int(d)) #形成date类型
    today = date.today() #当前日期
    delta = today - myb #当前日期和生日的时间差
    print 'You have lived %s days' % delta.days
    myb = myb.replace(year = today.year)
    if myb.month < today.month: #当年的生日已经过了，需要等到来年了
        myb = myb.replace(year = today.year + 1) 
    delta = myb - today
    print 'It will be your birthday after %s days' % delta.days    
    
if __name__ == "__main__":
    ex_615('24/2/1979')
    ex_615('29/5/1979')