#!/usr/bin/env python
# -*- encoding: utf-8 -*-
class Time60(object):
    '''
    用小时:分钟对来表示时间
    '''
    def __init__(self,obj1,obj2=None):
        '''
        接受下面三种初始化方式：
        (30,15) #hour,minitues
        ({'hr':30,'min':15})
        ('30:15')
        '''
        if type(obj1) == type(0):
            #integer
            if obj2 != None and type(obj2) == type(0):
                self.__hr = obj1
                self.__min = obj2
            else:
                raise TypeError,'Time60 requires string,dict and numeric pair'
        elif type(obj1) == type({}):
            if obj1.has_key('hr') and obj1.has_key('min'):
                self.__hr = obj1['hr']
                self.__min = obj1['min']
            else:
                raise TypeError,'Time60 requires string,dict and numeric pair'
        elif type(obj1) == type(''):
            (hr,min) = obj1.split(':')
            self.__hr = int(hr)
            self.__min = int(min)
        else:
            raise TypeError,'Time60 requires string,dict and numeric pair'
            
    
    def __str__(self):
        return '(%02d:%02d)' % (self.__hr,self.__min)
        
    def __repr__(self):
        return '(%d:%d)' % (self.__hr,self.__min)
    
    def __add__(self,obj):
        
        if isinstance(obj,Time60):
            hr = self.__hr + obj.__hr
            min = self.__min + obj.__min
            if min > 60:
                (deltahr,min) = divmod(min,60)
                hr += deltahr
            return self.__class__(hr,min)
        else:
            print 'obj must be a Time60 class instance'

