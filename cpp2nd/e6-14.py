#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from random import  randrange
def ex_614(arg):
    '''
    设计一个"石头,剪子,布"游戏
    布>石头
    石头>剪刀
    剪刀>布
    我们分别用b,s,j表布，石头和剪刀
    因为只有三个元素的比较，为了减少if语句，我们构建一个比较结果集cm，它保存了三者比较后前者胜出的结果。
    如果给出的选项和随机选项构成的在这个结果集中，则表示给出项胜，否则表示平手或者给出项输
    '''
    order = ['b','s','j']
    cm = [('b','s'),('s','j'),('j','b')]
    if not arg in order:
        print "your choice is illege,you just only use the one of following three: %s" % ' '.join(order)
        sys.exit(1)    
    #random
    pos = order[randrange(0,len(order))]
    if pos == arg:
        print 'we both chose %s equal' % pos
    if (arg,pos) in cm:
        print '%s > %s you win' % (arg,pos)
    else:
        print '%s < %s You lose!' % (arg,pos)
        
if __name__ == "__main__":
    ex_614('b')
    ex_614('j')
    ex_614('s')