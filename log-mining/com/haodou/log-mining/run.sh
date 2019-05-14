#!/bin/bash

# define variables
datetime=`date  +%F`
day30ago=`date -d "30 days ago" +%F`
logfile=/tmp/recommed_mining_$datetime.log
errorfile=/tmp/recommed_mining_error_$datetime.log
log30file=/tmp/recommed_mining_${day30ago}.log
error30file=/tmp/recommed_mining_error_${day30ago}.log

#基本方法：（1）计算出每个菜谱，搜索词，标签的相关菜谱
#          （2）将菜谱，搜索词，标签均视为特征，用来为用户建模

#待扩展点：a. 计算相关菜谱时，是用共现关系。在统计共现时，同一个用户产生的行为中，只有相邻的20个菜谱被纳入统计口径
#             这样计算出来的相关菜谱，相似性较高，多样化不足，后续要放开限制进行比对。
#		   b. 没有采用标准的协同过滤算法
#

#本算法相当于一个简化版的Item-based协同过滤算法
#修改tagRecipe中的相似度计算方法，就可以得到一个完整版的协同过滤算法
#

#解决相对路径问题
CPATH=$(dirname $0)
if [ ! -z $1 ]; then
    CPATH=$1
    echo $1
fi
cd $CPATH

./run_nolog.sh > $logfile 2> $errorfile

# 删除30天之前的日志
rm -f  $log30file
rm -f  $error30file


