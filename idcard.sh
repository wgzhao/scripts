#!/bin/bash
#Description: Convert your 15bit old ID card into 18bit ID Card
#Author: wgzhao wgzhao # gmail dot com
#Blog: http://blog.wgzhao.com

crc=( 1 0 X 9 8 7 6 5 4 3 2 ) #CRC bit
weight=( 7 9 10 5 8 4 2 1 6 3 7 9 10 5 8 4 2 ) #weight factor
oldcard=${1:-123}
while true
do
  echo $oldcard |egrep -q "^[0-9]{15}$"
  [ $? -eq 0 ] && break
  echo -n "Pls hit your 15bit old ID card:"
  read oldcard
done

#insert two number "19" after sixth
tmpcard="`echo ${oldcard:0:6}`19`echo ${oldcard:6}`"

#split tmpcard into single digtial
y=0
for i in `echo {0..16}`
do
  a=${tmpcard:$i:1}
  (( y += $a * ${weight[$i]} ))
done

crcindex=`expr $y % 11`

echo "Your New 18bit ID Card is : ${tmpcard}${crc[$crcindex]}"

exit 0
