
if [ -z $1 ]; then
	echo "未指定待评估文件!such as curve.zj"
	exit 1
fi

file=$1
e=3000
rm $file.test
for((i=0;i<$e;i++))
	do

		cat $file | python tt.py t8 0.75 >> $file.test

	done

#年化
cat $file.test | python tt.py
#风报比
cat $file.test | python tt.py t7
#盈亏比
cat $file.test | python tt.py t7    mrate:

#最大下降
cat $file.test | python tt.py t7 最大下降:

#最大下降周期
cat $file.test | python tt.py t7 最大下降周期:

