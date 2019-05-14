
if [ -z $1 ]; then
	echo "未指定待评估文件!such as curve.zj"
	exit 1
fi

file=$1
e=10
rm $file.test
for((i=0;i<$e;i++))
do
	cat $file | sort | python dayEarnCombine.py 2010 2014 > /dev/null 2>> $file.test
done

cat $file.test | python tt.py 
cat $file.test | python tt.py t7
cat $file.test | python tt.py t7	mrate:

