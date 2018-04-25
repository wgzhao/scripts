if [ -z $1 ]; then
	echo "没有指定日期"
	exit 1
fi

sh getLog.sh $1

if [ $? -ne 0 ]; then
	echo "读取日志失败！"
	exit 1
fi

function delete(){
rm ~/data/push/$1/receiveLog.txt
rm  ~/data/push/$1/sendedLog.txt
rm ~/data/push/$1/userPolicy.txt
}

python evaluate.py $1 2> ~/data/push/$1/err.txt

if [ $? -ne 0 ]; then
	delete $1
	echo "评估失败！"
	exit 1
fi

delete $1

