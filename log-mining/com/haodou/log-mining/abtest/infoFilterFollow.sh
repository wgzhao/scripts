if [ -z $1 ] ;then
	echo "没有指定AB选项！！"
	exit 1
fi
python filterAB.py abUser.txt $1 1407146070  | python followMethod.py f1 info.getinfo rid

