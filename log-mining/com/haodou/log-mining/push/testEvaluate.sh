for((i=1;i<10;i++))
	do
		today=`date -d -"$i"day +%Y-%m-%d`
		sh evaluate.sh $today
	done

