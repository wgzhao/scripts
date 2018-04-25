i=`date +%Y-%m-%d`
echo $i
while (( $i <= '2014-12-09' ))
	do
		echo $i
		i=$(python dateAdd.py $i 1)
	done
