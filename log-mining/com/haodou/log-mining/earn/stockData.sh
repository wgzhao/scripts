
while read stockId
	do
		
		hive -e "select * from stock_day."$stockId > stock_day/$stockId.txt

	done < "data/sz.txt"
	

