source conf.sh

hdfs dfs -text /user/zhangzhonghui/tagid/dict/* | awk -F"	" '{OFS="\t";print $2,$1}' > $data_tmp_dir/tagid.dict
if [ $? -ne 0 ]; then
	echo "get tagDict fail!!"
	exit 1
fi

cp $data_tmp_dir/tagid.dict $data_all_dir/tagid.dict
awk -F"	" '{OFS="\t";if($2 >= 300) print $1,$2,$3}' $data_all_dir/tagmodel > $data_tmp_dir/freqTag.txt
if [ $? -ne 0 ]; then
	echo "get freq tag fail!!"
	exit 1
fi

cat $data_tmp_dir/tagid.dict $data_tmp_dir/freqTag.txt | sort | python freqTag.py > $data_tmp_dir/freqTag.dict
if [ $? -ne 0 ]; then
	echo "get freq dict fail!!"
	exit 1
fi

cp $data_tmp_dir/freqTag.dict $data_all_dir/freqTag.dict


