#!/bin/bash


#基本方法：（1）计算出每个菜谱，搜索词，标签的相关菜谱
#          （2）将菜谱，搜索词，标签均视为特征，用来为用户建模

#待扩展点：a. 计算相关菜谱时，是用共现关系。在统计共现时，同一个用户产生的行为中，只有相邻的20个菜谱被纳入统计口径
#             这样计算出来的相关菜谱，相似性较高，多样化不足，后续要放开限制进行比对。
#		   b. 没有采用标准的协同过滤算法
#

#本算法相当于一个简化版的Item-based协同过滤算法
#修改tagRecipe中的相似度计算方法，就可以得到一个完整版的协同过滤算法
#

source ./conf.sh

mkdir $data_dir
mkdir $data_all_dir
mkdir $data_tmp_dir
#python DBUserTag.py > userPersonalTag.txt
yd=`date -d -1day +%Y%m%d`
#hive -e "select userid,favorite from recipe_user_"$yd".user" | awk '{if($2 != "NULL") print $0}' > userPersonalTag.txt
python readDB/DBItem.py userTag > $data_dir/userPersonalTag.txt  #2015-10-25
if [ $? -eq 0 ]; then
	echo "read user personal tag ok!"
	cp $data_dir/userPersonalTag.txt $data_all_dir
fi

cd userRecom


sh acc.sh

cd ..


#python recipeQuality.py > quality.txt
hive -e "select recipeid,Title,Rate,CommentCount,LikeCount,ViewCount,Step,PhotoCount,CreateTime,Status from haodou_recipe_"$yd".recipe" > $data_dir/recipe.txt
cat $data_dir/recipe.txt | python recipeQuality.py >  $data_dir/quality.txt

if [ $? -eq 0 ]; then
	echo "compute recipe quality ok!"
	cp $data_dir/quality.txt $data_all_dir
fi

awk '{print $1}' $data_dir/quality.txt > $data_tmp_dir/quality.rid
awk -F"	" '{OFS="\t";print $1,$NF}' $data_dir/quality.txt > $data_tmp_dir/quality.df


sh tagRecipeAdd.sh  #必须先运行tagRecipeAdd.sh，然后才能运行userModelAdd.sh。因为需要前者提供从tag到id的映射
sh userModelAdd.sh

#无关紧要的错误可能影响下面这个
#if [ $? -ne 0 ]; then
#	exit 1;
#fi

hdfs dfs -text /user/zhangzhonghui/userModel/today.filter.id/* | python checkRepeat.py > $data_all_dir/usermodel 
hdfs dfs -text /user/zhangzhonghui/tagRecipe/today.filter.id/* | python checkRepeat.py > $data_all_dir/tagmodel
hdfs dfs -text /user/zhangzhonghui/userModel/today.add.id/* > $data_dir/usermodel
hdfs dfs -text /user/zhangzhonghui/tagRecipe/today.add.id/* > $data_dir/tagmodel


#获得频繁使用的tag,下面这个计算一次就可以
sh freqTag.sh

