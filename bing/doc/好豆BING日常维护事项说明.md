
好豆BING日常维护事项说明  
=====================

* 作者：数据中心  
* 版本：v0.1  
* 创建日期：2015/11/20  
* 更新日期：2015/12/03  


# 维护事项索引

* 菜谱IDFA比对  
* 成果照活动数据导出  
* 菜谱4.0/小组4.0数据统计  
* 好豆菜谱数据统计(全站)  
* BI网站  

# 菜谱IDFA比对

## 前台说明

前台路径： bi.haodou.cn > 菜谱渠道推广 > 菜谱IDFA比对  
前台URL： [http://bi.haodou.cn/component/idfa/api](http://bi.haodou.cn/component/idfa/api)  

菜谱IDFA 对比文件注意事项  
操作流程：  
1.在左侧下拉框中选择渠道类型  
2.在右侧下拉框中选择比对结果接受人邮件地址  
3.按<选择对比文件>按钮选择比对文件  
4.点击<上传文件并开始对比>按钮上传比对  
只接受文件类型为xls，xlsx，csv以及zip压缩格式的文件。一次上传一个文件，可上传多次  
如果文件格式为zip压缩格式，则压缩文件所包含的文件格式必须一致，即xls，xlsx和csv三种格式中的一种，且必须是都是一个渠道的文件  
点击<上传文件并开始对比>按钮后，文件开始上传过程，上传成功后开始比对过程。比对完成后将会发送Email比对结果  
请在左侧下拉条中选择渠道和文件类型，如果为zip压缩包，则按照压缩包内文件格式和渠道类型选择。比如，haodou.zip压缩包内包含的是力美待对比文件file1.csv, file2.csv，则选择csv读取力美。右侧选择比对完成后结果接受邮件地址  

## 后台说明

后台代码路径： 北京HD14机器 `/home/hdfs/idfa-match/`  

调度方式： 通过run.sh，使用nohup方式将服务脚本bottle_server.py 丢在后台运行。（重启服务器后，需要手工启动）  

每天比对是从`logs.log_php_app_log`表中读idfa，抽取到日志文件logXXXX-XX-XX。  

Web页面文件地址： 北京HD50机器 `/home/ruby/dep/z/app/views/components/idfa/api.html`

## Q1:任务是否在运行

处理： 登录北京HD14机器，查看是否有`hadoopchann2elmatch_v3.py` 进程在跑。  
命令： `ps aux | grep -v grep | grep hadoopchannelmatch_v3.py`  

## Q2:如何增加渠道

处理： 登录北京HD14机器，修改代码目录下的渠道配置文件data.txt，参照格式增加即可。  

## Q3:清理过期日志文件

比对日志保存在本地，命名为logXXXX-XX-XX，目前每天日志大小为500M左右，一般保存1+1个月，大约需要40G磁盘空间。对比目前的磁盘空间分配偏紧，需要注意清理过期日志文件。  

```
$> df -h
Filesystem      Size  Used Avail Use% Mounted on
/dev/sda3        58G   46G  9.4G  83% /
tmpfs            32G   20K   32G   1% /dev/shm
/dev/sda1       194M   74M  111M  40% /boot
/dev/sda4        20G  1.5G   17G   9% /opt
/dev/sda6       1.7T  1.5T  129G  93% /hadoop1
/dev/sdb        1.8T  1.5T  332G  82% /hadoop2
/dev/sdc        1.8T  1.5T  304G  84% /hadoop3
/dev/sdd        1.8T  1.5T  318G  83% /hadoop4
/dev/sde        1.8T  1.5T  311G  84% /hadoop5
/dev/sdf        1.8T  1.5T  310G  84% /hadoop6
/dev/sdg        1.8T  1.5T  320G  83% /hadoop7
/dev/sdh        1.8T  1.5T  327G  83% /hadoop8
/dev/sda2        97G   50G   42G  55% /home
```

## Q4:重启服务时报错`socket.error: [Errno 98] Address already in use`。  

原因：如果确定没有服务进程`bottle_server.py`以及`hadoopchannelmatch_v3.py`，则很可能是上一次操作中启动了hive子进程(java进程)，而hive进程未结束。  
先检查端口占用情况，然后检查相应进程，并杀掉该进程，即可重启服务。  

```
$> netstat -apn|grep 9898
可能的结果类似
tcp        0      0 0.0.0.0:9898                0.0.0.0:*                   LISTEN      3348/python2.7
或者
tcp        0      0 0.0.0.0:9898                0.0.0.0:*                   LISTEN      41421/java
```

注意，首次查出是python占用导致的，在杀掉python进程后需要复查端口占用情况。因为该python进程可能起了子进程，在杀掉python进程后，端口占用会被其子进程继承。  

解决办法：在`subprocess.Popen`调用时，加入参数`close_fds=True`，关闭从父进程继承的文件描述符。  

## Q5:提交了不出数据   

有可能是xls文件中数据放在第2页，第1个页没有数据导致。或者是由于有多个sheet导致。   

## BUG1:结果数据的记录条数不正确（可能是翻番）  

原因：可能是之前的处理进程异常后，处理过程产生的文件未清理，导致读取了重复记录。  
临时处理：在重启服务的脚本中，加入了清理历史中间文件的rm，保证新起的服务处理正常。  


# 成果照活动数据导出 

## 前台说明

前台路径： bi.haodou.cn > 菜谱4.0数据汇总 > 作品统计 > 成果照主题活动数据导出  
前台URL： [http://bi.haodou.cn/component/works/export](http://bi.haodou.cn/component/works/export)  

操作流程：  
1. 选择活动类型  
2. 选择日期区间 (开始和结束时间都是以作品的发布时间为准) 开始日期(从0时开始)结束日期(截止到23:59:59)  
3. 选择需要导出项目(以下类目中，作品指的是审核通过的作品，新用户指的是在制定的时间区间内在指定的活动类型下发布了第一个作品的用户)  
4. 填写邮件接收地址  

## 后台说明

代码库： `git@g.haodou.cn:data-center/recipe_topic_report.git`  
后台代码路径： 北京HD14机器 `/home/hdfs/recipe_topic_report`  

调度方式： 通过run.sh，使用nohup方式将服务脚本server.py 丢在后台运行。（重启服务器后，需要手工启动）  

## Q1:如何增加成果照活动类型

登录北京HD50机器，修改活动类型配置文件`/home/ruby/dep/z/app/views/layouts/export.html.haml`  


# 菜谱4.0/小组4.0数据统计 

## 前台说明

前台路径： bi.haodou.cn > 菜谱4.0数据统计/小组4.0数据统计  
前台URL： [http://bi.haodou.cn](http://bi.haodou.cn)  

## 后台说明

菜谱4.0后台代码路径： 北京HD15机器 `/home/hdfs/recipe_analysis`  
小组4.0后台代码路径： 北京HD15机器 `/home/hdfs/hd_group_analysis`  

调度方式： 通过crontab 每天定时启动脚本运行。  

```
$> crontab -l
0 7 * * * cd /home/hdfs/hd_group_analysis && /usr/local/bin/python2.7 /home/hdfs/hd_group_analysis/group_analysis.py --mode=remote
0 7 * * * cd /home/hdfs/recipe_analysis && /usr/local/bin/python2.7 /home/hdfs/recipe_analysis/PhotoAnalysis.py 
0 7 * * * cd /home/hdfs/recipe_analysis && /usr/local/bin/python2.7 /home/hdfs/recipe_analysis/RecipeAnalysis_v1.py 
0 7 * * * cd /home/hdfs/recipe_analysis && /usr/local/bin/python2.7 /home/hdfs/recipe_analysis/AlbumAnalysis_v1.py
```

写入库是北京HD50机器的mysql dw库。直接存到mysql，不需要`bing2mysql`过程。  


# 好豆菜谱数据统计(全站)  

## 前台说明

前台路径： bi.haodou.cn > 好豆菜谱数据统计(全站)  
前台URL： [http://bi.haodou.cn](http://bi.haodou.cn)  

## 后台说明

代码库： `git@g.haodou.cn:data-center/bing.git`下的`/task/showdb_rpt_index.py`  
后台代码路径： 北京HD10机器 `/home/crontab/bing/task/showdb_rpt_index.py`  

调度方式： 已经纳入Azkaban。  

生成的指标通过showdb2hive.py同步到hive表。  
部分指标（如留存率）走bing的标准过程生成，参考bing项目即可了解。  

注意：1.基础数据由*常正*每月底提供下月数据，维护进py文件即可。  
2.必须按日期顺序执行。如果之前天有错误，则之后也会错误。必须从第1次错误日期开始补全。  
3.代码维护应该提交git，10机器会自动同步代码变动，不要直接修改北京HD10机器的本地代码。  

# BI网站  

## 前台说明

前台路径： bi.haodou.cn  
前台URL： [http://bi.haodou.cn](http://bi.haodou.cn)  

## 后台说明

后台代码路径： 北京HD50机器 `/home/ruby/dep/z/`

启动服务：`thin start -C config/thin.yml`

开发帐号及语言为ruby  
根据url路径可以找出相应代码，例如`bi.haodou.cn/components/works/export`, 对应代码位置为`dep/z/app/controllers/components/works_controller.rb`里的post方法  


# 这是个模板 

## 前台说明

前台路径： bi.haodou.cn >  >  
前台URL： [http://bi.haodou.cn](http://bi.haodou.cn)  

## 后台说明

后台代码路径： 

## Q1:

## Q2:


