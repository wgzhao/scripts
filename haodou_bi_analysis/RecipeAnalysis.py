#!/usr/bin/python
# -*- coding:utf-8 -*-
#  菜谱相关分析

from datetime import datetime,date
import MySQLdb
import sys

    
def adminAnalysis(rundate):
            
        #make it into tuple
        tdate=(rundate,)
        
        #删除菜谱管理员当天的操作数据，如果存在的话
        # model
        # 1 点赞
        # 2 发私信
        # 3 评论
        
        bjcur.execute("delete from bi_recipe_admin_operate_summary WHERE createtime='%s' and model in(1,2,3)" % rundate)
        
        #删除菜谱校验总结当天数据，如果存在的话
        # type
        # 1 菜谱审核数/编辑数/删除数/屏蔽数
        # 2 专辑审核数/隐藏数/删除数
        # 3 作品通过数/删除数
        
        bjcur.execute("delete from bi_recipe_admin_verify_summary WHERE createtime='%s' and `type` in (1,2,3)" % rundate )
       
        # 查询需要的数据
        qsqls=["""select au.adminuserid,u.username,count(d.id) from haodou_digg.Digg d        
                  inner join haodou_admin.AdminUser au on au.adminuserid=d.userid
                  left outer join haodou_passport.User u on u.userid=au.adminuserid
                  where au.adminusergroup=10 and date(d.createtime) = '%s' and d.itemtype=1
                  group by au.adminuserid,u.username;""",
               """select au.adminuserid,ur.username,count(u.messageid) from haodou_center.UserMessage u
                   inner join haodou_admin.AdminUser au on au.adminuserid=u.userid
                   left outer join haodou_passport.User ur on ur.userid=au.adminuserid
                   where au.adminusergroup=10 and date(u.updatetime) = '%s'
                   group by au.adminuserid,ur.username;
                   """,
             """select au.adminuserid,u.username,c.platform,c.type,count(c.commentid) 
                from haodou_comment.Comment c
                inner join haodou_admin.AdminUser au on au.adminuserid=c.userid
                left outer join haodou_passport.User u on u.userid=au.adminuserid
                where au.adminusergroup=10 and date(c.createtime) = '%s' and c.status=1
                group by au.adminuserid,u.username,c.platform,c.type;
                """,
             """select au.adminuserid,u.username,log.logtype,count(log.logid)
                 from haodou_admin.AdminLog log
                 inner join haodou_admin.AdminUser au on au.adminuserid=log.userid
                 left outer join haodou_passport.User u on u.userid=au.adminuserid
                 where date(log.createtime) = '%s' and au.adminusergroup=10
                 and log.menukey='recipe_recipe_cpsh' and log.logtype in(1,2,4,7)
                 group by au.adminuserid,u.username,log.logtype;
                 """,
             """select au.adminuserid,u.username,log.logtype,count(log.logid)
                from haodou_admin.AdminLog log
                inner join haodou_admin.AdminUser au on au.adminuserid=log.userid
                left outer join haodou_passport.User u on u.userid=au.adminuserid
                where date(log.createtime) = '%s'
                and log.menukey='recipe_album_album' and log.logtype in(1,2,3) and au.adminusergroup=10
                group by au.adminuserid,u.username,log.logtype;
                """,
             """select au.adminuserid,u.username,log.logtype,count(log.logid)
                from haodou_admin.AdminLog log
                inner join haodou_admin.AdminUser au on au.adminuserid=log.userid
                left outer join haodou_passport.User u on u.userid=au.adminuserid
                where date(log.createtime) = '%s'
                and log.menukey='recipe_recipe_photo' and log.logtype in(2,3) and au.adminusergroup=10
                group by au.adminuserid,u.username,log.logtype;
                """]
            
        # 查询的数据需要回写的SQL语句
        isqls=["""INSERT INTO bi_recipe_admin_operate_summary (createtime,userid,username,count,type,platform,model) VALUES (%s,%s,%s,%s,-1,-1,1)""",
            """INSERT INTO bi_recipe_admin_operate_summary (createtime,userid,username,count,type,platform,model) VALUES (%s,%s,%s,%s,-1,-1,2)""",
            """INSERT INTO bi_recipe_admin_operate_summary (createtime,userid,username,count,type,platform,model) VALUES (%s,%s,%s,%s,%s,%s,3)""",
            """INSERT INTO bi_recipe_admin_verify_summary (createtime,userid,username,count,logtype,type) VALUES (%s,%s,%s,%s,%s,1)""",
            """INSERT INTO bi_recipe_admin_verify_summary (createtime,userid,username,count,logtype,type) VALUES (%s,%s,%s,%s,%s,2)""",
            """INSERT INTO bi_recipe_admin_verify_summary (createtime,userid,username,count,logtype,type) VALUES (%s,%s,%s,%s,%s,3)"""]
        
       
        cnt=range(len(qsqls))
       
        for i in cnt:
           tjcur.execute(qsqls[i]  % rundate)
           result = []
           for row in tjcur.fetchall():
               result.append(tdate + row)
           bjcur.executemany(isqls[i],result)
       
        # close connection
        bjmysql.commit()
        bjmysql.close()
        tjmysql.commit()
        tjmysql.close()




def adminAnalysis(rundate):
            
        #make it into tuple
        tdate=(rundate,)
        
        #删除菜谱封面专辑当天的操作数据，如果存在的话
        # type
        # 0 创建数
        # 1 通过审核数
        # 2 1星/2星/3星/4星/5星专辑数
        # 3 评论数
        # 4 关注数
        # 5 分享数
        
        bjcur.execute("delete from bi_recipe_album_analysis where createtime='%s' and type in (0,1,2,3,4,5)" % rundate)
        
        # 查询需要的数据
        qsqls=["""select to_date(createtime) createtime,count(albumid),count(distinct(userid))
		from haodou_recipe.Album
		where to_date(createtime) = '%s'
		group by to_date(createtime)""",
        """select to_date(createtime) createtime,count(albumid),count(distinct(userid))
        		from haodou_recipe.Album
        		where to_date(createtime) = '%s' and status=1
        		group by to_date(createtime)""",
          """select to_date(createtime) createtime,rate,count(albumid)
         		from haodou_recipe.Album
         		where to_date(createtime) = '%s'
         		and status=1 and rate in (1,2,3,4,5)
         		group by to_date(createtime),rate;""", 
        """select to_date(createtime) createtime,platform,count(commentid),count(distinct(userid))
        		from haodou_comment.Comment
        		where type=1 and status=1 and to_date(createtime) = '%s'
        		group by to_date(createtime),platform""",
          """select to_date(createtime) createtime,count(albumid),count(distinct(userid))
          		from haodou_recipe_albumfollow.Albumfollow
          		where to_date(createtime) = '%s'  group by to_date(createtime)""",
         """select to_date(createtime) createtime,count(feedid),count(distinct(userid))
         		from haodou_center.UserFeed
         		where to_date(createtime) ='%s'
         		and type = 113  group by to_date(createtime)"""]
            
        # 查询的数据需要回写的SQL语句
        isqls=["INSERT INTO bi_recipe_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',-1,%s,%s,0)",
               "INSERT INTO bi_recipe_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',-1,%s,%s,1)",
               "INSERT INTO bi_recipe_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',-1,%s,%s,2)", 
               "INSERT INTO bi_recipe_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',%s,%s,%s,3)",
               "INSERT INTO bi_recipe_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',-1,%s,%s,4)",
               "INSERT INTO bi_recipe_album_analysis (createtime,platform,total,total_person,type) VALUES ('%s',-1,%s,%s,5)"]
        
       
        cnt=range(len(qsqls))
       
        for i in cnt:
           tjcur.execute(qsqls[i]  % rundate)
           result = []
           for row in tjcur.fetchall():
               result.append(tdate + row)
           bjcur.executemany(isqls[i],result)
       
    	
        #详情页阅读数  platform 0,2,4
	
    	sql = "delete from bi_recipe_album_analysis where createtime='%s' and type=6"
    	bjcur.execute(sql % rundate)
	
    	#web
    	sql = """\
    	select w.logdate,count(r.albumid),count(distinct(concat(remote_addr,'-',http_user_agent))) 
    	from www_haodou_com w
    	inner join hd_haodou_recipe_%s.album r on gamidfurl(w.path)=r.albumid
    	where w.logdate = '%s' and r.status=1 and gamidfurl(w.path) <> 0
    	group by w.logdate;
    	"""   
        


if __name__ == '__main__':

    if len(sys.argv) < 2:
        rundate = str(date.fromordinal(date.today().toordinal() -1 ))
    else:
        rundate = sys.argv[1]
        
    global bjmysql = MySQLdb.connect(host='211.151.138.246', user='bi',passwd='bi_haodou!@#',db='dw',charset="utf8")
    global bjcur = bjmysql.cursor()
    global tjmysql = MySQLdb.connect(host='10.1.1.70',user='bi',passwd='bi_haodou',charset='utf8')
    global tjcur = tjmysql.cursor()
    
    print u"当前分析时间是：" + rundate
    adminAnalysis(rundate)

    bjmysql.commit()
    bjmysql.close()
    tjmysql.commit()
    tjmysql.close()
    