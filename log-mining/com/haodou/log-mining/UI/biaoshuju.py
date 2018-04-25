#encoding=utf-8

import sys
import os.path
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import codecs

import urllib

sys.path.append("../util")
from utf8 import *
import random
import os
import htmlUtil

sys.path.append("../feedback")
import search

TagFileDir="../tag/code0327/"
TagFile=TagFileDir+"mergeCateName.txt"
RecipeStepDir="/data/tmp/"

from tornado.options import define, options
define("port", default=80, help="run on the given port", type=int)

class IndexHandler(tornado.web.RequestHandler):
	def get(self):
		self.render('index.html')

class TagHandler(tornado.web.RequestHandler):
	def get(self):
		self.render('tag.html')

class FeedHandler(tornado.web.RequestHandler):
        def get(self):
                self.render('feed.html')
	
class PoemPageHandler(tornado.web.RequestHandler):
	def post(self):
		noun1 = self.get_argument('noun1')
		noun2 = self.get_argument('noun2')
		verb = self.get_argument('verb')
		noun3 = self.get_argument('noun3')
		self.render('poem.html', roads=noun1, wood=noun2, made=verb,difference=noun3)		

class ParentsPageHandler(tornado.web.RequestHandler):
	def post(self):
		word=self.get_argument('word')
		ws={}
		ws[word]=1
		wps={}
		while len(ws) > 0:
			print ws
			newWs={}
			for line in codecs.open(TagFile,"r","utf-8"):
				cols=line.strip().split("\t")
				if len(cols) < 2:
					continue
				w=cols[0]
				if w not in ws:
					continue
				if w not in wps:
					wps[w]={}
				for p in cols[1:]:
					wps[w][p]=1
					if p not in wps:
						newWs[p]=1
						wps[p]={}
			ws=newWs
		self.render('tag_parents.html',word=word,wps=wps)		

def findLines(origFile,w1,w2):
	lines=[]
	if len(w1) <=0 and  len(w2) <= 0:
		return lines
	file="./tmp/_tmp_.%d"%(random.randrange(100000))
	os.system("rm "+file+";")	
	cmd="cat "+origFile
	if len(w1) > 0:
		cmd+=" | grep "+ustr(w1)
	if len(w2) > 0:
		cmd+=" | grep "+ustr(w2)
	cmd+="> "+file
	os.system(cmd)
	for line in codecs.open(file,"r","utf-8"):		
			lines.append(line)
			if len(lines) > 1000:
				break
	os.system("rm "+file+";")
	return lines

def getValue(self,s,default=""):
	try:
		return self.get_argument(s)
	except:
		return default

class SearchRecipeHandler(tornado.web.RequestHandler):
	def post(self):
		w1=getValue(self,"word1")
		w2=getValue(self,"word2")
		lines=findLines(RecipeStepDir+"recipeStep.txt",w1,w2)
		news=[]
		for line in lines:
			news.append(htmlUtil.boldSub2(line,w1,w2))
		#print len(news)
		self.render("tag_recipe.html",word1=w1,word2=w2,lines=news)



class FeedSearchHandler(tornado.web.RequestHandler):
	def getTime(self):
		begin=getValue(self,"begin",default="0000-00-00 00:00:00")
		end=getValue(self,"end",default="9999-99-99 00:00:00")
		return (begin,end)

	def postTime(self):
		y1=getValue(self,"syear1")
                m1=getValue(self,"smonth1")
               	d1=getValue(self,"sday1")
              	h1=getValue(self,"shour1")
                i1=getValue(self,"sminute1")
                s1=getValue(self,"ssecond1")
                y2=getValue(self,"syear2")
                m2=getValue(self,"smonth2")
                d2=getValue(self,"sday2")
                h2=getValue(self,"shour2")
                i2=getValue(self,"sminute2")
                s2=getValue(self,"ssecond2")
                begin="%s-%s-%s %s:%s:%s"%(formatNum(y1),formatNum(m1),formatNum(d1),formatNum(h1),formatNum(i1),formatNum(s1))
                end="%s-%s-%s %s:%s:%s"%(formatNum(y2),formatNum(m2),formatNum(d2),formatNum(h2),formatNum(i2),formatNum(s2))
		return (begin,end)

	def response(self,begin,end):
		word=getValue(self,"word").encode("utf-8")
		feeds,subs=search.searchW(word,begin,end)
		newSubs=[]
		for w,sn in subs:
			newSubs.append((w.decode("utf-8"),sn,urllib.quote(w+" "+word)))
		news=[]
		nn=0
		for feed in feeds:
			ncs=[]
			ws=word.split()
			w1=""
			w2=""
			if len(ws) > 0:w1=ws[0]
			if len(ws) > 1:w2=ws[1]
			cs=htmlUtil.split(feed.content,w1,w2)
			for c in cs:
				ncs.append(c.decode("utf-8"))
			ncs.append(feed.uuid)
			ncs.append(feed.channel)
			ncs.append(feed.createTime)
			ncs.append(feed.version)
			ncs.append(feed.tagStr())
			news.append(ncs)
			nn+=1
			if nn > 300:
				break
		self.render("feed_search.html",begin=begin,ubegin=urllib.quote(begin),end=end,uend=urllib.quote(end),word=word.decode("utf-8"),lines=news,subs=newSubs,totalNum=len(feeds))
	
	def get(self):
		begin,end=self.getTime()
		self.response(begin,end)

	def post(self):
		begin,end=self.postTime()
		self.response(begin,end)		

def formatNum(n):
	n=n.encode("utf-8")
	if len(n) == 0:
		return "00"
	if len(n) == 1:
		return "0"+n
	else:
		return n
	

class FeedNewHandler(tornado.web.RequestHandler):
	def post(self):
		isNew=False
		newType=getValue(self,"isNew").encode("utf-8")
		if newType == "新词":
			isNew=True
		y1=getValue(self,"year1")
		m1=getValue(self,"month1")
		d1=getValue(self,"day1")
		h1=getValue(self,"hour1")
		i1=getValue(self,"minute1")
		s1=getValue(self,"second1")
                y2=getValue(self,"year2")
                m2=getValue(self,"month2")
                d2=getValue(self,"day2")
                h2=getValue(self,"hour2")
                i2=getValue(self,"minute2")
                s2=getValue(self,"second2")
		begin="%s-%s-%s %s:%s:%s"%(formatNum(y1),formatNum(m1),formatNum(d1),formatNum(h1),formatNum(i1),formatNum(s1))
		end="%s-%s-%s %s:%s:%s"%(formatNum(y2),formatNum(m2),formatNum(d2),formatNum(h2),formatNum(i2),formatNum(s2))		
		phaseDict=search.phaseW(begin,end)
		if isNew:
			phaseDict=sorted(phaseDict, lambda x, y: cmp(x[1], y[1]), reverse=True)
		ws=[]
		i=0
		N=500
		for w,v,n,av,an in phaseDict:
			i+=1
			if i > N:
				break
			ws.append((urllib.quote(w),w.decode("utf-8"),an,n))
		self.render("feed_new.html",begin=begin,ubegin=urllib.quote(begin),end=end,uend=urllib.quote(end),lines=ws)		

if __name__ == "__main__":
	tornado.options.parse_command_line()
	app = tornado.web.Application(
		handlers=[
			(r"/", IndexHandler),
			(r"/tag", TagHandler),
			(r"/feed",FeedHandler),
			(r'/tag/parents', ParentsPageHandler),
			(r'/tag/recipe',SearchRecipeHandler),
			(r'/feed/search',FeedSearchHandler),
			(r'/feed/new',FeedNewHandler),
			],
		template_path=os.path.join(os.path.dirname(__file__), "templates")
	)
	http_server = tornado.httpserver.HTTPServer(app)
	http_server.listen(options.port)
	tornado.ioloop.IOLoop.instance().start()



