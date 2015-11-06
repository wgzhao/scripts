#!/usr/bin/env python3
# -*- coding:utf-8 -*-
'''
将文本文件转为epub格式电子书
自动抽取文本文件中的章节来生成目录
这里假定文本的第一行为书名，而第二行为书作者
'''

import re
import os
import sys
import argparse
from ebooklib import epub


def create_ebook(args):
   
    pattern = re.compile(r'^第[一二三四五六七八九十1234567890]{1,}(章|回) ',re.UNICODE |re.L |re.M|re.X)
    filepath = args.filepath
    lines = open(filepath).readlines()
    rows = len(lines)
    
    # 假定第一行是标题，第二行为作者
    # 标题可以有书名号，作者一行可能有作者：这样的关键字
    if args.author:
        author = args.author
    else:
        author = lines[1].replace('作者：','').strip()
    
    if args.title:
        title = args.title
    else:
        title = lines[0].replace('《','').replace('》','').strip()
    book = epub.EpubBook()
    
    #set metadata
    book.set_identifier('id1234567890')
    book.set_title(title)
    book.set_language('zh')
    
    book.add_author(author)
    
    if args.cover:
        basename = os.path.basename(args.cover)
        book.set_cover('cover.jpg',open(args.cover,'rb').read())


    # 抽取所有的章节名字以及章节所在的行数，形成列表,最后形成的格式如下
    #[(0,'正文'),(10,'第一章'),.....(678,'第八章')]

    toc=[(0,'正文')]
  
    for i in range(rows):
        if pattern.match(lines[i].strip()):
            #这是一个章节名，记录到章节目录中
            toc.append((i,lines[i].strip()))
    
    #增加一个虚拟章节，主要是记录书结尾的行数
    toc.append((i,'结尾'))
        
    
    #定义TOC
    book.toc = []
    # basic spine
    book.spine = ['nav']
    for i in range(len(toc) - 1):
        
        #保存章节内容的文件名
        ch_filename= 'chapter-%03d.html' % i
        #章节名字
        ch_name = toc[i][1]
        #增加目录名
        book.toc.append(epub.Link(ch_filename,ch_name,ch_name))
        #创建章节
        ch = epub.EpubHtml(title="<h2>" + ch_name + "</h2>",file_name=ch_filename,lang='zh')
        #增加章节内容，为刚章节的行数到下一个章节的行数之间的总行数内容
        ch.content = '<br/>'.join(lines[toc[i][0]:toc[i + 1][0]])
        #将章节追加到书中
        book.add_item(ch)
        #将章节目录追加到书脊中
        book.spine.append(ch)
        
    #add default NCX and Nav file
    book.add_item(epub.EpubNcx())
    book.add_item(epub.EpubNav())
    
    #define CSS style
    #适应平板
    css_info = R'''
   @namespace epub "http://www.idpf.org/2007/ops";
   body {
       font-family: Cambria, Liberation Serif, Bitstream Vera Serif, Georgia, Times, Times New Roman, serif;
   }
   h2 {
        text-align: left;
        text-transform: uppercase;
        font-weight: 200;     
   }
   ol {
           list-style-type: none;
   }
   ol > li:first-child {
           margin-top: 0.3em;
   }
   nav[epub|type~='toc'] > ol > li > ol  {
       list-style-type:square;
   }
   nav[epub|type~='toc'] > ol > li > ol > li {
           margin-top: 0.3em;
   }
    '''
    nav_css = epub.EpubItem(uid='style_nav',file_name='style.css',media_type='text/css',content=css_info)
    
    # add CSS file
    book.add_item(nav_css)
    
    # write to the file
    if args.outfile:
        outpath = args.outfile
    else:
        outpath = "%s.epub" % title
    epub.write_epub(outpath,book,{})
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Convert text to e-pub/mobi ebook')
    parser.add_argument('--file','-f',required=True,dest='filepath',help='the text-book filepath')
    parser.add_argument('--author','-a',dest='author',help="the book's author")
    parser.add_argument('--name','-t',dest='title',help="the book's title")
    parser.add_argument('--cover','-c',dest='cover',help="the book's cover filepath(support jpeg,png)")
    parser.add_argument('--output','-o',dest='outfile',help="the output ebook filepath")
    #parser.add_argument('--format','-m',dest='out format',help="the output ebook format,need kindlegen")
    if len(sys.argv) < 2:
        parser.print_help()
    else:
        args = parser.parse_args()
        create_ebook(args)
 
        