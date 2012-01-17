#!/usr/bin/python -tt
#-*- coding:utf-8 -*-
__Author__ = 'wgzhao, wgzhao##gmail.com'
__URL__ = 'http://blog.wgzhao.com/2010/05/28/core-python-\
    programming-2nd-chapter-19-exercise.html'
'''
Core Python Programming 2nd exercise
chapter 19
'''
from Tkinter import *
from tkFileDialog   import asksaveasfilename, askopenfilename
from tkSimpleDialog import askstring
from tkMessageBox import askyesnocancel, showinfo
from commands import getoutput

class SimpleEdit:
    '''
    python/tkinter based simple plain text editor
    '''

    def __init__(self,filename=None):
        self.deftitle = 'new document'
        self.top = Tk()
        self.top.geometry('400x300')
        self.top.title('Simple Text Editor V0.1')
        self.menu = Menu(self.top)
        self.top.config(menu=self.menu)

        #file menu
        filemenu= Menu(self.menu)
        self.menu.add_cascade(label="File", menu=filemenu)
        filemenu.add_command(label="New Ctrl+N", command=self.newfile)
        filemenu.add_command(label="Open... Ctrl+O", command=self.openfile)
        filemenu.add_command(label="Save... Ctrl+S", command=self.savefile)
        filemenu.add_separator()
        filemenu.add_command(label="Exit Ctrl+Q", command=self.quit)

        #tools menu
        toolmenu=Menu(self.menu)
        self.menu.add_cascade(label='Tool',menu=toolmenu)
        toolmenu.add_command(label='Find... Ctrl+F',command=self.findstr)
        toolmenu.add_command(label='Spell check',command=self.spellcheck)

        #help menu
        helpmenu = Menu(self.menu)
        self.menu.add_cascade(label="Help", menu=helpmenu)
        helpmenu.add_command(label="About...", command=self.about)

        #textarea
        self.fm = Frame(self.top)
        self.fm.pack(side=TOP)

        #file name label
        self.ftitle = StringVar(self.fm)
        self.fname = Label(self.fm, textvariable=self.ftitle)
        self.ftitle.set(self.deftitle)
        self.fname.pack(side=TOP)

        self.textarea = Text(self.fm, wrap=WORD, width=82, height=300)
        self.scroll = Scrollbar(self.fm)
        self.textarea.configure(yscrollcommand=self.scroll.set)
        self.textarea.pack(side=LEFT)
        self.scroll.pack(side=RIGHT, fill=Y)


        #shortcut keyboard n->new,o->open,s->save,q->quit
        for key in 'nosqf':
            self.top.bind_all('' % key, self.handler)
        if filename:
            self.textarea.insert('1.0',open(filename).read())
            self.ftitle.set(filename)

    def handler(self, event):
        func = {'s': self.savefile, 'n': self.newfile, \
                'o': self.openfile, 'q': self.quit, 'f': self.findstr}
        keyname = event.keysym
        apply(func[keyname])

    def quit(self):
        if self.textarea.edit_modified():
            ans = askyesnocancel('Save', "file has modified,save it before exit?")
            if ans:
                self.savefile()
            elif ans == False:
                self.top.quit()
            elif ans == None:
                pass
        else:
            self.top.quit()

    def newfile(self):
        self.textarea.delete('1.0', END)
        self.ftitle.set(self.deftitle)
        self.textarea.edit_modified(False)

    def openfile(self):
        filename = askopenfilename()
        text = open(filename, 'r').read()

        self.textarea.insert(INSERT, text)


        #set the file title
        self.ftitle.set(filename)
        self.textarea.edit_modified(False)

    def savefile(self):
        if self.ftitle.get() != self.deftitle:
            filename = self.ftitle.get()
        else: #a new document has not save yet
            filename = asksaveasfilename()
        if filename:
            alltext = self.textarea.get('1.0', END)
            open(filename, 'w').write(alltext)
            self.ftitle.set(filename)

    def about(self):
        showinfo(title='About', message='''
            SimpleEdit V1.0
                wgzhao
            http://blog.wgzhao.com
            ''')

    def findstr(self):
        '''
        find word or string
        '''
        target = askstring('SimpleEditor', 'Search String?')
        if target:
            where = self.textarea.search(target, INSERT, END)
            if where:                                    
                pastit = where + ('+%dc' % len(target))
                #self.text.tag_remove(SEL, '1.0', END)
                self.textarea.tag_add(SEL, where, pastit)
                self.textarea.mark_set(INSERT, pastit)
                self.textarea.see(INSERT)
                self.textarea.focus()

    def spellcheck(self):
        #auto save it before spell check
        self.savefile()
        result = getoutput('spell %s' % self.ftitle.get()).split('\n')
        if len(result):
            self.textarea.tag_config('match',background='yellow')
            begin = '1.0'
            for word in result:
                where = self.textarea.search(word,begin,END)
                pos = where + "+%dc" % len(word)
                self.textarea.tag_add('match',where,pos)
                begin = pos #find from current position

def main():
    s = SimpleEdit()
    mainloop()

if __name__ == '__main__':
    main()
