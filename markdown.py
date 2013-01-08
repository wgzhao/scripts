#!/usr/bin/env python
'''
Hey, this is GPLv3 and copyright 2010 Jezra
'''
import gtk
import webkit
import markdown #http://www.freewisdom.org/projects/python-markdown/
global default_file
default_file = "markdown.txt"
try:
    import gtkspell
    has_gtkspell=True
except:
    has_gtkspell=False

class application:

    def __init__(self):
        #build the UI
        winder = gtk.Window(gtk.WINDOW_TOPLEVEL)
        winder.connect("destroy",self.quit)
        winder.maximize()
        #add some accellerators
        #create an accellerator group for this window
        accel = gtk.AccelGroup()
        #add the ctrl+q to quit
        accel.connect_group(gtk.keysyms.q, gtk.gdk.CONTROL_MASK, gtk.ACCEL_VISIBLE, self.quit_accel )
        #add ctrl+s to save
        accel.connect_group(gtk.keysyms.s, gtk.gdk.CONTROL_MASK, gtk.ACCEL_VISIBLE, self.save_accel )
        #lock the group
        accel.lock()
        #add the group to the window
        winder.add_accel_group(accel)
        #we need a paned
        pane = gtk.HPaned()
        #and a vbox
        box = gtk.VBox(False)
        #add the pane to the box
        box.pack_start(pane)
        #add the box to the window
        winder.add(box)
        #do the text crap for the first pane
        self.tb = gtk.TextBuffer()
        tv = gtk.TextView(self.tb)
        tv.set_wrap_mode(gtk.WRAP_WORD)
        #try and add spell checking to the textview
        if has_gtkspell:
            #what if there is no aspell library?
            try:
                self.spell = gtkspell.Spell(tv)
                self.has_spell_library=True
            except Exception:
                #bummer
                self.has_spell_library=False
                print Exception.message
        #add the text view to a scrollable window
        input_scroll = gtk.ScrolledWindow()
        input_scroll.add_with_viewport(tv)
        input_scroll.set_policy(gtk.POLICY_AUTOMATIC,gtk.POLICY_AUTOMATIC)
        #add to the pane
        pane.pack1(input_scroll,True)
        #make the HTML viewable area
        self.wv = webkit.WebView()
        #disable the plugins for the webview
        ws = self.wv.get_settings()
        ws.set_property('enable-plugins',False)
        self.wv.set_settings(ws)
        out_scroll = gtk.ScrolledWindow()
        out_scroll.add(self.wv)
        out_scroll.set_policy(gtk.POLICY_AUTOMATIC,gtk.POLICY_AUTOMATIC)
        pane.add2(out_scroll)
        #we will add buttons at the bottom
        bbox = gtk.HButtonBox()
        bbox.set_layout(gtk.BUTTONBOX_START)
        #we need buttons!
        savebutton = gtk.Button("Save")
        savebutton.connect("clicked", self.save)
        markdownbutton = gtk.Button("_Markdown",use_underline=True)
        markdownbutton.connect("clicked", self.markdown)
        #add the buttons to the bbox
        bbox.pack_start(savebutton,False,False,0)
        bbox.pack_start(markdownbutton,False,False,0)
        #add the bbox to the box
        box.pack_start(bbox,False,False,0)
        winder.show_all()
        self.read_default_file()
    def run(self):
        gtk.main()
    def quit(self,widget=None):
        self.save()
        gtk.main_quit()
    def markdown(self,widget):
        text = self.get_buffer_text()
        mdtext = markdown.markdown(text)
        self.wv.load_html_string(mdtext,"file:///")
    def read_default_file(self):
        try:
            f = open(default_file,"r")
            text = f.read()
            self.tb.set_text(text)
            f.close()
        except:
            pass
    def quit_accel(self,accel_group, acceleratable, keyval, modifier):
        self.quit()
    def save_accel(self,accel_group, acceleratable, keyval, modifier):
        self.save()
    def save(self,widget=None):
        #get the text
        text = self.get_buffer_text()
        f = open(default_file,"w")
        f.write(text)
        f.close()
    def get_buffer_text(self):
        start_iter = self.tb.get_start_iter()
        end_iter = self.tb.get_end_iter()
        text=self.tb.get_text(start_iter,end_iter)
        return text
if "__name__"=="__main__":
    a = application()
    a.run()