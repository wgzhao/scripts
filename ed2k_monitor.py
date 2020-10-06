#!/usr/bin/python -tt
#-*- coding:utf-8 -*-
__DOC__="""
monitor your clipboard
convert your ed2k links into "correct" encode
"""

import gtk
import urllib.request, urllib.error, urllib.parse
import re

class Ed2kConvert(object):

    def __init__(self):
        self.clip = gtk.clipboard_get(gtk.gdk.SELECTION_CLIPBOARD)
        self.clip.connect("owner-change", self._clipboard_changed)


    def ed2k_handler(self,links):
        '''
        use unquote method included by urllib2 correct nonormal url
        '''
        cvtlinks = []
        for link in links:
            link = urllib.parse.unquote(link)
            cvtlinks.append(link)
        self.clip.set_text('\n'.join(cvtlinks))


    def _clipboard_changed(self,clipboard, event):
        self.text = clipboard.wait_for_text()
        #print text
        if self.text:
            self.plugins_routing()

    def plugins_routing(self):
        p = re.compile(r'(ed2k://.*?/)')
        matches = p.findall(self.text.replace('\n', ''))
        if not matches:
            return False
        else:
            self.ed2k_handler(matches)

if __name__ == '__main__':
    s = Ed2kConvert()
    gtk.main()
