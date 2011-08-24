#!/usr/bin/python
# coding: utf-8
#
# Copyright(R) 2010 Tualatrix Chou <tualatrix@gmail.com>
# http://imtx.me
#
# The easiest way to download videos from sohu.com

import sys
reload(sys)
sys.setdefaultencoding('utf8')
import re
import os
import optparse
import urllib
import simplejson

def get_vid_and_pid(url):
    vid_pattern = re.compile('var vid="(\d+)"')
    pid_pattern = re.compile('var pid ="(\d+)"')

    print("Try to get vid and pid...")
    html = urllib.urlopen(url).read()
    return vid_pattern.findall(html)[0], pid_pattern.findall(html)[0]

def get_video_file(vid, pid):
    url = 'http://hot.vrs.sohu.com/vrs_flash.action?vid=%s&pid=%s' % (vid, pid)
    json = simplejson.loads(urllib.urlopen(url).read())
    tv_name = json['data']['tvName']

    for num, video_url in enumerate(json['data']['clipsURL']):
        basename = os.path.basename(video_url)
        os.system('wget %s' % video_url)
        os.system('mv -v %s %s' % (basename, tv_name + '-0%d.mp4' % (num + 1)))

def get_hd_video_file(vid, pid):
    url = 'http://hot.vrs.sohu.com/vrs_flash.action?vid=%s&pid=%s' % (vid, pid)
    json = simplejson.loads(urllib.urlopen(url).read())
    tv_name = json['data']['tvName']
    clip_urls = json['data']['clipsURL']
    allot_ip = json['allot']
    su_urls = json['data']['su']

    for num, clip_url in enumerate(json['data']['clipsURL']):
        tv_url = clip_url.split('tv')[1]
        su_url = su_urls[num]
        next_url = 'http://%s/?prot=2&file=/tv%s&new=%s' % (allot_ip, tv_url, su_url)
        next_data = urllib.urlopen(next_url).read().split('|')
        print("Get next data: %s" % next_data)
        new_url, key = next_data[0], next_data[3]
        final_url = '%stv%s?key=%s' % (new_url, tv_url, key)
        basename = os.path.basename(final_url)
        print("Get final url: %s" % final_url)
        os.system('wget  %s' % final_url)
        os.system('mv -v %s %s' % (basename, tv_name + '-0%d.mp4' % (num + 1)))

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: %s [url1 url2 ...] -g' % sys.argv[0])
        sys.exit(1)

    parser = optparse.OptionParser(prog="sohu-video-downloader",
                                   description="")
    parser.add_option("-g", "--gq", action="store_true", default=False,
                      help="Download HD quailty video.  [default: %default]")
    options, args = parser.parse_args()

    if options.gq:
        ENABLE_HD = True
    else:
        ENABLE_HD = False

    for url in sys.argv[1:]:
        if url.startswith('http'):
            vid, pid = get_vid_and_pid(url)
            if ENABLE_HD:
                get_hd_video_file(vid, pid)
            else:
                get_video_file(vid, pid)
