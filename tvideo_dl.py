#!/usr/bin/env python3
""""
Twitter video download utility, only support Python3
requires:
ffmpeg-python
requests
beautifulsoup4
m3u8
"""
import os
import argparse

import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urlparse
import m3u8
from pathlib import Path
import re
import ffmpeg
import shutil


class TwitterDownloader:
    """
    tw-dl offers the ability to download videos from Twitter feeds.

    **Disclaimer** I wrote this to recover a video for which the original was lost. Consider copyright before downloading
    content you do not own.
    """
    video_player_prefix = 'https://twitter.com/i/videos/tweet/'
    video_api = 'https://api.twitter.com/1.1/videos/tweet/config/'
    tweet_data = {}

    def __init__(self, tweet_url, output_dir = './output', debug = 0, proxy=None):
        self.tweet_url = tweet_url
        self.output_dir = output_dir
        self.debug = debug
        if proxy:
            os.environ['HTTP_PROXY'] = proxy
            os.environ['HTTPS_PROXY'] = proxy

        if debug > 2:
            self.debug = 2
        """
        if the given tweet_url starts with https://t.cn , it means it's shorten url,
        It must be get the **real** url first
        """
        
        if not tweet_url.startswith('https://twitter.com/'):
            if proxy:
                rs = requests.head(tweet_url)
            else:
                rs = requests.head(tweet_url)
            if rs.status_code == 301:
                self.tweet_url = rs.headers['location']
                
        """
        tweet_url  https://twitter.com/realDonaldTrump/status/1166769660450226177/video/1
        usrlpaser 
        ParseResult(scheme='https', netloc='twitter.com', path='/realDonaldTrump/status/1166769660450226177/video/1', params='', query='', fragment='')
        """

        path = urlparse(self.tweet_url).path
        self.tweet_data['tweet_url'] = self.tweet_url
        self.tweet_data['user'] = path.split('/')[1]
        self.tweet_data['id'] = path.split('/')[3]

        output_path = Path(output_dir)
        storage_dir = output_path / self.tweet_data['user'] 
        Path.mkdir(storage_dir, parents = True, exist_ok = True)
        self.storage = str(storage_dir)



        self.requests = requests.Session()

    def download(self):
        self.__debug('Tweet URL', self.tweet_data['tweet_url'])
        # video has already download ?
        video_fpath = self.storage + '/' + self.tweet_data['id'] + '.mp4'
        if os.path.exists(video_fpath):
            print("video has downloaded, skip it")
            return video_fpath
        # Get the bearer token
        token = self.__get_bearer_token()

        # Get the M3u8 file - this is where rate limiting has been happening
        video_host, playlist = self.__get_playlist(token)

        if playlist.is_variant:
            #print('Multiple resolutions found. Slurping all resolutions.')
            print("Multiple solutions found, try to download the most high solution video")

           
            """
            playlists #EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=256000,RESOLUTION=480x270,CODECS="mp4a.40.2,avc1.4d001e"
            /ext_tw_video/1166768947112415233/pu/pl/480x270/7wz-fvD0iI1qf3JT.m3u8
            #EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=832000,RESOLUTION=640x360,CODECS="mp4a.40.2,avc1.4d001f"
            /ext_tw_video/1166768947112415233/pu/pl/640x360/QDXRskHOUfOYMpDT.m3u8
            #EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=2176000,RESOLUTION=1280x720,CODECS="mp4a.40.2,avc1.640020"
            /ext_tw_video/1166768947112415233/pu/pl/1280x720/C-hLXTyyNB8tXrfI.m3u8
            """
            playlists = {}
            for plist in playlist.playlists:
                playlists[plist.stream_info.bandwidth] = {
                    'resolution': '{}x{}'.format(*plist.stream_info.resolution),
                    'fpath': Path(self.storage) / Path(self.tweet_data['id'] + '.mp4'),
                    'video_url': video_host + plist.uri
                }

            # pick the best quality video
            curr_record = playlists[max(playlists.keys())]

            print("Downloading ", curr_record['resolution'])
            
            self._get_video_content(video_host = video_host, fpath=curr_record['fpath'], video_url = curr_record['video_url'])
            return video_fpath
        else:
            print("Single solution file not support yet")
            return None

    def _get_video_content(self, video_host, fpath, video_url, ):

        ts_m3u8_response = self.requests.get(video_url, 
                                headers = {'Authorization': None})
        ts_m3u8_parse = m3u8.loads(ts_m3u8_response.text)

        ts_list = []
        ts_full_file_list = []

        for ts_uri in ts_m3u8_parse.segments.uri:
            # ts_list.append(video_host + ts_uri)

            ts_file = requests.get(video_host + ts_uri)
            fname = ts_uri.split('/')[-1]
            ts_path = Path(self.storage) / Path(fname)
            ts_list.append(ts_path)

            ts_path.write_bytes(ts_file.content)

        ts_full_file = Path(self.storage) / Path(self.tweet_data['id'] + '.ts')
        ts_full_file = str(ts_full_file)
        ts_full_file_list.append(ts_full_file)

        # Shamelessly taken from https://stackoverflow.com/questions/13613336/python-concatenate-text-files/27077437#27077437
        with open(str(ts_full_file), 'wb') as wfd:
            for f in ts_list:
                with open(f, 'rb') as fd:
                    shutil.copyfileobj(fd, wfd, 1024 * 1024 * 10)

        for ts in ts_full_file_list:
            print('\t[*] Doing the magic ...')
            ffmpeg\
                .input(ts)\
                .output(str(fpath), acodec = 'copy', vcodec = 'libx264', format = 'mp4', loglevel = 'error')\
                .overwrite_output()\
                .run()

        print('\t[+] Doing cleanup')

        for ts in ts_list:
            p = Path(ts)
            p.unlink()

        for ts in ts_full_file_list:
            p = Path(ts)
            p.unlink()

    def __get_bearer_token(self):
        video_player_url = self.video_player_prefix + self.tweet_data['id']
        video_player_response = self.requests.get(video_player_url)
        self.__debug('Video Player Body', '', video_player_response.text)

        js_file_soup = BeautifulSoup(video_player_response.text, 'html.parser')
        js_file_url = js_file_soup.find('script')['src']
        js_file_response = self.requests.get(js_file_url)
        self.__debug('JS File Body', '', js_file_response.text)

        bearer_token_pattern = re.compile('Bearer ([a-zA-Z0-9%-])+')
        bearer_token = bearer_token_pattern.search(js_file_response.text)
        bearer_token = bearer_token.group(0)
        self.requests.headers.update({'Authorization': bearer_token})
        self.__debug('Bearer Token', bearer_token)

        return bearer_token


    def __get_playlist(self, token):
        config_url = self.video_api + self.tweet_data['id'] + '.json'
        self.__debug("config url", config_url)
        player_config_req = self.requests.get(config_url)

        self.__debug("request player config status:", str(player_config_req.status_code))

        player_config = json.loads(player_config_req.text)

        if 'errors' not in player_config:
            self.__debug('Player Config JSON', '', json.dumps(player_config))
            m3u8_url = player_config['track']['playbackUrl']

        else:
            self.__debug('Player Config JSON - Error', json.dumps(player_config['errors']))
            print('[-] Rate limit exceeded. Try again later.')
            sys.exit(1)

        # Get m3u8
        m3u8_response = self.requests.get(m3u8_url)
        self.__debug('M3U8 Response', '', m3u8_response.text)

        m3u8_url_parse = urlparse(m3u8_url)
        video_host = m3u8_url_parse.scheme + '://' + m3u8_url_parse.hostname

        m3u8_parse = m3u8.loads(m3u8_response.text)

        return [video_host, m3u8_parse]

    def __debug(self, msg_prefix, msg_body, msg_body_full = ''):
        if self.debug == 0:
            return

        if self.debug == 1:
            print('[Debug] ' + '[' + msg_prefix + ']' + ' ' + msg_body)

        if self.debug == 2:
            print('[Debug] ' + '[' + msg_prefix + ']' + ' ' + msg_body + ' - ' + msg_body_full)

if __name__ == '__main__':
    import sys

    if sys.version_info[0] == 2:
        print('Python3 is required.')
        sys.exit(1)

    parser = argparse.ArgumentParser()
    parser.add_argument('tweet_url', help = 'The video URL on Twitter (https://twitter.com/<user>/status/<id>).')
    parser.add_argument('-o', '--output', dest = 'output', default = './output', help = 'The directory to output to. The structure will be: <output>/<user>/<id>.')
    parser.add_argument('-d', '--debug', default = 0, action = 'count', dest = 'debug', help = 'Debug. Add more to print out response bodies (maximum 2).')
    parser.add_argument('-x', '--proxy', dest='proxy', help="Use proxy instead of directly connection")

    args = parser.parse_args()

    twitter_dl = TwitterDownloader(args.tweet_url, args.output, args.debug, args.proxy)
    video_fpath = twitter_dl.download()
    print("Vide has downloaded at {}".format(video_fpath))