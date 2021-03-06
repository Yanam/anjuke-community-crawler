#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Lukin'

import os
import re
import sys
import time
from urllib.parse import urlparse
from configparser import ConfigParser

ABS_PATH = os.path.dirname(os.path.realpath(sys.argv[0]))

def config(node, key):
    '''
    读取配置
    @:param string node
    @:param string key
    @:return mixed
    '''
    cfg = ConfigParser()
    cfg.read_file(open(ABS_PATH + '/config.ini'))
    return cfg.get(node, key)

def t2date(timestamp, format="%Y-%m-%d %X"):
    """
    转换时间戳为字符串
    @param timestamp    时间戳
    @param format       格式化字符串
    """
    return time.strftime(format, time.localtime(timestamp))

def log(msg):
    """
    日志
    @param msg  日志内容
    """
    print("[%s][PID: %s] %s" % (t2date(time.time(), '%X'), os.getpid(), msg))

def format_url(base_url, html):
    """
    格式化URL
    @param base_url     基础URL
    @param html         html内容
    @return string      处理之后的html
    """
    urls = []
    matches = re.findall(r'''(<(?:a|link)[^>]+?href=([^>\s]+)[^>]*>)''', html, re.I)
    if matches != None:
        for url in matches:
            if (url in urls) == False:
                urls.append(url)

    matches = re.findall(r'''(<(?:img|script)[^>]+?src=([^>\s]+)[^>]*>)''', html, re.I)
    if matches != None:
        for url in matches:
            if (url in urls) == False:
                urls.append(url)

    if urls.count == 0: return html
    # parse url
    aurl = urlparse(base_url)
    # base host
    base_host = "%s://%s" % (aurl.scheme, aurl.netloc)
    # base path
    if aurl.path:
        base_path = os.path.dirname(aurl.path) + '/'
    else:
        base_path = '/'
        # base url
    base_url = base_host + base_path
    # foreach urls
    for tag in urls:
        url = tag[1].strip('"').strip("'")
        # url empty
        if url == '': continue
        # http https ftp skip
        if re.search(r'''^(http|https|ftp)\:\/\/''', url, re.I): continue
        # 邮件地址,javascript,锚点
        if url[0] == '#' or url[0:7] == 'mailto:' or url[0:11] == 'javascript:': continue
        # 绝对路径 /xxx
        if url[0] == '/':
            url = base_host + url

        # 相对路径 ../xxx
        elif url[0:3] == '../':
            while url[0:3] == '../':
                url = url[3:]
                if len(base_path) > 0:
                    base_path = os.path.dirname(base_path)
                    if base_path == '/': base_path = ''

                if url == '../':
                    url = ''
                    break

            url = base_host + base_path + '/' + url
        # 相对于当前路径 ./xxx
        elif url[0:2] == './':
            url = base_url + url[2:]
        # 其他
        else:
            url = base_url + url
            # 替换标签
        href = tag[0].replace(tag[1], '"%s"' % (url))
        html = html.replace(tag[0], href)

    return html

def get_urls(html):
    """
    取得所有连接
    @param html     html内容
    @return array   url list
    """
    urls = []
    if len(html) == 0: return urls
    matches = re.findall(r'''<a[^>]+href=([^>\s]+)[^>]*>''', html, re.I)
    if matches != None:
        for href in matches:
            url = href.strip('"').strip("'")
            if url == '' or url == '#': continue
            anchor = url.find('#')
            if anchor != -1:
                url = url[0:anchor]
            urls.append(url)

    return urls

def read_file(path):
    """
    read_file
    @param path     文件路径
    """
    fp = open(path, 'rb')
    r = fp.read()
    fp.close()
    return r

def mid(content, start, end=None, clear=None):
    """
    字符串截取函数

    @param content      内容
    @param start        开始字符串   以括号"("开始且结尾的则按照正则表达式执行
    @param end          结束字符串   以括号"("开始且结尾的则按照正则表达式执行
    @param clear        清理
    @return string      截取之后的内容

    """
    if len(content) == 0 or len(start) == 0: return ''
    # start
    if start[0] == '(' and start[-1] == ')':
        start = re.search(start, content, re.I)
        if start == None:
            return ''
        else:
            start = start.group()

    # end
    if end and end[0] == '(' and end[-1] == ')':
        end = re.search(end, content, re.I)
        if end == None:
            end = ''
        else:
            end = end.group()

    # find start
    start_pos = content.find(start)
    if start_pos == -1 or end == '': return ''
    # substr
    if end == None:
        content = content[start_pos:]
    else:
        start_len = len(start)
        end_pos = content[start_pos + start_len:].find(end)
        if end_pos == -1:
            return ''
        else:
            content = content[start_pos + start_len: end_pos + start_pos + start_len]

    # clear
    if isinstance(clear, list) or isinstance(clear, tuple):
        for rule in clear:
            if rule[0] == '(' and rule[-1] == ')':
                content = re.sub(rule, '', content, re.I | re.S)
            else:
                content = content.replace(rule, '')
    elif clear != None:
        if clear[0] == '(' and clear[-1] == ')':
            content = re.sub(clear, '', content, re.I | re.S)
        else:
            content = content.replace(clear, '')

    return content

