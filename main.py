#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Lukin'

import re
import requests
import LazyFW
import MySQLdb as mysql
from urllib.parse import urlparse
from pyquery import PyQuery as pq
from multiprocessing import Process, Queue

# 下载线程数
THREAD_MAX = int(LazyFW.config('Collect', 'THREAD_MAX'))
# 超时
TIMEOUT = int(LazyFW.config('Collect', 'TIMEOUT'))
# USER_AGENT
USER_AGENT = str(LazyFW.config('Collect', 'USER_AGENT'))

# DB config
DB_HOST = str(LazyFW.config('DB', 'HOST'))
DB_USER = str(LazyFW.config('DB', 'USER'))
DB_PASS = str(LazyFW.config('DB', 'PASS'))
DB_NAME = str(LazyFW.config('DB', 'NAME'))
DB_PORT = int(LazyFW.config('DB', 'PORT'))
FTQUEUE = Queue()

# 浏览器里保存的cookies
COOKIES = LazyFW.read_file(LazyFW.ABS_PATH + '/cookies.txt')


def get_conn():
    global DB_HOST, DB_USER, DB_PASS, DB_NAME, DB_PORT
    return mysql.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME, port=DB_PORT, charset='utf8')


def create_db():
    '''
    创建数据库
    '''
    DB_CONN = get_conn()
    c = DB_CONN.cursor()

    # 创建表
    c.execute(
        r'''
CREATE TABLE IF NOT EXISTS `urls` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `url` varchar(255) NOT NULL DEFAULT '',
  `status` tinyint(1) NOT NULL DEFAULT '0',
  `uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `url` (`url`),
  KEY `status` (`status`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
''')

    c.execute(
        r'''
CREATE TABLE IF NOT EXISTS `datas` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `cid` int(11) NOT NULL DEFAULT '0' COMMENT '小区ID',
  `name` varchar(50) NOT NULL DEFAULT '' COMMENT '小区名',
  `city` varchar(50) NOT NULL DEFAULT '' COMMENT '城市',
  `xingzhengqu` varchar(50) NOT NULL DEFAULT '' COMMENT '行政区',
  `bankuai` varchar(50) NOT NULL DEFAULT '' COMMENT '所属板块',
  `address` varchar(255) NOT NULL DEFAULT '' COMMENT '地址',
  `jingweidu` varchar(255) NOT NULL DEFAULT '' COMMENT '经纬度',
  PRIMARY KEY (`id`),
  UNIQUE KEY `cid` (`cid`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
''')

    c.close()
    DB_CONN.commit()
    DB_CONN.close()


def url_insert(url):
    DB_CONN = get_conn()
    c = DB_CONN.cursor()
    try:
        c.execute(r'''INSERT INTO `urls` (`url`) VALUES(%s);''', (url,))
        result = True
    except mysql.IntegrityError:
        result = None

    c.close()
    DB_CONN.commit()
    DB_CONN.close()
    return result


def url_update(url, status):
    DB_CONN = get_conn()
    c = DB_CONN.cursor()
    try:
        c.execute(r'''UPDATE `urls` SET `status`=%s WHERE `url`=%s;''',
                  (status, url,))
        result = True
    except mysql.IntegrityError:
        result = None

    c.close()
    DB_CONN.commit()
    DB_CONN.close()
    return result


def addInfo(info):
    DB_INSERT = get_conn()
    c = DB_INSERT.cursor()
    try:
        c.execute(
            r'''INSERT INTO `datas` (`cid`,`name`,`city`,`xingzhengqu`,`bankuai`,`address`,`jingweidu`) VALUES(%s,%s,%s,%s,%s,%s,%s);''',
            (info['cid'], info['name'], info['city'], info['xingzhengqu'], info['bankuai'], info['address'],
             info['jingweidu']))
        result = True
    except mysql.IntegrityError:
        c.execute(
            r'''UPDATE `datas` SET `name`=%s,`city`=%s,`xingzhengqu`=%s,`bankuai`=%s,`address`=%s, `jingweidu`=%s WHERE `cid`=%s;''',
            (info['name'], info['city'], info['xingzhengqu'], info['bankuai'], info['address'], info['jingweidu'],
             info['cid'],))
        result = True
    except mysql.OperationalError:
        result = None

    c.close()
    DB_INSERT.commit()
    DB_INSERT.close()
    return result


def fetch(url):
    global FTQUEUE, USER_AGENT
    LazyFW.log("Fetch URL: %s" % (url))
    try:
        urls = urlparse(url)
        http = requests.Session()
        r = http.get(url, timeout=TIMEOUT, headers={
            'User-Agent': USER_AGENT,
            'Referer': 'http://%s' % (urls.hostname),
            'Cookie': COOKIES
        })

        if r.status_code == 200 or r.status_code == 304:
            html = r.text
            html = LazyFW.format_url(url, html)
            d = pq(html)
            scan_html = d('.div-border div.items').eq(0)
            scan_html = "%s %s" % (scan_html, d('div.maincontent').html())

            communitys = d('#list-content .li-itemmod').items()
            for community in communitys:
                cid = re.sub(r'[^\d]', '', community.find('h3 a').attr('href'))
                name = community.find('h3').text()
                city = re.sub(r'^.+\/\/([^\.]+).+$', r'\1', community.find('h3 a').attr('href'))
                xingzhengqu = community.find('address').text().split('］')[0].strip('［').split('-')[0]
                bankuai = community.find('address').text().split('］')[0].strip('［').split('-')[1]
                address = community.find('address').text().split('］')[1].strip()
                jingweidu = re.sub(r'^.+#l1=([^&]+)&l2=([^&]+).+$', r'\1,\2', community.find('.bot-tag a').eq(1).attr('href'))

                # 添加信息
                data = {
                    "cid": cid,
                    "name": name,
                    "city": city,
                    "xingzhengqu": xingzhengqu,
                    "bankuai": bankuai,
                    "address": address,
                    "jingweidu": jingweidu,
                }

                insert_s = addInfo(data)
                if insert_s:
                    LazyFW.log(
                        "Add Data: cid: %s, name: %s, city: %s, xingzhengqu: %s, bankuai: %s, address: %s, jingweidu: %s" % (
                            data['cid'], data['name'], data['city'], data['xingzhengqu'], data['bankuai'],
                            data['address'], data['jingweidu'],))

            # 收集网址
            urls = LazyFW.get_urls(scan_html)

            num = 0

            for u in urls:
                if re.search(r'''community/''', u) != None:
                    # 需屏蔽规则
                    if re.search(r'''community/(?:.+/)?(o\d|props|round|trends|photos2|view|jiedu)/''', u) != None:
                        pass
                    else:
                        insert_r = url_insert(u)
                        if insert_r:
                            FTQUEUE.put_nowait(u)
                            num += 1

            # 输出添加URL信息
            if num > 0:
                LazyFW.log("Add URL: %d %s" % (num, url))

            # 设置网址状态
            url_update(url, 1)

    except Exception as e:
        LazyFW.log("Error: %s" % (e,))
        url_update(url, 2)
        return False

    return True


def worker(queue):
    '''
    线程worker
    :param ftype:
    :param queue:
    :return:
    '''

    while True:
        # 队列为空，停止
        if queue.empty():
            LazyFW.log('''TaskEmpty: break''')
            break

        try:
            task = queue.get_nowait()
            # LazyFW.log('''TaskGet: %s''' % (task, ))
            fetch(task)

        except Exception as e:
            LazyFW.log('''TaskError(%s)''' % (e,))


def main():
    global FTQUEUE
    create_db()
    fetch(r'https://shanghai.anjuke.com/community/?from=navigation')
    fetch(r'https://beijing.anjuke.com/community/?from=navigation')
    fetch(r'https://guangzhou.anjuke.com/community/?from=navigation')
    fetch(r'https://shenzhen.anjuke.com/community/?from=navigation')

    # 再次查询出数据
    DB_CONN = get_conn()
    LazyFW.log(r'''SELECT `url` FROM `urls` where `status`=0;''')
    c = DB_CONN.cursor()
    c.execute(r'''SELECT `url` FROM `urls` where `status`=0;''')
    for row in c.fetchall():
        FTQUEUE.put_nowait(row[0])

    c.close()
    DB_CONN.commit()
    DB_CONN.close()

    workers = []
    for i in range(THREAD_MAX):
        p = Process(target=worker, args=(FTQUEUE,))
        p.daemon = True
        p.start()
        workers.append(p)

    for p in workers:
        p.join()


if __name__ == '__main__':
    main()
