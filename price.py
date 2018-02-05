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
COOKIES = LazyFW.read_file(LazyFW.ABS_PATH + '/cookies-5i5j.txt')


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
CREATE TABLE IF NOT EXISTS `price_urls` (
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
CREATE TABLE IF NOT EXISTS `prices` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `fang_id` varchar(100) NOT NULL DEFAULT '' COMMENT '房源ID',
  `xiaoqu_url` varchar(255) NOT NULL DEFAULT '' COMMENT '小区URL',
  `xiaoqu_name` varchar(100) NOT NULL DEFAULT '' COMMENT '小区名',
  `city` varchar(50) NOT NULL DEFAULT '' COMMENT '城市',
  `area` decimal(10,2) NOT NULL DEFAULT 0 COMMENT '面积',
  `price` decimal(10,2) NOT NULL DEFAULT 0 COMMENT '价格',
  `huxing` varchar(50) NOT NULL DEFAULT '' COMMENT '户型',
  `qianyue` varchar(50) NOT NULL DEFAULT '' COMMENT '签约时间',
  `zhuangxiu` varchar(50) NOT NULL DEFAULT '' COMMENT '装修类型',
  PRIMARY KEY (`id`),
  UNIQUE KEY `fang_id` (`fang_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
''')

    c.close()
    DB_CONN.commit()
    DB_CONN.close()


def url_insert(url):
    DB_CONN = get_conn()
    c = DB_CONN.cursor()
    try:
        c.execute(r'''INSERT INTO `price_urls` (`url`) VALUES(%s);''', (url,))
        result = True
    except mysql.IntegrityError:
        result = None
    except Exception as e:
        result = None

    c.close()
    DB_CONN.commit()
    DB_CONN.close()
    return result


def url_update(url, status):
    DB_CONN = get_conn()
    c = DB_CONN.cursor()
    try:
        c.execute(r'''UPDATE `price_urls` SET `status`=%s WHERE `url`=%s;''',
                  (status, url,))
        result = True
    except mysql.IntegrityError:
        result = None
    except Exception as e:
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
            r'''INSERT INTO `prices` (`fang_id`,`xiaoqu_url`,`xiaoqu_name`,`city`,`area`,`price`,`huxing`,`qianyue`,`zhuangxiu`) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);''',
            (info['fang_id'], info['xiaoqu_url'], info['xiaoqu_name'], info['city'], info['area'], info['price'],
             info['huxing'], info['qianyue'], info['zhuangxiu']))
        result = True
    except mysql.IntegrityError:
        c.execute(
            r'''UPDATE `prices` SET `xiaoqu_url`=%s,`xiaoqu_name`=%s,`city`=%s,`area`=%s,`price`=%s, `huxing`=%s, `qianyue`=%s, `zhuangxiu`=%s WHERE `fang_id`=%s;''',
            (info['xiaoqu_url'], info['xiaoqu_name'], info['city'], info['area'], info['price'], info['huxing'],
             info['qianyue'], info['zhuangxiu'],
             info['fang_id'],))
        result = True
    except mysql.OperationalError:
        result = None

    c.close()
    DB_INSERT.commit()
    DB_INSERT.close()
    return result


def addInfoExt(info):
    DB_INSERT = get_conn()
    c = DB_INSERT.cursor()
    try:
        c.execute(
            r'''INSERT INTO `prices` (`fang_id`,`city`,`area`,`zhuangxiu`,`qianyue`,`price`) VALUES(%s,%s,%s,%s,%s,%s);''',
            (info['fang_id'], info['city'], info['area'], info['zhuangxiu'], info['qianyue'], info['price']))
        result = True
    except mysql.IntegrityError:
        c.execute(
            r'''UPDATE `prices` SET `area`=%s,`city`=%s,`zhuangxiu`=%s,`qianyue`=%s,`price`=%s WHERE `fang_id`=%s;''',
            (info['area'], info['city'], info['zhuangxiu'], info['qianyue'], info['price'], info['fang_id'],))
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
            scan_html = html

            if re.search(r'''5i5j.com/xiaoqu/(\d+)\.html''', url) != None:
                d = pq(html)
                scan_html = d('div.cur-path-box').html()
                scan_html = r'%s <h3 class="erjtit">租房成交记录%s' % (
                    scan_html, LazyFW.mid(html, '<h3 class="erjtit">租房成交记录', '</div>'))

            elif re.search(r'''5i5j.com/xiaoqu/''', url) != None:
                d = pq(html)
                scan_html = d('div.pxMain .tiaoBox li').eq(0).html()
                scan_html = "%s %s" % (scan_html, d('div.pListBox div.lfBox').html())

            elif re.search(r'''5i5j.com/leased/(\d+)\.html''', url) != None:
                html = html.replace('/sold/', '/leased/')
                d = pq(html)

                fang_id = re.sub(r'^.+/(\d+)\.html(?:.+)?$', r'\1', url)
                xiaoqu_url = d('div.detail-main div.infomain a.infotit').attr('href')
                xiaoqu_name = d('div.detail-main div.infomain a.infotit').text()
                city = re.sub(r'^.+://([^\.]+)(.+)?$', r'\1', url)
                area = 0
                price = re.sub(r'[^\d]', r'', d('div.house-info p.cjinfo').eq(0).text())
                huxing = d('h1.house-tit').text().split(' ')[1]
                qianyue = d('div.house-info p.cjinfo').eq(1).text()

                # 添加信息
                data = {
                    "fang_id": fang_id,
                    "xiaoqu_url": xiaoqu_url,
                    "xiaoqu_name": xiaoqu_name,
                    "city": city,
                    "area": area,
                    "price": price,
                    "huxing": huxing,
                    "qianyue": qianyue,
                    "zhuangxiu": '--',
                }
                insert_s = addInfo(data)
                if insert_s:
                    LazyFW.log(
                        "Add Data: fang_id: %s, xiaoqu_name: %s, city: %s, price: %s, huxing: %s, qianyue: %s, area: %s, xiaoqu_url: %s" % (
                            data['fang_id'], data['xiaoqu_name'], data['city'], data['price'], data['huxing'],
                            data['qianyue'], data['area'], data['xiaoqu_url'],))

                # 收集当前小区的其他房源和面积
                for ele in d('ul.yizucontent').items():
                    fang_url = ele.find('li').eq(0).find('a').attr('href')
                    fang_id = re.sub(r'^.+/(\d+)\.html(?:.+)?$', r'\1', fang_url)
                    area = re.sub(r'[^\d]', r'', ele.find('li').eq(1).text())
                    city = re.sub(r'^.+://([^\.]+)(.+)?$', r'\1', fang_url)
                    zhuangxiu = ele.find('li').eq(2).text()
                    qianyue = ele.find('li').eq(3).text()
                    price = re.sub(r'[^\d]', r'', ele.find('li').eq(4).text())

                    ele_data = {
                        "fang_id": fang_id,
                        "area": area,
                        "city": city,
                        "zhuangxiu": zhuangxiu,
                        "qianyue": qianyue,
                        "price": price,
                    }
                    insert_s = addInfoExt(ele_data)
                    if insert_s:
                        LazyFW.log(
                            "Update Data: fang_id: %s, area: %s, city: %s, zhuangxiu: %s, qianyue: %s, price: %s" % (
                                ele_data['fang_id'], ele_data['area'], ele_data['city'], ele_data['zhuangxiu'],
                                ele_data['qianyue'],
                                ele_data['price'],))

            # 收集网址
            urls = LazyFW.get_urls(scan_html)

            num = 0

            for u in urls:
                if re.search(r'''5i5j.com/(xiaoqu|leased)/''', u) != None:
                    # 需屏蔽规则
                    if re.search(r'''5i5j.com/(xiaoqu|leased)/(?:.+/)?(o1|o5|r2)/''',
                                 u) != None:
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
        raise
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
    fetch(r'https://sh.5i5j.com/xiaoqu/')
    fetch(r'https://bj.5i5j.com/xiaoqu/')
    fetch(r'https://hz.5i5j.com/xiaoqu/')
    fetch(r'https://sz.5i5j.com/xiaoqu/')
    fetch(r'https://wh.5i5j.com/xiaoqu/')
    fetch(r'https://wx.5i5j.com/xiaoqu/')
    fetch(r'https://cd.5i5j.com/xiaoqu/')
    fetch(r'https://cs.5i5j.com/xiaoqu/')
    fetch(r'https://nj.5i5j.com/xiaoqu/')
    fetch(r'https://nn.5i5j.com/xiaoqu/')
    fetch(r'https://tj.5i5j.com/xiaoqu/')
    fetch(r'https://ty.5i5j.com/xiaoqu/')
    fetch(r'https://zz.5i5j.com/xiaoqu/')

    # 再次查询出数据
    DB_CONN = get_conn()
    LazyFW.log(r'''SELECT `url` FROM `price_urls` where `status`=0;''')
    c = DB_CONN.cursor()
    c.execute(r'''SELECT `url` FROM `price_urls` where `status`=0;''')
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
