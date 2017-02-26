# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import codecs
import pymongo

class HttpproxymiddlewaretestPipeline(object):

    def __init__(self):
        self.file = codecs.open('a.json',mode='a+',encoding='utf-8')
    def process_item(self, item, spider):
        line = json.dumps(dict(item)) + '\n'
        self.file.writelines(line.decode('unicode_escape'))
        return item

"""
Scrapy Pipeline
    http://scrapy-chs.readthedocs.io/zh_CN/0.24/topics/item-pipeline.html

在爬取新闻的评论列表时,判断用户是否已经存在
    在生成 Request 的时候进行判重,在 pipeline 中进行持久化

在用户的评论页时,判断其中的新闻是否已经存在
SSDB 官网 文档 http://ssdb.io/ssdb-get-started.pdf
ssdb.zset('test','a') # 在表 test 中插入 'a'
ssdb.zget('test','b') # 在表中获取 'b' 如果存在返回 scores

>>> ssdb.zexists('zset_2', 'key2')
        True
>>> ssdb.zdel("zset_2", 'key1')
        True
>>> ssdb.zdel("zset_2", 'key_not_exist')
        False
>>> ssdb.zdel("zset_not_exist", 'key1')
        False
>>> ssdb.zclear('zset_1')
        7
>>> ssdb.zclear('zset_1')
        0
>>> ssdb.zlist('zset_ ', 'zset_z', 10)
        ['zset_1', 'zset_2']
>>> ssdb.zlist('zset_ ', '', 3)
        ['zset_1', 'zset_2']
>>> ssdb.multi_zdel('zset_1', 'a', 'b', 'c', 'd')
        4
>>> ssdb.multi_zdel('zset_1', 'a', 'b', 'c', 'd')
        0
>>> ssdb.multi_zdel('zset_2', 'key2_not_exist', 'key5_not_exist')
        0
"""

class SSDBPipeline(object):

    def __init__(self):
        self.host = 'localhost'
        self.port = 1234

    def open_spider(self, spider):
        #self.ssdb = SSDB(host=self.host,port=self.port)
        pass

    def process_item(self, item, spider):
        tabname = str(item.__class__.__name__)
        if tabname=='NewsItem':
            self.ssdb.zset(tabname,item['newsid'])
        elif tabname=='UserItem':
            self.ssdb.zset(tabname, item['userid'])
        elif tabname=='UrlItem':
            self.visitedUrl(item['url'])
            return None
        return item

class MongoPipeline(object):

    def __init__(self):
        self.mongo_uri = 'mongodb://localhost:27017'

    def open_spider(self,spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client['qq']


    def close_spider(self,spider):
        self.client.close()

    def process_item(self,item,spider):
        collection_name = str(item.__class__.__name__)
        self.db[collection_name].insert(dict(item))
        return item