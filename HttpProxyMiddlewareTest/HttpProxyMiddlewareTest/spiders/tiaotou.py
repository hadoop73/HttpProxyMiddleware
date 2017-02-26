# coding:utf-8

import scrapy
from ..items import NewsItem,ReadItem
from scrapy.http import Request
import scrapy
from ..items import NewsItem,ReadItem
from scrapy.http import Request

import re
import json

import logging

logger = logging.getLogger("test spider")
logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='myapp.log',
                    filemode='w')
"""
任务: 去重,从用户页爬取新闻链接,新闻内容去除 原标题
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
ssdb.zremove('test','a') # 删除 'a'
 SSDB  Python https://github.com/wrongwaycn/ssdb-py
 修改端口 ssdb.conf 文件为 1234
 启动: ./ssdb-server ssdb.conf
 后台启动:./ssdb-server -d ssdb.conf
 # stop ssdb-server
$ kill `cat ./var/ssdb.pid`

去重: https://www.zhihu.com/question/30329757
布隆过滤:http://blog.csdn.net/hguisu/article/details/7866173
from pybloom import BloomFilter

添加的用户与新闻的关系,没有对用户进行判断,因为用户可能为 0
新闻存在有下划线,是错误的

去重思路:在SSDB中保存四张表,一张表:已经爬取的 userid,还没爬取的 userid  通过一个时间阀值调整,再次爬取失败网页的url
爬下来的的 userid,从还没有爬取表中删除;同时 保存在 userid 的 bloom filter 中
已经爬取的新闻,还没爬取的新闻
已经爬取的新闻,从还没爬取的新闻表中删除;同时保存在 newsid 的 bloom filter 中

需要一个保存爬取过用户,新闻的数据库,以免重启爬虫时不再重新爬
另外,每隔一段时间,从爬取失败的数据库中新建 Request ,重新爬取
"""
from pybloom import BloomFilter
from ..ssdbOp import *


class TSpider(scrapy.spiders.Spider):

    name = "tt"
    allowed_domains = ["toutiao.com"]
    website_possible_httpstatus_list = [403]
    handle_httpstatus_list = [403]
    start_urls = (
        'http://www.toutiao.com/api/pc/feed/?category=news_society&utm_source=toutiao'+\
        '&%20widen=1&max_behot_time=1487415031&max_behot_time_tmp'+\
        '=1487415031&as=A1E5384A685350E%20&cp=58A83325100EEE1',
    )
    #start_urls = ['http://www.toutiao.com/a6388306099012190465/']
    sufixUrl = "http://www.toutiao.com/api/pc/feed/?category=news_society&utm_source=toutiao"+\
        "&%20widen=1&max_behot_time="

    bnews = BloomFilter(capacity=1000000, error_rate=0.00001)
    buser = BloomFilter(capacity=100000, error_rate=0.00001)
    burl = BloomFilter(capacity=1000000, error_rate=0.00001)

    host = 'localhost'
    port = 1234


    def parse(self,response):
        try:
            if response.body == "banned":
                req = response.request
                req.meta["change_proxy"] = True
                yield req
            else:
                current_url = str(response.url)
                visited(sdb=self.sdb,data=current_url)
                data = response.body_as_unicode()
                data = json.loads(data)
                #print data
                max_behot_time = data['next']['max_behot_time']
                nextimeUrl = self.sufixUrl + str(max_behot_time)
                data = data['data']
                for d in data:
                    print d
                    try:
                        if d['tag']=='news_society':
                            a = "http://www.toutiao.com/a" + d['group_id'] + "/"
                            print "="*60,a
                            req = Request(a,self.parseArticle)
                            yield req
                    except:
                        pass
                req = Request(nextimeUrl, self.parse)       # 爬取最新时间的科技新闻
                yield req
        except:
            pass

    """
    http://www.toutiao.com/api/pc/feed/?category=news_tech&utm_source=toutiao&
    widen=1&max_behot_time=1487415031&max_behot_time_tmp=1487415031&as=A1E5384A685350E
    &cp=58A83325100EEE1
    """
    def parseArticle(self, response):

            if response.body == "banned":
                req = response.request
                req.meta["change_proxy"] = True
                yield req
            else:
                # 1) 爬取文章
                item = NewsItem()
                current_url = str(response.url)  # 爬去时请求的 url
                visited(self.sdb, names='visitedUrl', data=current_url)  # 设置已经访问过了的 URL
                unicode_body = response.body_as_unicode()  # 返回的 html unicode 编码
                titleXpath = "//div[@id='article-main']/h1[@class='article-title']/text()"  # 标题
                title = response.xpath(titleXpath).extract()[0].encode('utf-8')
                print title
                articleXpath = "//div[@class='article-content']/div/p/text()"
                contentList = []
                for p in response.xpath(articleXpath).extract():
                    contentList.append(p.encode('utf-8'))
                content = "\n".join(contentList)  # 新闻内容

                sourceXpath = "//div[@class='articleInfo']/span[@class='src']/text()"
                resource = response.xpath(sourceXpath).extract()[0].encode('utf-8')  # 新闻来源

                timeXpath = "//div[@class='articleInfo']/span[@class='time']/text()"  # 时间
                t = response.xpath(timeXpath).extract()[0].encode('utf-8')

                p = re.compile('(?<=numCutByComma\(.)\d+')
                m = re.search(p,unicode_body)
                hotNums = m.group(0)

                p = re.compile("(?<=group_id: ')\d+")
                m = re.search(p,unicode_body)
                group_id = m.group(0)

                p = re.compile("(?<=item_id: ')\d+")
                m = re.search(p, unicode_body)
                item_id = m.group(0)

                item['url'] = current_url
                item['hotNum'] = hotNums
                item['title'] = title

                item['time'] = t
                item['resource'] = resource
                item['newsid'] = current_url.split('/')[-2] if len(current_url.split('/')[-1])<2 else current_url.split('/')[-1]
                item['content'] = content
                yield item

                # 爬取文章评论
                url = "http://www.toutiao.com/api/comment/list/?group_id="+ \
                              group_id + "&item_id=" + item_id + "&offset={}&count=10"
                commentUrl = url.format("5")
                req = Request(commentUrl,self.parseComments)
                req.meta['offset'] = 5
                req.meta['url'] = url
                req.meta['newsid'] = item['newsid']
                yield req


    # 获取文章评论
    def parseComments(self,response):
            """
            {"message": "success", "data": {"has_more": true, "total": 381, "comments": []}
            :param response:
            :return:
            """
            data = response.body_as_unicode()
            data = json.loads(data)
            if data['data']['has_more']:
                comments = data['data']['comments']
                for d in comments:
                    item = ReadItem()
                    item['create_time'] = d['create_time']      # 创建时间
                    item['digg_count'] = d['digg_count']        # 点赞数量
                    item['reply_count'] = d['reply_count']      # 回复数量
                    item['user_id'] = d['user']['user_id']
                    item['news_id'] = response.meta['newsid']
                    yield item

                # 继续爬取剩余评论
                url = response.meta['url']
                commentUrl = url.format(str(response.meta['offset']+10))
                req = Request(commentUrl,self.parseComments)
                req.meta['offset'] = response.meta['offset']+10
                req.meta['newsid'] = response.meta['newsid']
                req.meta['url'] = response.meta['url']
                yield req






























