# coding:utf-8

import scrapy
from ..items import NewsItem,ReadItem,NewsTotalItem
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
#from ssdb import SSDB
from ..ssdbOp import *
from datetime import datetime, date, timedelta
import random
import time

"""
1) 按照日期爬取每一天的新闻
    按照日期,每次增加一天,从 2016-01-01 开始
    网页 URL 格式:http://roll.news.qq.com/index.htm?site=news&mod=1&date=2016-01-01&cata=
2) 爬取每一天的每个 page 的新闻
    网页 URL 格式:
    http://roll.news.qq.com/interface/roll.php?0.5693016620783735&cata=&site=news&date=2016-01-01&page=1&mode=1&of=json
    其中 roll.php?{随机数}&cata=&site=news&date={日期}&page={页数}&mode=1&of=json
    返回数据格式:{"response":{"code":"0","msg":"Success","dext":""},
    "data":{"count":6,"page":1,"article_info":"{新闻}"}}
    需要判断 page <= count 是否全部 page 都已经返回
3) 爬取每篇新闻
    从 2 中解析每篇新闻
4) 爬取每篇新闻的评论
    热评论 url : http://coral.qq.com/article/1283962322/hotcomment?reqnum=10&callback=myHotcommentList&_=1487934911377
    普通评论:http://coral.qq.com/article/1358447741/comment?commentid=0&reqnum=10&tag=&callback=mainComment&_=1487941399669
            http://coral.qq.com/article/{newid}/comment?commentid=0&reqnum=10&tag=&callback=mainComment&_={timestamp}
    返回数据格式:{"data":{"commentid":[{"id":123456,"userid":"1111"
                "time":145236,timeDifference:"2016年04月02日 09:13:18",
                "up":"4","rep":"1",
                "userinfo":{"userid":"1111"}}],
                reqnum:20, # 请求返回数目
                retnum:2,  # 实际返回个数,retnum < reqnum 则全部返回
                "first":"123456","last":"1234567","total":65}}

日期增加一天
>>> from datetime import datetime
>>> a = datetime(2012, 9, 23)
>>> print(a + timedelta(days=10))
2012-10-03 00:00:00

日期转字符串
>>> z
datetime.datetime(2012, 9, 23, 21, 37, 4, 177393)
>>> nice_z = datetime.strftime(z, '%A %B %d, %Y')
>>> nice_z
'Sunday September 23, 2012'

a = datetime(2012, 9, 23)
z = (a + timedelta(days=1))
nice_z = datetime.strftime(z, '%Y-%m-%d')
print nice_z
2012-09-24
"""

class QSpider(scrapy.spiders.Spider):

    name = "qq"
    allowed_domains = ["qq.com"]
    website_possible_httpstatus_list = [0]
    handle_httpstatus_list = [0]
    #start_urls = ["http://news.qq.com/a/20160201/012106.htm"]
    start_urls =["http://roll.news.qq.com/interface/roll.php?" + str(random.random()) + "&cata=&site=news&date=2016-02-01&page=1&mode=1&of=json"]
    day = datetime(2016,2, 1)
    urlStr = "http://roll.news.qq.com/interface/roll.php?{rand}&cata=&site=news&date={time}&page={page}&mode=1&of=json"

    host = 'localhost'
    port = 1234

    def getNewsUrl(self,article_info):
        p = re.compile('http:.*?htm')
        return p.findall(article_info)


    #  获取每一天的新闻列表,能够获取新闻列表,然后继续生成新闻页面
    def parse(self,response):
        """
        返回数据格式:{"response":{"code":"0","msg":"Success","dext":""},
    "data":{"count":6,"page":1,"article_info":"{新闻}"}}
    需要判断 page <= count 是否全部 page 都已经返回
        :param response:
        :return:
        """
        if response.status in [403]:
            req = response.request
            print "+"*90,response.body
            req.meta["change_proxy"] = True
            yield req
        else:
            try:
                data = str(response.body)
                jsn = json.loads(data,encoding='gbk')
                page = int(jsn['data']['page'])
                count = int(jsn['data']['count'])

                if page >= count:
                    self.day = self.day + timedelta(days=1)
                    page = 1
                if self.day < datetime(2016,8, 1):    #  先爬取每天的列表，再去爬取列表中的新闻
                    rand = random.random()
                    page += 1
                    dayUrl = self.urlStr.format(rand=str(rand),
                                                time=datetime.strftime(self.day, '%Y-%m-%d'),
                                                page=str(page))
                    req = Request(dayUrl, self.parse, headers={"Referer": "http://roll.news.qq.com/"})
                    yield req  # 继续爬取每天的每页新闻

                article_info = jsn['data']['article_info']
                urls = self.getNewsUrl(article_info)  # 获取所有的新闻 url
                if urls != None:
                    for u in urls:
                        if "tuhua" not in u:
                            rq = Request(u, self.parseNews,headers={"Referer":"http://roll.news.qq.com/"})
                            yield rq

            except:
                pass


    # 解析每个页面的新闻数据,包括 title,content,time,url,newsid,并生成评论页 url
    def parseNews(self,response):
        if response.status in [403]:
            req = response.request
            print "+" * 90, response.body
            req.meta["change_proxy"] = True
            yield req
        elif response.status not in [404]:
            try:
                current_url = str(response.url)

                titleXpath = "//div[@id='C-Main-Article-QQ']/div[@class='hd']/h1/text()"
                title = response.xpath(titleXpath).extract()[0].encode('utf-8')
                contentXpath = "//div[@id='Cnt-Main-Article-QQ']/p/text()"
                content = ""
                for text in response.xpath(contentXpath).extract(): #[0].encode('utf-8')
                    content += " "+text.encode('utf-8')

                timeXpath = "//span[@class='article-time']/text()"
                newstime = response.xpath(timeXpath).extract()[0].encode('utf-8')
                resourceXpath = "//div[@class='ll']/span[@class='color-a-1']/a/text()"
                resource = response.xpath(resourceXpath).extract()[0].encode('utf-8')


                s = str(response.body)
                s = s.replace(' ','')
                p = re.compile("(?<=cmt_id=)\d+")
                m = re.search(p,s)
                newsid = m.group(0)

                item = NewsItem()
                item['url'] = current_url
                item['title'] = title
                item['time'] = newstime
                item['resource'] = resource
                item['newsid'] = newsid
                item['content'] = content
                yield item

                a = int(round(time.time()))
                s = str(a) + str(random.randint(100, 900))
                commentUrl = "http://coral.qq.com/article/{newsid}/comment?commentid=0&reqnum=10&tag=&callback=mainComment&_={timestamp}".format(newsid=newsid,
                                                                                                                                                timestamp=s)
                req = Request(commentUrl,self.parseComments,headers={"Referer":"http://roll.news.qq.com/"})
                req.meta['newsid'] = newsid
                yield req
            except:
                pass


    # 解析评论页,获得 newsid,userid,time,replynum,up
    def parseComments(self,response):
        """
           普通评论:http://coral.qq.com/article/1358447741/comment?commentid=0&reqnum=10&tag=&callback=mainComment&_=1487 9413 9966 9
            http://coral.qq.com/article/{newid}/comment?commentid=0&reqnum=10&tag=&callback=mainComment&_={timestamp}
    返回数据格式:{"data":{"commentid":[{"id":123456,"userid":"1111"
                "time":145236,timeDifference:"2016年04月02日 09:13:18",
                "up":"4","rep":"1",
                "userinfo":{"userid":"1111"}}],
                reqnum:20, # 请求返回数目
                retnum:2,  # 实际返回个数,retnum < reqnum 则全部返回
                "first":"123456","last":"1234567","total":65}} 从 last 开始
        :param response:
        :return:
        """
        if response.status in [403]:
            req = response.request
            req.meta["change_proxy"] = True
            yield req
        elif response.status not in [404]:
            try:
                data = str(response.body)
                p = re.compile("\{.*\}")
                m = re.search(p,data)
                data = m.group(0)
                jsn = json.loads(data)
                item = NewsTotalItem()
                item['newsid'] = response.meta['newsid']
                item['total'] = jsn['data']['total']
                yield item
                if ("errorMsg" not in jsn) and (jsn['data']['total']<=10000):   # 只爬取评论量少于 1w 的文章评论
                    retnum = jsn['data']['retnum']
                    reqnum = jsn['data']['reqnum']
                    if retnum > 0:
                        for m in jsn['data']['commentid']:
                            item = ReadItem()
                            item['create_time'] = m['time']
                            item['newsid'] = response.meta['newsid']
                            item['userid'] = m['userid']
                            item['digg_count'] = m['up']
                            item['reply_count'] = m['rep']
                            yield item
                    if (retnum==reqnum) and (jsn['data']['total'])>0:
                        last = jsn['data']['last']
                        a = int(round(time.time()))

                        s = str(a) + str(random.randint(100, 900))
                        commentUrl = "http://coral.qq.com/article/{newsid}/comment?commentid={last}&reqnum=10&tag=&callback=mainComment&_={timestamp}".format(
                                        newsid=str(response.meta['newsid']),last=last,timestamp=s)
                        req = Request(commentUrl,self.parseComments,headers={"Referer":"http://roll.news.qq.com/"})
                        req.meta['newsid'] = str(response.meta['newsid'])
                        yield req
            except:
                pass



