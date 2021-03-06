# coding:utf-8

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
 SSDB  Python https://github.com/wrongwaycn/ssdb-py                                       
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

class TestSpider(scrapy.spiders.Spider):

    name = "test"
    allowed_domains = ["163.com",'netease.com']
    website_possible_httpstatus_list = [403]
    handle_httpstatus_list = [403]
    start_urls = (
        'http://news.163.com/17/0218/15/CDIL6HKO000187V5.html',
        'http://money.163.com/17/0218/06/CDHNN1AE002580S6.html'
    )



    bnews = BloomFilter(capacity=1000000, error_rate=0.00001)
    buser = BloomFilter(capacity=100000, error_rate=0.00001)
    burl = BloomFilter(capacity=1000000, error_rate=0.00001)

    host = 'localhost'
    port = 1234


    def parse(self, response):
        """  //div[@class='post_tie_top']/a[@class='post_cnum_tie js-tielink js-tiejoincount']/@href
         1) 从一篇文章开始,爬取文章内容    文章 url 去重
         2) 获取文章的评论 url
         3) 从评论的页面获取所有的用户评论
         4) 继续爬取这些用户的页面        用户 userid 去重
         5) 从用户页面获取财经新闻
        :param response:
        :return:
        """
        try:
            if response.body == "banned":
                req = response.request
                req.meta["change_proxy"] = True
                yield req
            else:
                # 1) 爬取文章
                item = NewsItem()
                current_url = str(response.url)  # 爬去时请求的 url
                visited(self.sdb,names='visitedUrl',data=current_url)    # 设置已经访问过了的 URL
                unicode_body = response.body_as_unicode()  # 返回的 html unicode 编码
                titleXpath = "//div[@id='epContentLeft']/h1/text()"  # 标题
                title = response.xpath(titleXpath).extract()[0].encode('utf-8')

                timeXpath = "//div[@class='post_time_source']/text()"  # 时间
                t = response.xpath(timeXpath).extract()[0].encode('utf-8')

                p = re.compile('\d.+\d')
                m = re.search(p, t)
                newstime = m.group(0)
                # logger.info(str(response.body))
                hotNumsPath = "//div[@class='post_tie_top']/a/text()"
                hotNums = response.xpath(hotNumsPath).extract()[0]

                sourceXpath = "//div[@class='post_time_source']/a[@id='ne_article_source']/text()"
                resource = response.xpath(sourceXpath).extract()[0].encode('utf-8')  # 新闻来源

                contentXpath = "//div[@class='post_body']/div[@id='endText']/p/text()"  # 内容
                contentList = []
                for p in response.xpath(contentXpath).extract():
                    contentList.append(p.encode('utf-8'))
                content = "\n".join(contentList)  # 新闻内容

                item['url'] = current_url
                item['hotNum'] = hotNums
                item['title'] = title
                item['time'] = newstime
                item['resource'] = resource
                item['newsid'] = current_url.split('/')[-1].split('.')[0]
                item['content'] = content
                # yield item

                #commentXpath = "//div[@class='post_comment_joincount']/a[@class='js-tiejoincount js-tielink']/@href"
                #print response.xpath(commentXpath).extract()[0].encode('utf-8')

                p = re.compile('http://comment\..*?html')
                m = re.search(p, unicode_body)
                # 2) 爬取文章评论页面 http://comment.money.163.com/money_bbs/CCQIF2SJ002580S6.html
                commentUrl = m.group(0)
                # 会从评论页的 tiepage.js 中得到爬取数据的 url
                req = Request(commentUrl, callback=self.parse_tiepage)
                req.meta['newsid'] = current_url.split('/')[-1].split('.')[0]  # CCQIF2SJ002580S6
                visited(self.sdb, names='news', data=req.meta['newsid'])  # 设置已经访问过了的 newsid
                p = re.compile('(?<=//).*?(?=/)')
                m = re.search(p, current_url)
                req.meta['localhost'] = m.group(0)

                # print m.group(0)
                print commentUrl
                yield req
        except:
            pass


    def parse_tiepage(self, response):
        """
        #解析 tiepage.js 获得数据的url
        #:param response:
        #:return:
        """
        if response.body == "banned":
            req = response.request
            req.meta["change_proxy"] = True
            yield req
        else:
            body = response.body                                                # 返回的 html
            s = str(body)
            p = re.compile('http:.+tiepage\.js')                                # 用于测试获得文章的评论情况
            try:
                m = re.search(p, s)
                a = m.group(0)
                req = Request(a, callback=self.parse_comment)
                req.meta['newsid'] = response.meta['newsid']
                req.meta['offset'] = 0
                req.meta['localhost'] = response.meta['localhost']
                yield req
            except:
                logger.error(str(response.url))
                pass


            # 处理 tiepage.js,获得评论数据

    # 抓取评论页 URL
    def parse_comment(self, response):
            if response.body == "banned":
                req = response.request
                req.meta["change_proxy"] = True
                yield req
            else:
                s = str(response.body)
                pa = re.compile('\s')
                s = pa.sub(r'', s)
                try:
                    p = re.compile('(?<=productKey=\")\w+')
                    m = re.search(p, s)
                    a = m.group(0)
                    newsid = response.meta['newsid']

                    n = "http://comment." + response.meta['localhost'] + "/api/v1/products/" + a + \
                        "/threads/" + newsid + "/comments/" + "newList" + \
                        "?offset=" + str(response.meta['offset']) + \
                        "&limit=30&showLevelThreshold=72&headLimit=1&tailLimit=2&callback=getData&ibc=newspc"
                    print n

                    req = Request(n, callback=self.parse_user)
                    req.meta['newsid'] = newsid
                    req.meta['offset'] = 0
                    req.meta['productKey'] = a
                    req.meta['localhost'] = response.meta['localhost']
                    yield req

                except:
                    logger.error(str(response.url))
                    pass

                # print response.xpath(uxpath).extract()
                """"""
                # for userUrl in response.xpath(uxpath).extract():
                #    f.writelines(userUrl.encode('utf-8'))
                """"""
                # 提取 userId,如果没有访问过继续访问
                """http://tie.163.com/reply/myaction.jsp?action=reply&username=jOTcjMuY29t&userId=59355993&f=gentienickname
                """
                # 获取用户评论文章的链接,判断是否

    # 抓取评论页用户,最后一页 newListSize:0  parse_user,通过 userid 访问用户页
    def parse_user(self, response):
        if response.body == "banned":
            req = response.request
            req.meta["change_proxy"] = True
            yield req
        else:
            # 获取新闻评论页的用户评论,以及用户信息
            current_url = str(response.url)  # 爬去时请求的 url
            s = str(response.body)
            pa = re.compile('\s')
            s = pa.sub(r'', s)
            """
             s = getData({json 数据格式})
            """
            try:
                p = re.compile('\{.*\}(?=\))')
                jsn = re.search(p, s)
                comments = jsn.group(0)
                a = json.loads(comments)
                cms = a['comments']
                if len(a['comments']) != 0:
                    # 没有数据返回 0
                    """
                        http://comment.money.163.com/api/v1/products/a2869674571f77b5a0867c3d71db5856/threads/
                        CCUQ70TP002580S6/comments/newList?offset=60&limit=30&showLevelThreshold=72&
                        headLimit=1&tailLimit=2&callback=getData&ibc=newspc
                    """
                    for com in cms:
                        if cms[com]['user']['userId'] != 0:
                            itme = ReadItem()
                            itme['time'] = cms[com]['createTime'][:10] + " " + cms[com]['createTime'][10:]  # 浏览时间
                            # print cms[com]['vote']  # 点赞数
                            itme['userid'] = cms[com]['user']['userId']  # userId
                            itme['newsid'] = response.meta['newsid']  # 存储评论了新闻的用户
                            yield itme

                            if cms[com]['user']['userId'] not in self.buser:  # 判断用户是否已经爬取过,不重复则访问
                                userUrl = "http://tie.163.com/reply/myaction.jsp?action=reply&username=" + \
                                          str(cms[com]['user']['id']) + \
                                          "&userId=" + str(itme['userid']) + "&f=gentienickname"  # 用户主页 URL
                                rq = Request(userUrl, self.parse_user_comments)
                                rq.meta['user'] = cms[com]['user']['id']
                                visited(self.sdb, names='user', data=cms[com]['user']['id'])  # 设置已经访问过了的 userid
                                yield rq  # 爬取用户页
                            else:  # 此时 userid 在 buser 中已经出现过,需要在 ssdb 中再判断一次
                                if not isExists(self.sdb, names='user', data=cms[com]['user']['userId']):  # 在 ssdb 中没出现过
                                    userUrl = "http://tie.163.com/reply/myaction.jsp?action=reply&username=" + \
                                              str(cms[com]['user']['id']) + \
                                              "&userId=" + str(itme['userid']) + "&f=gentienickname"  # 用户主页 URL
                                    rq = Request(userUrl, self.parse_user_comments)
                                    rq.meta['user'] = cms[com]['user']['id']
                                    yield rq


                                    # 同时继续爬取新闻剩余的用户数据
                    usUrl = 'http://comment.' + response.meta['localhost'] +'/api/v1/products/' + str(response.meta['productKey']) + \
                            '/threads/' + str(response.meta['newsid']) + '/comments/newList?offset=' + \
                            str(int(response.meta['offset']) + 30) + \
                            '&limit=30&showLevelThreshold=72&headLimit=1&tailLimit=2&callback=getData&ibc=newspc'
                    req = Request(usUrl, self.parse_user)
                    req.meta['productKey'] = response.meta['productKey']
                    req.meta['newsid'] = response.meta['newsid']
                    req.meta['offset'] = int(response.meta['offset']) + 30
                    req.meta['localhost'] = response.meta['localhost']
                    yield req  # 继续爬取剩余的评论用户数据
                    # print cms[com]['user']['id']  # id 用于访问用户页
                    # 继续爬取用户的信息,包括用户的评论新闻
                    # print "============="
            except:
                logger.error(str(response.url))
                pass


    # feed.js 获得用户的 productsKey
    def parse_user_comments(self, response):
        try:
            if response.body == "banned":
                req = response.request
                req.meta["change_proxy"] = True
                yield req
            else:
                body = response.body                                        # 返回的 html
                s = str(body)
                try:
                    p = re.compile('http:.+feed\.js')                           # 通过 feed.js 来获得个人页 productsKey
                    m = re.search(p, s)
                    a = m.group(0)
                    logger.info("爬取的 feed.js 文件:" + a)
                    rq = Request(a, callback=self.parse_user_comments_articles)
                    rq.meta['user'] = response.meta['user']
                    yield rq
                except:
                    logger.error(str(response.url))
                # 从 feed.js 中获得用户的评论情况
        except:
            pass

    # 从用户的个人页获取用户的评论
    def parse_user_comments_articles(self, response):
        try:
            if response.body == "banned":
                req = response.request
                req.meta["change_proxy"] = True
                yield req
            else:
                body = response.body
                s = str(body)
                try:
                    p = re.compile('(?<=products/)\w+(?=/users/0/comments\?username\=)')
                    m = re.search(p, s)                                                         # 用于测试获得文章的评论情况
                    productsKey = m.group(0)
                    # 用户的评论页的数据
                    a = "http://comment.api.163.com/api/v1/products/" + productsKey \
                        + "/users/0/comments?username=" + str(response.meta['user']) + "&offset=" \
                        + "0&limit=30&ibc=newspc"
                    rq = Request(a, callback=self.parse_user_comments_doc)
                    rq.meta['offset'] = 0
                    rq.meta['productsKey'] = productsKey
                    yield rq
                except:
                    logger.error(str(response.url))
        except:
            pass

                                                                               # 爬取用户评论过的文章

    # 获得用户的阅读列表
    def parse_user_comments_doc(self, response):
        try:
            if response.body == "banned":
                req = response.request
                req.meta["change_proxy"] = True
                yield req
            else:
                body = response.body
                s = str(body)
                logger.info(s)
                a = json.loads(s)
                threads = a['threads']
                if len(threads)!=0:
                    commentsAndDocId = a['commentIdsAndDocId']
                    for comentAndDoc in commentsAndDocId:
                        commentIds = comentAndDoc['commentIds'].split(",")
                        for commentId in commentIds:
                            try:  # comentAndDoc['docId']+"_"+str(commentId) 构造的关键词可能错误
                                comments = a['comments'][comentAndDoc['docId']+"_"+str(commentId)]     # 获取评论 ID

                                if comments['user']['userId'] != 0:                             # 判断是否为未登录用户
                                    item = ReadItem()
                                    item['newsid'] = comentAndDoc['docId']
                                    item['time'] = comments['createTime']                       # 获取评论时间
                                    item['userid'] = comments['user']['userId']                 # 获取评论用户 id

                                    item['title'] = threads[item['newsid']]['title']            # 新闻标题
                                    item['newsUrl'] = threads[item['newsid']]['url']            # 新闻 URL
                                    print "======================",item['newsUrl']
                                    yield item

                                    if threads[comments['user']['userId']]['url'] not in self.burl:             # 新闻页去重
                                        req = Request(threads[comments['user']['userId']]['url'],self.parse)    # 爬取新闻页
                                        yield req
                                    else:
                                        if not isExists(self.sdb,names='visitedUrl', data=threads[comments['user']['userId']]['url']):  # 在 ssdb 中没出现过
                                            req = Request(threads[comments['user']['userId']]['url'], self.parse)  # 爬取新闻页
                                            yield req
                            except:
                                logger.error(str(response.url))
                                pass

                    # 用户的评论页的数据
                    # 这次的 offset 已经爬完,下次的 offset 需要再加 30
                    nextPageUrl = "http://comment.api.163.com/api/v1/products/" + response.meta['productsKey'] \
                        + "/users/0/comments?username=" + str(response.meta['user']) + "&offset=" \
                        + str(response.meta['offset']+30) +"&limit=30&ibc=newspc"
                    rq = Request(nextPageUrl,self.parse_user_comments_doc)
                    rq.meta['offset'] = response.meta['offset'] + 30                            # 下一次的偏移起点
                    rq.meta['productsKey'] = response.meta['productsKey']
                    yield rq
        except:
            pass


