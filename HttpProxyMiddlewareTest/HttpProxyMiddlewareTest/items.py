# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.item import Field


class HttpproxymiddlewaretestItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class NewsTotalItem(scrapy.Item):
    newsid = Field()
    total = Field()    # 新闻总评论数

class NewsItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    url = Field()
    title = Field()
    content = Field()
    time = Field()
    resource = Field()
    newsid = Field()


class UrlItem(scrapy.Item):
    url = Field()

class UserItem(scrapy.Item):
    userid = Field()    # 用户的 userid
    uid = Field()       # id



class ReadItem(scrapy.Item):
    userid = Field()               # 用户的 userid
    newsid = Field()               # 新闻的 id
    create_time = Field()           # 评论的时间
    digg_count = Field()            # 点赞数
    reply_count = Field()           # 回复数量
