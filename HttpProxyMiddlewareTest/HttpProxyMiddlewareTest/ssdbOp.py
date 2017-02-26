# -*- coding: utf-8 -*-




# 在SSDB中存储失败了的 URL
def getNotVisitUrl(sdb):
    urlList = sdb.zlist('visitUrl', 50)  # 每次获取 50 个没有访问的 URL 继续访问
    print urlList
    sdb.multi_zdel('visitUrl',urlList)
    return urlList                             # 返回一个 list

# 存放还没下载的 URL User Newsid names可以取 visitedUrl user news
def setNotVisit(sdb,names,data):
    sdb.zset(names, data)

# 删除已经访问过的 url newsid userid names可以取 visitedUrl user news
def visited(sdb,names='url',data=''):
    sdb.zdel(names, data)

# 判断 url newsid userid 是否存在,names可以取 visitedUrl user news
def isExists(sdb,names,data):
    return sdb.zexists(names, data)

