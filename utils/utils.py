import tweepy
import csv
import json
import pprint
import os
import time
import datetime
import hashlib
import shutil
import pandas as pd
import socket

# import argparse
# API setting
language = "zh"
url = 'www.twitter.com'
page_type = 'twitter'
site_name = 'twitter'


def get_api(consumer_key, consumer_secret, access_key, access_secret):
    "获取API"
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True,
                     wait_on_rate_limit_notify=True)
    return api


def get_datapath():
    "返回数据保存路径"
    datasave = os.getcwd() + '\\result\\data'
    print(os.path.exists(datasave))
    if os.path.exists(datasave) is False:
        os.makedirs(datasave)
    print("收集到的数据保存在：", datasave)
    return datasave


def get_relation_path():
    """

    :return:
    """
    relationsave = os.getcwd() + '\\result\\relation'
    print(os.path.exists(relationsave))
    if os.path.exists(relationsave) is False:
        os.makedirs(relationsave)
    print("收集到的关系网络保存在：", relationsave)
    return relationsave


# 获取评论数(时耗较长)


def get_replies_count(api, tweet_id, tweet_name):
    "获取评论数（时耗较长）"
    replies = 0
    # nowtweet=api.search(q=f'to:{tweet_name}',since_id=tweet_id)
    for tweet in tweepy.Cursor(api.search, q='to:' + tweet_name, result_type='recent').items(100):
        if hasattr(tweet, 'in_reply_to_status_id_str'):
            if tweet.in_reply_to_status_id == tweet_id:
                replies += 1
    return replies


# list
# 推文链接


def get_twitter_url(user_name, status_id):
    "生成该条tweet的链接"
    return 'https://' + url + '/' + str(user_name) + "/status/" + str(status_id)


# 推文标题


def get_title(content):
    "获取正文标题"
    return str(content)[0:30]


def get_retweetINFO(status):
    "获取转推消息细节，返回"
    content = status.retweeted_status.full_text.replace('\n', ' ')
    content.replace('\r', '')
    content.replace('\t', ' ')
    isTopic = 2
    originalId = status.retweeted_status.id
    originalPoster = status.retweeted_status.user.screen_name
    title = 'Retweeted: ' + content[0:30]
    content = 'Retweeted: ' + content
    return isTopic, content, originalId, originalPoster, title


def get_commentINFO(status):
    "获取评论信息，返回"
    originalId = status.in_reply_to_status_id
    originalPoster = status.in_reply_to_screen_name
    content = status.full_text.replace('\n', ' ')
    content.replace('\r', '')
    content.replace('\t', ' ')
    title = content[0:30]  # 发文标题。正文前30字
    isTopic = 0
    return isTopic, content, originalId, originalPoster, title


def get_tweetINFO(status):
    "获取自己所发推文信息，返回"
    content = status.full_text.replace('\n', ' ')
    content.replace('\r', '')
    content.replace('\t', ' ')
    title = content[0:30]  # 发文标题。正文前30字
    isTopic = 1
    return isTopic, content, title


def get_comment_title(content):
    "获取回复标题"
    return str(content)[0:30]


# 发布时间


def get_publishTime(Time):
    "生成无符号时间"
    return Time.strftime("%Y%m%d%H%M%S")


def get_md5(url):
    "返回MD5编码"
    md5_obj = hashlib.md5(url.encode("utf-8"))
    return md5_obj.hexdigest()


def create_filepath(_path, q):
    "创建目录"
    savepath = os.path.join(_path, q)
    # print(os.path.isfile(save_tweet))
    if (os.path.isfile(savepath)):
        shutil.rmtree(savepath)
        os.makedirs(savepath)
    else:
        os.makedirs(savepath)
    return savepath


def create_file(_path):
    "创建文件并返回"
    ip = socket.gethostbyname(socket.gethostname())
    ip = ip.split('.')[2:4]
    save_file = open(
        _path + '\\twitter_' + datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[0:17] + '_' + ip[0] + '_' + ip[1],
        'w',
        newline="", encoding='utf-8')
    save_writer = csv.writer(save_file)
    return save_file, save_writer


def create_relation_file(_path, screen_name):
    """
    create save flie
    :param _path:
    :return:
    """
    save_file = open(_path + '\\' + screen_name + '-relation', 'w+', newline="", encoding='utf-8')
    return save_file


# 获取推文


def firstget_tweet(api, query, first, savepath, count):
    for status in tweepy.Cursor(api.user_timeline, id=query, count=2000, tweet_mode='extended', show_user=True).items():
        # pprint.pprint(status.author._json)
        # continue  #调试

        # 第一次搜索该用户则给该用户创建目录
        if first == 1 or count == 0:
            # first = 0
            save_file, save_writer = create_file(savepath)

        siteName = site_name  # 网站名称
        siteUrl = url  # 网站主页地址
        pageType = page_type  #
        poster = status.author.screen_name  # 发文者ID
        # url+'/'+str(poster)+'/status/'+str(status.id)    #发文地址
        pageUrl = get_twitter_url(poster, status.id)

        publishTime = get_publishTime(status.created_at)  # 发布时间
        GUID = publishTime + get_md5(pageUrl)  # 利用时间戳和链接生成的主键
        publishDate = publishTime[0:8]

        # pls = get_replies_count(status.id, poster)  # 评论数
        pls = 0

        zfs = status.retweet_count  # 转发数
        # dzs=status._json['retweeted_status']['favorite_count']    #点赞数
        dzs = status.favorite_count
        dataPusher = 'njust'
        posterId = status.author.screen_name  #
        articleID = status.id  # 该文id

        # region = status.place  # 发布消息位置
        if status.place == None:
            region = "[]"
        else:
            name = status.place.name
            country = status.place.country
            region = [country, name]
        # credit=1    #该消息置信度，默认1

        # 每条数据以<REC>起始
        # 判断是否为转推
        # if str(status.retweeted) == True:     #失效
        if str(status.full_text)[0:2] == 'RT':
            isTopic, content, originalId, originalPoster, title = get_retweetINFO(
                status)
            save_file.writelines(["<REC>\n", "<GUID>=" + str(GUID) + '\n', "<stieName>=" + str(siteName) + '\n',
                                  "<siteUrl>=" + str(siteUrl) + '\n', "<pageType>=" + str(pageType) + '\n',
                                  "<title>=" + title + '\n', "<content>=" + content + '\n',
                                  "<poster>=" + str(poster) + '\n', "<pageUrl>=" + pageUrl + '\n',
                                  "<isTopic>=" + str(isTopic) + '\n', "<publishTime>=" + str(publishTime) + '\n',
                                  "<publishDate>=" + str(publishDate) + '\n', "<pls>=" + str(pls) + '\n',
                                  "<zfs>=" + str(zfs) + '\n', "<dzs>=" + str(dzs) + '\n',
                                  "<dataPusher>=" + dataPusher + '\n', "<posterID>=" + str(posterId) + '\n',
                                  "<articleId>=" + str(articleID) + '\n', "<originallId>=" + str(originalId) + '\n',
                                  "<originalPoster>=" + originalPoster + '\n', "<region>=" + str(region) + '\n', '\n'])
        # # 判断是否为评论
        elif (status.in_reply_to_status_id != None):
            isTopic, content, originalId, originalPoster, title = get_commentINFO(
                status)
            save_file.writelines(["<REC>\n", "<GUID>=" + str(GUID) + '\n', "<stieName>=" + str(siteName) + '\n',
                                  "<siteUrl>=" + str(siteUrl) + '\n', "<pageType>=" + str(pageType) + '\n',
                                  "<title>=" + title + '\n', "<content>=" + content + '\n',
                                  "<poster>=" + str(poster) + '\n', "<pageUrl>=" + pageUrl + '\n',
                                  "<isTopic>=" + str(isTopic) + '\n', "<publishTime>=" + str(publishTime) + '\n',
                                  "<publishDate>=" + str(publishDate) + '\n', "<pls>=" + str(pls) + '\n',
                                  "<zfs>=" + str(zfs) + '\n', "<dzs>=" + str(dzs) + '\n',
                                  "<dataPusher>=" + dataPusher + '\n', "<posterID>=" + str(posterId) + '\n',
                                  "<articleId>=" + str(articleID) + '\n', "<originallId>=" + str(originalId) + '\n',
                                  "<originalPoster>=" + originalPoster + '\n', "<region>=" + str(region) + '\n', '\n'])
        else:
            isTopic, content, title = get_tweetINFO(status)
            save_file.writelines(["<REC>\n", "<GUID>=" + str(GUID) + '\n', "<stieName>=" + str(siteName) + '\n',
                                  "<siteUrl>=" + str(siteUrl) + '\n', "<pageType>=" + str(pageType) + '\n',
                                  "<title>=" + title + '\n', "<content>=" + content + '\n',
                                  "<poster>=" + str(poster) + '\n', "<pageUrl>=" + pageUrl + '\n',
                                  "<isTopic>=" + str(isTopic) + '\n',
                                  "<publishTime>=" + str(publishTime) + '\n',
                                  "<publishDate>=" + str(publishDate) + '\n', "<pls>=" + str(pls) + '\n',
                                  "<zfs>=" + str(zfs) + '\n', "<dzs>=" + str(dzs) + '\n',
                                  "<dataPusher>=" + dataPusher + '\n', "<posterID>=" + str(posterId) + '\n',
                                  "<articleId>=" + str(articleID) + '\n', "<region>=" + str(region) + '\n', '\n'])

        # 每50条数据保存一次
        count += 1
        if (count == 50):
            count = 0
            save_file.close()
            time.sleep(1)
        first = 0


def get_tweet(api, query, save_file, save_writer, since_at):
    for status in tweepy.Cursor(api.user_timeline, id=query, count=2000, tweet_mode='extended', show_user=True).items():
        publishTime = get_publishTime(status.created_at)  # 发布时间
        if since_at[0:13] > publishTime:
            break
        siteName = site_name  # 网站名称
        siteUrl = url  # 网站主页地址
        pageType = page_type  #
        poster = status.author.screen_name  # 发文者ID
        # url+'/'+str(poster)+'/status/'+str(status.id)    #发文地址
        pageUrl = get_twitter_url(poster, status.id)

        # publishTime = get_publishTime(status.created_at)  # 发布时间
        GUID = publishTime + get_md5(pageUrl)  # 利用时间戳和链接生成的主键
        publishDate = publishTime[0:8]

        # pls = get_replies_count(status.id, poster)  # 评论数
        pls = 0

        zfs = status.retweet_count  # 转发数
        # dzs=status._json['retweeted_status']['favorite_count']    #点赞数
        dzs = status.favorite_count
        dataPusher = 'njust'
        posterId = status.author.screen_name  #
        articleID = status.id  # 该文id

        # region = status.place  # 发布消息位置
        if status.place == None:
            region = "[]"
        else:
            name = status.place.name
            country = status.place.country
            region = [country, name]
        # credit=1    #该消息置信度，默认1

        # 每条数据以<REC>起始
        # 判断是否为转推
        # if str(status.retweeted) == True:     #失效
        if str(status.full_text)[0:2] == 'RT':
            isTopic, content, originalId, originalPoster, title = get_retweetINFO(
                status)

            save_file.writelines(["<REC>\n", "<GUID>=" + str(GUID) + '\n', "<stieName>=" + str(siteName) + '\n',
                                  "<siteUrl>=" + str(siteUrl) + '\n', "<pageType>=" + str(pageType) + '\n',
                                  "<title>=" + title + '\n', "<content>=" + content + '\n',
                                  "<poster>=" + str(poster) + '\n', "<pageUrl>=" + pageUrl + '\n',
                                  "<isTopic>=" + str(isTopic) + '\n', "<publishTime>=" + str(publishTime) + '\n',
                                  "<publishDate>=" + str(publishDate) + '\n', "<pls>=" + str(pls) + '\n',
                                  "<zfs>=" + str(zfs) + '\n', "<dzs>=" + str(dzs) + '\n',
                                  "<dataPusher>=" + dataPusher + '\n', "<posterID>=" + str(posterId) + '\n',
                                  "<articleId>=" + str(articleID) + '\n', "<originallId>=" + str(originalId) + '\n',
                                  "<originalPoster>=" + originalPoster + '\n', "<region>=" + str(region) + '\n', '\n'])
        # # 判断是否为评论
        elif (status.in_reply_to_status_id != None):
            isTopic, content, originalId, originalPoster, title = get_commentINFO(
                status)
            save_file.writelines(["<REC>\n", "<GUID>=" + str(GUID) + '\n', "<stieName>=" + str(siteName) + '\n',
                                  "<siteUrl>=" + str(siteUrl) + '\n', "<pageType>=" + str(pageType) + '\n',
                                  "<title>=" + title + '\n', "<content>=" + content + '\n',
                                  "<poster>=" + str(poster) + '\n', "<pageUrl>=" + pageUrl + '\n',
                                  "<isTopic>=" + str(isTopic) + '\n', "<publishTime>=" + str(publishTime) + '\n',
                                  "<publishDate>=" + str(publishDate) + '\n', "<pls>=" + str(pls) + '\n',
                                  "<zfs>=" + str(zfs) + '\n', "<dzs>=" + str(dzs) + '\n',
                                  "<dataPusher>=" + dataPusher + '\n', "<posterID>=" + str(posterId) + '\n',
                                  "<articleId>=" + str(articleID) + '\n', "<originallId>=" + str(originalId) + '\n',
                                  "<originalPoster>=" + originalPoster + '\n', "<region>=" + str(region) + '\n', '\n'])
        else:
            isTopic, content, title = get_tweetINFO(status)
            save_file.writelines(["<REC>\n", "<GUID>=" + str(GUID) + '\n', "<stieName>=" + str(siteName) + '\n',
                                  "<siteUrl>=" + str(siteUrl) + '\n', "<pageType>=" + str(pageType) + '\n',
                                  "<title>=" + title + '\n', "<content>=" + content + '\n',
                                  "<poster>=" + str(poster) + '\n', "<pageUrl>=" + pageUrl + '\n',
                                  "<isTopic>=" + str(isTopic) + '\n',
                                  "<publishTime>=" + str(publishTime) + '\n',
                                  "<publishDate>=" + str(publishDate) + '\n', "<pls>=" + str(pls) + '\n',
                                  "<zfs>=" + str(zfs) + '\n', "<dzs>=" + str(dzs) + '\n',
                                  "<dataPusher>=" + dataPusher + '\n', "<posterID>=" + str(posterId) + '\n',
                                  "<articleId>=" + str(articleID) + '\n', "<region>=" + str(region) + '\n', '\n'])


def get_statusINFO(api, query, user_count):
    "获取指定目标的账户号信息"
    status = api.get_user(query)

    user_INFO = {}
    data = json.loads(json.dumps(user_INFO))
    # 目标账号信息
    mbzhxx = {}
    mbzhxx['mbzhid'] = 'twitterZHXX_' + str(user_count)  # status.id
    mbzhxx['ptmc'] = site_name  # 平台名称
    mbzhxx['zhid'] = status.screen_name
    mbzhxx['mbnc'] = status.name
    mbzhxx['indexUrl'] = url + '/' + status.screen_name
    mbzhxx['type'] = []
    mbzhxx['age'] = ''
    mbzhxx['xb'] = ''
    area = status.location
    if area == None:
        area = ''
    mbzhxx['area'] = area
    mbzhxx['hyd'] = ''
    last_modified = time.strftime("%Y%m%d%H%M%S")
    if hasattr(status, 'status'):
        inuse = "是"
        # last_modified = get_publishTime(status.status.created_at)
    else:
        inuse = "否"
        # last_modified = '[]'
    mbzhxx['inuse'] = inuse
    mbzhxx['last_modified'] = last_modified
    mbzhxx['gzs'] = str(status.followers_count)
    mbzhxx['ms'] = status.description.replace('\n', ';')
    data['mbzhxx'] = mbzhxx

    return data


def get_friends(api, screen_name, relationpath):
    savefile = create_relation_file(relationpath, screen_name)
    friends_data = {}
    friends = json.loads(json.dumps(friends_data))
    friends["type"] = "关注"
    friends["startNodeId"] = screen_name
    friends["startNodeLabel"] = "twitter账号"

    for user in tweepy.Cursor(api.friends, screen_name=screen_name).items():
        friends["endNodeId"] = user.screen_name
        friends["endNodeLabel"] = "twitter账号"
        props = {}
        props["关注时间"] = "未知"
        friends["props"] = props
        json.dump(friends, savefile, ensure_ascii=False)
        savefile.writelines('\n')


def first_getTweet(api, datapath, relateionpath, first, count, user_count, namelist_file):
    file_users = open('twitter_userINFO.json', 'w+', encoding='utf-8-sig')
    file_friends = open('twitter_friends.json', 'w+', encoding='utf-8-sig')
    # 建立一个列表
    datalist_user = {}
    data_user = json.loads(json.dumps(datalist_user))
    data_user['datalist'] = []
    datalist_friends = {}
    data_friend = json.loads(json.dumps(datalist_friends))

    for query in namelist_file.iloc[:, [2]].values:
        q = str(query).replace('[\'', '')
        q = q.replace('\']', '')
        # firstget_tweet(api, q, first, datapath, count)
        user_count += 1
        # data_user['datalist'].append(get_statusINFO(api, q, user_count))
        data_friend[q] = get_friends(api, q, relateionpath)
        print(q, " finished!")
    json.dump(data_user, file_users, indent=4, ensure_ascii=False)
    print("用户信息保存在 twitter_userINFO.json文件中")
    json.dump(data_friend, file_friends, indent=4, ensure_ascii=False)
    print("users' friends list saved in twitter_friends.json")


def get_newTweet(api, datapath, namelist_file, since_at):
    "获取上次爬取时间到现在时刻内的新数据"
    save_file, save_writer = create_file(datapath)
    for query in namelist_file.iloc[:, [2]].values:
        q = str(query).replace('[\'', '')
        q = q.replace('\']', '')
        get_tweet(api, q, save_file, save_writer, since_at)
        print(q, " finished!")


def get_since_at():
    "获取上次获取数据的时间戳"
    since_at_file = open('since_at', 'r')
    since_at = since_at_file.readline()
    since_at_file.close()
    return since_at


def save_since_at():
    """保存当前的时间戳"""
    since_at = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[0:17]
    since_at_file = open('since_at', 'w')
    since_at_file.writelines(since_at)
    since_at_file.close()
