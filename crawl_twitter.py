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
# import argparse
# API setting
language = "zh"
url = 'twitter.com'
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
    datasave = os.getcwd()+'\data'
    print("收集到的数据保存在：", datasave)
    return datapath
# 获取评论数(时耗较长)


def get_replies_count(tweet_id, tweet_name):
    "获取评论数（时耗较长）"
    replies = 0
    # nowtweet=api.search(q=f'to:{tweet_name}',since_id=tweet_id)
    for tweet in tweepy.Cursor(api.search, q='to:'+tweet_name, result_type='recent').items(100):
        if hasattr(tweet, 'in_reply_to_status_id_str'):
            if tweet.in_reply_to_status_id == tweet_id:
                replies += 1
    return replies
# list
# 推文链接


def get_twitter_url(user_name, status_id):
    "生成该条tweet的链接"
    return url + str(user_name) + "/status/" + str(status_id)
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
    title = 'Retweeted: '+content[0:30]
    content = 'Retweeted: '+content
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
    if(os.path.isfile(savepath)):
        shutil.rmtree(savepath)
        os.makedirs(savepath)
    else:
        os.makedirs(savepath)
    return savepath


def create_file(_path):
    "创建文件并返回"
    save_file = open(_path+'\\twitter_'+time.strftime("%Y%m%d%H%M%S"), 'w',
                     newline="", encoding='utf-8')
    save_writer = csv.writer(save_file)
    return save_file, save_writer
# 获取推文


def get_tweet(api, query, frist, savepath, count):

    for status in tweepy.Cursor(api.user_timeline, id=query, count=2000, tweet_mode='extended', show_user=True).items():
        # pprint.pprint(status.author._json)
        # continue  #调试

        # 第一次搜索该用户则给该用户创建目录
        if frist == 1 or count == 0:
            # frist = 0
            save_file, save_writer = create_file(savepath)

        siteName = site_name  # 网站名称
        siteUrl = url  # 网站主页地址
        pageType = page_type   #
        poster = status.author.screen_name  # 发文者ID
        # url+'/'+str(poster)+'/status/'+str(status.id)    #发文地址
        pageUrl = get_twitter_url(poster, status.id)

        publishTime = get_publishTime(status.created_at)  # 发布时间
        GUID = publishTime+get_md5(pageUrl)  # 利用时间戳和链接生成的主键
        publishDate = publishTime[0:8]

        # pls = get_replies_count(status.id, poster)  # 评论数
        pls = 0

        zfs = status.retweet_count  # 转发数
        # dzs=status._json['retweeted_status']['favorite_count']    #点赞数
        dzs = status.favorite_count
        dataPusher = 'njust'
        posterId = status.author.screen_name       #
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

            save_file.writelines(["<REC>\n", "<GUID>="+str(GUID)+'\n', "<stieName>="+str(siteName)+'\n', "<siteUrl>="+str(siteUrl)+'\n', "<pageType>="+str(pageType)+'\n', "<title>="+title+'\n', "<content>="+content+'\n', "<poster>="+str(poster)+'\n', "<pageUrl>="+pageUrl+'\n', "<isTopic>="+str(isTopic)+'\n', "<publishTime>="+str(publishTime)+'\n',
                                 "<publishDate>="+str(publishDate)+'\n', "<pls>="+str(pls)+'\n', "<zfs>="+str(zfs)+'\n', "<dzs>="+str(dzs)+'\n', "<dataPusher>="+dataPusher+'\n', "<posterID>="+str(posterId)+'\n', "<articleId>="+str(articleID)+'\n', "<originallId>="+str(originalId)+'\n', "<originalPoster>="+originalPoster+'\n', "<region>="+str(region)+'\n', '\n'])
        # # 判断是否为评论
        elif (status.in_reply_to_status_id != None):
            isTopic, content, originalId, originalPoster, title = get_commentINFO(
                status)
            save_file.writelines(["<REC>\n", "<GUID>="+str(GUID)+'\n', "<stieName>="+str(siteName)+'\n', "<siteUrl>="+str(siteUrl)+'\n', "<pageType>="+str(pageType)+'\n', "<title>="+title+'\n', "<content>="+content+'\n', "<poster>="+str(poster)+'\n', "<pageUrl>="+pageUrl+'\n', "<isTopic>="+str(isTopic)+'\n', "<publishTime>="+str(publishTime)+'\n',
                                 "<publishDate>="+str(publishDate)+'\n', "<pls>="+str(pls)+'\n', "<zfs>="+str(zfs)+'\n', "<dzs>="+str(dzs)+'\n', "<dataPusher>="+dataPusher+'\n', "<posterID>="+str(posterId)+'\n', "<articleId>="+str(articleID)+'\n', "<originallId>="+str(originalId)+'\n', "<originalPoster>="+originalPoster+'\n', "<region>="+str(region)+'\n', '\n'])
        else:
            isTopic, content, title = get_tweetINFO(status)
            save_file.writelines(["<REC>\n", "<GUID>="+str(GUID)+'\n', "<stieName>="+str(siteName)+'\n', "<siteUrl>="+str(siteUrl)+'\n', "<pageType>="+str(pageType)+'\n', "<title>="+title+'\n', "<content>="+content+'\n', "<poster>="+str(poster)+'\n', "<pageUrl>="+pageUrl+'\n', "<isTopic>="+str(isTopic)+'\n',
                                 "<publishTime>="+str(publishTime)+'\n', "<publishDate>="+str(publishDate)+'\n', "<pls>="+str(pls)+'\n', "<zfs>="+str(zfs)+'\n', "<dzs>="+str(dzs)+'\n', "<dataPusher>="+dataPusher+'\n', "<posterID>="+str(posterId)+'\n', "<articleId>="+str(articleID)+'\n', "<region>="+str(region)+'\n', '\n'])

        # 每50条数据保存一次
        count += 1
        if(count == 50):
            count = 0
            save_file.close()
            time.sleep(1)
        frist = 0


def get_statusINFO(api, query, user_count, file):
    "获取指定目标的账户号信息"
    status = api.get_user(query)

    user_INFO = {}
    data = json.loads(json.dumps(user_INFO))
    # print(user_count)
    # pprint.pprint(status)

    user_INFO = {}
    data = json.loads(json.dumps(user_INFO))
    # 目标账号信息
    mbzhxx = {}
    mbzhxx['mbzhid'] = 'twitterZHXX_'+str(user_count)  # status.id
    mbzhxx['ptmc'] = site_name  # 平台名称
    mbzhxx['zhid'] = status.screen_name
    mbzhxx['mbnc'] = status.name
    mbzhxx['indexUrl'] = url+'/'+status.screen_name
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
    mbzhxx['ms'] = status.description.replace('\n', ';')
    data['mbzhxx'] = mbzhxx

    return data


def main():
    # parser=argparse.ArgumentParser()
    # parser.add_argument('')
    global frist, count, user_count
    frist = 1
    count = 0
    user_count = 0
    namelist_file = pd.read_excel(
        '20210322.xlsx', engine='openpyxl')
    # consumer_key = ""
    # consumer_secret = ""
    # access_key = ""
    # access_secret = ""
    
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = get_api(consumer_key, consumer_secret, access_key, access_secret)
    datapath = get_datapath()

    file = open('twitter_userINFO.json', 'w+', encoding='utf-8-sig')
    # 建立一个列表
    datalist = {}
    data = json.loads(json.dumps(datalist))
    data['datalist'] = []

    for query in namelist_file.iloc[:, [2]].values:
        q = str(query).replace('[\'', '')
        q = q.replace('\']', '')
        get_tweet(api, q, frist, datapath, count)
        user_count += 1
        data['datalist'].append(get_statusINFO(api, q, user_count, file))
        print(q, " finished!")
    json.dump(data, file, indent=4, ensure_ascii=False)
    print("用户信息保存在 twitter_userINFO.json文件中")
    print("All finished!")


if __name__ == '__main__':
    main()
