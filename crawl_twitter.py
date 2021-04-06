import tweepy
import pandas as pd
from utils import utils as tw


def main():
    # parser=argparse.ArgumentParser()
    # parser.add_argument('')
    global first, count, user_count
    first = 0  # 第一次运行
    count = 0
    user_count = 0
    namelist_file = pd.read_excel(
        '20210322.xlsx', engine='openpyxl')
    # consumer_key = ""
    # consumer_secret = ""
    # access_key = ""
    # access_secret = ""
    consumer_key = "ioTGfhxK3Fylub82QJmLMB6mB"
    consumer_secret = "fg19r72exdPQfNa0HzBRNPzUrPKZI4YvU4FVDEmlWxkaVcFuKs"
    access_key = "1372722615991738368-bQ4nwuK0vKY95zAIoIAaeqP44DYg56"
    access_secret = "qjZnegLBlk7OvBKGrjFvfjRV6rPu0descZhLdtaILLVlH"

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tw.get_api(consumer_key, consumer_secret, access_key, access_secret)

    datapath = tw.get_datapath()

    # 读取时间戳
    since_at = tw.get_since_at()

    if (first == 1):
        tw.first_getTweet(api, datapath, first, count,
                          user_count, namelist_file)
        first == 0
    else:
        tw.get_newTweet(api, datapath, namelist_file, since_at)

    tw.save_since_at()

    print("All finished!")


if __name__ == '__main__':
    main()
