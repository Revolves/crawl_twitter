result文件夹为收集的数据(其中data文件夹为收集到的推文数据，relation为关注列表)
表格文件为目标用户列表
keys为twitterAPI密钥
twitter_userINFO.json为目标用户账号信息
since_at运行时间戳（用于生成当前获取数据的时间点）

运行步骤：
安装依赖库：pip install -r .\requirements.txt

运行crawl_twitter.py,(第一次运行，设置first=1,爬取后续数据设置first=0)
