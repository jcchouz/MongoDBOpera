import pymongo

# 建立连接
myclient = pymongo.MongoClient("mongodb://202.204.62.145:27017/", username='admin', password='afish1001')


# 创建数据库
mydb = myclient["runoobdb"]


# 显示所有数据库
dblist = myclient.list_database_names()
for db_name in dblist:
    print(db_name)


# 判断集合是否已存在
collist = mydb. list_collection_names()
if "test_col" in collist:   # 判断 sites 集合是否存在
    print("集合已存在！")


# 创建集合
mycol = mydb["sites"]  # (注意: 在 MongoDB 中，集合只有在内容插入后才会创建! 就是说，创建集合(数据表)后 \
                                          # 要再插入一个文档(记录)，集合才会真正创建。)


# 插入单个文档 insert_one()方法
mydict = {"name": "RUNOOB", "alexa": "10000", "url": "https://www.runoob.com"}
x = mycol.insert_one(mydict)
print(x)
print(x.inserted_id)  # 返回 _id 字段(如果我们在插入文档时没有指定 _id，MongoDB 会为每个文档添加一个唯一的 id)


# 插入多个文档 insert_many()方法
mylist = [
    {"name": "Taobao", "alexa": "100", "url": "https://www.taobao.com"},
    {"name": "QQ", "alexa": "101", "url": "https://www.qq.com"},
    {"name": "Facebook", "alexa": "10", "url": "https://www.facebook.com"},
    {"name": "知乎", "alexa": "103", "url": "https://www.zhihu.com"},
    {"name": "Github", "alexa": "109", "url": "https://www.github.com"}
]
x = mycol.insert_many(mylist)
print(x.inserted_ids)  # 输出插入的所有文档对应的 _id 值


# 插入指定 _id 的多个文档 在文档中自己指定 _id
mycol = mydb["sites2"]
mylist = [
    {"_id": 1, "name": "RUNOOB", "address": "菜鸟教程"},
    {"_id": 2, "name": "Google", "address": "Google 搜索"},
    {"_id": 3, "name": "Facebook", "address": "脸书"},
    {"_id": 4, "name": "Taobao", "address": "淘宝"},
    {"_id": 5, "name": "Zhihu", "address": "知乎"}
]
x = mycol.insert_many(mylist)
print(x.inserted_ids)  # 输出插入的所有文档对应的 _id 值
