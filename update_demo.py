import pymongo

# 建立连接
myclient = pymongo.MongoClient("mongodb://202.204.62.145:27017/", username='admin', password='afish1001')
mydb = myclient["runoobdb"]
mycol = mydb["sites"]


# 修改一条数据 update_one()方法 ,如果查找到的匹配数据多于一条，则只会修改第一条。
myquery = {"alexa": "10000"}
newvalues = {"$set": {"alexa": "12345"}}
mycol.update_one(myquery, newvalues)  # 将 alexa 字段的值 10000 改为 12345
for x in mycol.find():  # 输出修改后的  "sites"  集合
    print(x)


# 修改所有数据 update_many()方法
myquery = {"name": {"$regex": "^G"}}
newvalues = {"$set": {"alexa": "123"}}
x = mycol.update_many(myquery, newvalues)  # 查找所有以 G 开头的 name 字段，并将匹配到所有记录的 alexa 字段修改为 123：
print(x.modified_count, "文档已修改")
