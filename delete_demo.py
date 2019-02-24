import pymongo

# 建立连接
myclient = pymongo.MongoClient("mongodb://202.204.62.145:27017/", username='admin', password='afish1001')
mydb = myclient["runoobdb"]
mycol = mydb["sites"]


# 删除一个文档 delete_one()方法 (若有相同的，则选择文档中排序最前面的)
myquery = {"name": "Taobao"}
mycol.delete_one(myquery)
for x in mycol.find():  # 删除后输出
    print(x)


# 删除多个文档 delete_many()方法
myquery = {"name": {"$regex": "^F"}}  # 正则表达式，删除所有 name 字段中以 F 开头的文档
print(mycol.count_documents(myquery))
x = mycol.delete_many(myquery)
print(x.deleted_count, "个文档已删除")
print(mycol.count_documents(myquery))


# 删除集合中的所有文档 delete_many()方法,设置查询参数为空
x = mycol.delete_many({})
print(x.deleted_count, "个文档已删除")


# 删除集合 drop()方法
mycol2 = mydb["site2"]
mycol2.drop()
