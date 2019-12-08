import pymongo

# 建立连接
client = pymongo.MongoClient(
    "mongodb://202.204.62.229:27017/nfca",
    username='afish',
    password='afish1001'
)
db = client["nfca"]
col_1 = db["instrument"]


# 查询一条数据 find_one()方法
x = col.find_one()  # 查询第一条数据
y = col.find_one({"name": "QQ"})  # 查询{"name": "QQ"}的第一条数据
print(x)
print(y)


# 查询集合中所有数据 find()方法
myresult = col.find()
# 总记录条数
print(myresult.count())
for x in myresult:
    print(x)


# 查询指定字段的所有数据 find()方法,将需要返回的字段对应值设置为 1
for x in col.find({}, {"_id": 0, "name": 1, "alexa": 1}):
    print(x)
# 注：除了 _id 可以指定 0 外，其余需要就指定 1 ，不需要就不写。如果设置了某一个字段为 0，则其他默认必须都为 1，反之亦然。
for x in col.find({}, {"alexa": 0}):
    print(x)
# 以下代码就会报错：
# for x in mycol.find({},{ "name": 1, "alexa": 0 }):
#   print(x)


# 根据指定条件查询 find()方法
myquery = {"name": "RUNOOB"}
mydoc = col.find(myquery)
for x in mydoc:
    print(x)


# 使用正则表达式查询
myquery = {"name": {"$regex": "^R"}}  # name 字段中第一个字母为 "R" 的数据
mydoc = col.find(myquery)
for x in mydoc:
    print(x)


# 高级查询
myquery = {"name": {"$gt": "H"}}  # name 字段中第一个字母 ASCII 值大于 "H" 的数据
mydoc = col.find(myquery)
for x in mydoc:
    print(x)


# 返回指定条数记录 limit()方法
myresult = col.find().limit(3)
# 输出结果
for x in myresult:
    print(x)
