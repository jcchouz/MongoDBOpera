import pymongo

# 建立连接
myclient = pymongo.MongoClient("mongodb://202.204.62.145:27017/", username='admin', password='afish1001')
mydb = myclient["runoobdb"]
mycol = mydb["sites"]

'''
sort()方法第一个参数为要排序的字段，第二个字段指定排序规则，1 为升序，-1 为降序，默认为升序。
'''


# 按照指定字段进行升序排序
mydoc = mycol.find().sort("alexa")  # 对字段 alexa 按升序排序
for x in mydoc:
    print(x)


# 按照指定字段进行降序排序
mydoc = mycol.find().sort("alexa", -1)  # 对字段 alexa 按降序排序
for x in mydoc:
    print(x)
