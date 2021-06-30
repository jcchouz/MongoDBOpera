import pymongo

# 建立连接
myclient = pymongo.MongoClient("mongodb://159.138.163.117:16002/")
mydb = myclient["nfca_db"]
mycol = mydb["point"]

mydb.authenticate("nfca", "nfca")

# 查询集合中所有数据 find()方法
myresult = mycol.find()
# 总记录条数
print(myresult.count())
for r in myresult:
    if not (58 < r["point_id"] < 67 or 91 < r["point_id"] < 95):
        myquery = {"point_id": r["point_id"]}
        newvalues = {"$set": {"value_min_2": 0.0, "value_max_2": 0.0}}
        mycol.update_one(myquery, newvalues)
        print(r["point_id"])