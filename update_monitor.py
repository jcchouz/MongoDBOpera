import pymongo

# 建立Mongodb连接
client = pymongo.MongoClient(
    "mongodb://202.204.54.23:27017/nfca_db",
    username='nfca',
    password='nfca'
)
db = client["nfca_db"]
col_1 = db["gms_monitor"]
col_2 = db['backfill_record']


# 修改所有数据 update_many()方法
for i in range(1, 11):
    backfill_record = col_2.find_one({"fill_id": i})
    if backfill_record:
        myquery = {"fill_id": i}
        newvalues = {"$set": {"fill_id": backfill_record["_id"]}}
        x = col_1.update_many(myquery, newvalues)
        print(x.modified_count, "文档已修改")
