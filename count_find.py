# -*- coding: UTF-8 -*-

# 读取集合中的文档数目

import pymongo

# 建立Mongodb连接
client = pymongo.MongoClient(
    "mongodb://202.204.54.23:27017/nfca_db",
    username='nfca',
    password='nfca'
)
db = client["nfca_db"]
col_1 = db["gms_now"]


# 对find_all计数
def count_find_all():
    count = col_1.find().count()
    if count != 69:
        print(count)

if __name__ == '__main__':

    while True:
        count_find_all()
