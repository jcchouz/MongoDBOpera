# -*- coding: UTF-8 -*-

# 更新浓度均匀度数据表，前端显示

import pymongo
import datetime
import random

"""
instrument_serial: 设备编号
density: 浓度
evenness: 均匀度
time: 监测时间
"""

nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 现在时间
print("========" + str(nowTime) + "========")

# 建立Mongodb连接
client = pymongo.MongoClient(
    "mongodb://202.204.54.23:27017/nfca_db",
    username='nfca',
    password='nfca'
)
db = client["nfca_db"]
col_1 = db["gms_den_even"]
col_2 = db["gms_den_even_now"]

# 一共有两个摄像头视频
instrument_serials = [1, 2]

for instrument_serial in instrument_serials:
    density = random.uniform(0.65, 0.67)
    evenness = random.uniform(0.9, 0.95)

    monitor = {
        "instrument_serial": instrument_serial,
        "density": density,
        "evenness": evenness,
        "time": datetime.datetime.strptime(str(nowTime), '%Y-%m-%d %H:%M:%S')
    }

    # 插入视频数据到历史数据表
    col_1.insert_one(monitor)

    # 更新实时监测值数据表的数据
    gms_den_even_now_query = col_2.find_one({"instrument_serial": instrument_serial})
    if gms_den_even_now_query is None:
        col_2.insert_one(monitor)
    else:
        newvalues = {"$set":
                         {
                             "instrument_serial": instrument_serial,
                             "density": density,
                             "evenness": evenness,
                             "time": datetime.datetime.strptime(str(nowTime), '%Y-%m-%d %H:%M:%S')
                          }
                     }
        col_2.update_one(gms_den_even_now_query, newvalues)